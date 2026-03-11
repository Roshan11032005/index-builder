// Package config loads runtime configuration from (in priority order):
//  1. Existing environment variables
//  2. .env file (optional)
//  3. Built-in defaults
package config

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// ─── Sub-configs ──────────────────────────────────────────────────────────────

// BucketConfig holds credentials for one S3 bucket.
type BucketConfig struct {
	Name      string
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
}

// ShardNode is a single RocksDB shard endpoint.
type ShardNode struct {
	ID  int
	URL string // used only for the cursor shard (gRPC address)
}

// ─── Main config ─────────────────────────────────────────────────────────────

// Config is the fully resolved runtime configuration.
type Config struct {
	// S3
	Buckets []BucketConfig

	// Shards — each entry holds the gRPC address (host:port)
	Shards         []ShardNode
	CursorShardIdx int // index into Shards used to persist the cursor key

	// Processing
	CompactionDeltaLimit int
	CompactionMaxAge     time.Duration
	SSTTempDir           string
	PollInterval         time.Duration

	// Operational HTTP server (metrics / health / force-compact)
	HTTPPort string
}

// Load reads .env (if present), merges with the environment, then builds Config.
func Load() (*Config, error) {
	if err := loadDotEnv(".env"); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	buckets := loadBuckets()
	shards, err := loadShards()
	if err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("config: no shards configured — set SHARD_URLS or SHARD_0, SHARD_1, …")
	}

	cfg := &Config{
		Buckets:              buckets,
		Shards:               shards,
		CursorShardIdx:       envInt("CURSOR_SHARD_IDX", 0),
		CompactionDeltaLimit: envInt("COMPACTION_DELTA_LIMIT", 50),
		CompactionMaxAge:     envDuration("COMPACTION_MAX_AGE", 24*time.Hour),
		SSTTempDir:           envStr("SST_TEMP_DIR", "./tmp/sst"),
		PollInterval:         envDuration("POLL_INTERVAL", 10*time.Second),
		HTTPPort:             envStr("HTTP_PORT", "8086"),
	}

	log.Printf("[config] %d bucket(s), %d shard(s), poll=%s, http_port=%s",
		len(cfg.Buckets), len(cfg.Shards), cfg.PollInterval, cfg.HTTPPort)

	return cfg, nil
}

// CursorShard returns the shard node designated for cursor persistence.
func (c *Config) CursorShard() ShardNode {
	idx := c.CursorShardIdx
	if idx < 0 || idx >= len(c.Shards) {
		return c.Shards[0]
	}
	return c.Shards[idx]
}

// ─── Bucket loading ───────────────────────────────────────────────────────────

func loadBuckets() []BucketConfig {
	// Multi-bucket: BUCKET_0="name|endpoint|access|secret|region", BUCKET_1=…
	var buckets []BucketConfig
	for i := 0; ; i++ {
		val := os.Getenv(fmt.Sprintf("BUCKET_%d", i))
		if val == "" {
			break
		}
		parts := strings.Split(val, "|")
		if len(parts) != 5 {
			log.Printf("[config] BUCKET_%d: expected name|endpoint|access|secret|region, skipping", i)
			continue
		}
		buckets = append(buckets, BucketConfig{
			Name: parts[0], Endpoint: parts[1],
			AccessKey: parts[2], SecretKey: parts[3], Region: parts[4],
		})
	}

	// Single-bucket fallback
	if len(buckets) == 0 {
		buckets = []BucketConfig{{
			Name:      envStr("S3_BUCKET", "events-bucket"),
			Endpoint:  envStr("S3_ENDPOINT", "http://localhost:9002"),
			AccessKey: envStr("S3_ACCESS_KEY", "admin"),
			SecretKey: envStr("S3_SECRET_KEY", "strongpassword"),
			Region:    envStr("S3_REGION", "us-east-1"),
		}}
	}
	return buckets
}

// ─── Shard loading ────────────────────────────────────────────────────────────

// loadShards supports three env layouts (in order of preference):
//  1. SHARD_URLS="host:port,host:port,…"   (comma-separated gRPC addresses)
//  2. SHARD_0="host:port", SHARD_1=…       (numbered variables)
//  3. NUM_SHARDS + NODE_START_PORT + SHARD_HOST  (legacy range)
func loadShards() ([]ShardNode, error) {
	if raw := os.Getenv("SHARD_URLS"); raw != "" {
		return parseSeparated(raw, ","), nil
	}

	var shards []ShardNode
	for i := 0; ; i++ {
		addr := os.Getenv(fmt.Sprintf("SHARD_%d", i))
		if addr == "" {
			break
		}
		shards = append(shards, ShardNode{ID: i, URL: strings.TrimSpace(addr)})
		log.Printf("[config] shard %d → %s", i, addr)
	}
	if len(shards) > 0 {
		return shards, nil
	}

	// Legacy: NUM_SHARDS + base port
	n := envInt("NUM_SHARDS", 0)
	if n > 0 {
		host := envStr("SHARD_HOST", "localhost")
		base := envInt("NODE_START_PORT", 4001)
		log.Printf("[config] legacy mode: %d shards from %s starting at port %d", n, host, base)
		for i := 0; i < n; i++ {
			addr := fmt.Sprintf("%s:%d", host, base+i)
			shards = append(shards, ShardNode{ID: i, URL: addr})
			log.Printf("[config] shard %d → %s", i, addr)
		}
	}
	return shards, nil
}

func parseSeparated(raw, sep string) []ShardNode {
	var out []ShardNode
	for i, part := range strings.Split(raw, sep) {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, ShardNode{ID: i, URL: part})
		log.Printf("[config] shard %d → %s", i, part)
	}
	return out
}

// ─── .env loader ─────────────────────────────────────────────────────────────

func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for line := 1; s.Scan(); line++ {
		text := strings.TrimSpace(s.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		k, v, found := strings.Cut(text, "=")
		if !found {
			log.Printf("[config] .env line %d: skipping malformed %q", line, text)
			continue
		}
		k = strings.TrimSpace(k)
		v = stripQuotes(strings.TrimSpace(v))
		if os.Getenv(k) == "" {
			_ = os.Setenv(k, v)
		}
	}
	return s.Err()
}

// ─── env helpers ─────────────────────────────────────────────────────────────

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var i int
	fmt.Sscanf(v, "%d", &i)
	return i
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("[config] %s: invalid duration %q, using default %s", key, v, def)
		return def
	}
	return d
}

func stripQuotes(s string) string {
	if len(s) >= 2 &&
		((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'')) {
		return s[1 : len(s)-1]
	}
	return s
}