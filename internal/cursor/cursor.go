// Package cursor manages the "last processed object" cursor.
// The cursor is persisted to both:
//   - The designated RocksDB shard  (primary, fast)
//   - Every S3 bucket               (fallback / redundancy)
package cursor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"index-builder/internal/s3store"
	"index-builder/internal/shard"
)

const (
	rocksDBKey = "cursor:last_processed"
	s3Key      = "_internal/cursor.json"
)

// State maps bucket names to the last processed object key in that bucket.
type State struct {
	Processed map[string]string `json:"processed"`
}

// Manager loads and saves the cursor using gRPC (RocksDB) and S3.
type Manager struct {
	mu           sync.Mutex
	state        State
	shards       *shard.Pool
	cursorShard  int
	s3clients    []*s3store.Client
}

// New returns a Manager. cursorShard is the shard index used for the RocksDB
// cursor key. s3clients should include one client per configured bucket.
func New(shards *shard.Pool, cursorShard int, s3clients []*s3store.Client) *Manager {
	return &Manager{
		state:       State{Processed: make(map[string]string)},
		shards:      shards,
		cursorShard: cursorShard,
		s3clients:   s3clients,
	}
}

// Load tries RocksDB first, then falls back to any S3 bucket.
// If no cursor is found anywhere it returns a fresh empty state without error.
func (m *Manager) Load(ctx context.Context) {
	if err := m.loadFromRocksDB(ctx); err == nil {
		log.Printf("[cursor] loaded from RocksDB: %v", m.state.Processed)
		go func() {
			if err := m.replicateToS3(context.Background()); err != nil {
				log.Printf("[cursor] S3 sync warning: %v", err)
			}
		}()
		return
	}

	log.Printf("[cursor] not in RocksDB, trying S3…")
	if err := m.loadFromS3(ctx); err == nil {
		log.Printf("[cursor] loaded from S3: %v", m.state.Processed)
		if err := m.saveToRocksDB(ctx); err != nil {
			log.Printf("[cursor] backfill to RocksDB warning: %v", err)
		}
		return
	}

	log.Printf("[cursor] no cursor found anywhere — starting from scratch")
}

// Get returns the last processed key for bucketName (empty string if none).
func (m *Manager) Get(bucketName string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state.Processed[bucketName]
}

// Advance updates the in-memory cursor for bucketName and persists it.
func (m *Manager) Advance(ctx context.Context, bucketName, objectKey string) error {
	m.mu.Lock()
	m.state.Processed[bucketName] = objectKey
	m.mu.Unlock()

	if err := m.replicateToS3(ctx); err != nil {
		return fmt.Errorf("cursor: S3 save: %w", err)
	}
	if err := m.saveToRocksDB(ctx); err != nil {
		log.Printf("[cursor] RocksDB save warning (non-fatal): %v", err)
	}
	return nil
}

// EnsureInS3 writes the current cursor to every S3 bucket (idempotent).
func (m *Manager) EnsureInS3(ctx context.Context) error {
	return m.replicateToS3(ctx)
}

// ─── RocksDB ─────────────────────────────────────────────────────────────────

func (m *Manager) saveToRocksDB(ctx context.Context) error {
	m.mu.Lock()
	data, err := json.Marshal(m.state)
	m.mu.Unlock()
	if err != nil {
		return err
	}

	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := m.shards.Put(tctx, m.cursorShard, rocksDBKey, string(data)); err != nil {
		return fmt.Errorf("cursor: rocksdb put: %w", err)
	}
	log.Printf("[cursor] saved to RocksDB shard %d", m.cursorShard)
	return nil
}

func (m *Manager) loadFromRocksDB(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	val, err := m.shards.Get(tctx, m.cursorShard, rocksDBKey)
	if err != nil || val == "" {
		return fmt.Errorf("cursor: rocksdb get: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return json.Unmarshal([]byte(val), &m.state)
}

// ─── S3 ───────────────────────────────────────────────────────────────────────

func (m *Manager) replicateToS3(ctx context.Context) error {
	m.mu.Lock()
	data, err := json.Marshal(m.state)
	m.mu.Unlock()
	if err != nil {
		return err
	}

	meta := map[string]string{
		"updated-at": time.Now().UTC().Format(time.RFC3339),
		"version":    "2",
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(m.s3clients))
	var ok atomic.Int32

	for _, cli := range m.s3clients {
		wg.Add(1)
		go func(c *s3store.Client) {
			defer wg.Done()
			tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if err := c.PutBytes(tctx, s3Key, "application/json", data, meta); err != nil {
				errCh <- err
			} else {
				ok.Add(1)
				log.Printf("[cursor] replicated to s3://%s/%s", c.BucketName(), s3Key)
			}
		}(cli)
	}
	wg.Wait()
	close(errCh)

	if ok.Load() == 0 {
		var errs []error
		for e := range errCh {
			errs = append(errs, e)
		}
		return fmt.Errorf("cursor: S3 save failed on all buckets: %v", errs)
	}
	return nil
}

func (m *Manager) loadFromS3(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for _, cli := range m.s3clients {
		var s State
		if err := cli.GetJSON(tctx, s3Key, &s); err != nil {
			continue
		}
		m.mu.Lock()
		m.state = s
		m.mu.Unlock()
		log.Printf("[cursor] loaded from s3://%s/%s", cli.BucketName(), s3Key)
		return nil
	}
	return fmt.Errorf("cursor: not found in any S3 bucket")
}