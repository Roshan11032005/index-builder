package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cespare/xxhash/v2"
)

// ═══════════════════════════════════════════════════════════════
//  Configuration
// ═══════════════════════════════════════════════════════════════

type ShardConfig struct {
	ID  int
	URL string
}

type MigrationConfig struct {
	OldShards       []ShardConfig
	NewShards       []ShardConfig
	S3Bucket        string
	S3Endpoint      string
	S3AccessKey     string
	S3SecretKey     string
	S3Region        string
	DryRun          bool
	BatchSize       int
	Concurrency     int
	ScanPageSize    int // Number of keys to fetch per API call
	MigrationBuffer int // Buffer size before writing to target
}

// ═══════════════════════════════════════════════════════════════
//  Migration Statistics
// ═══════════════════════════════════════════════════════════════

type MigrationStats struct {
	TotalKeys          atomic.Int64
	KeysToMigrate      atomic.Int64
	KeysMigrated       atomic.Int64
	KeysAlreadyCorrect atomic.Int64
	Errors             atomic.Int64
	ShardsScanned      atomic.Int64
	PagesScanned       atomic.Int64
	StartTime          time.Time
}

func (s *MigrationStats) PrintProgress() {
	elapsed := time.Since(s.StartTime)
	total := s.TotalKeys.Load()
	migrated := s.KeysMigrated.Load()
	correct := s.KeysAlreadyCorrect.Load()
	toMigrate := s.KeysToMigrate.Load()
	errors := s.Errors.Load()
	
	progress := float64(0)
	if toMigrate > 0 {
		progress = float64(migrated) / float64(toMigrate) * 100
	}
	
	rate := float64(migrated) / elapsed.Seconds()
	
	fmt.Printf("\n═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("⏱️  Migration Progress (elapsed: %s)\n", elapsed.Round(time.Second))
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
	fmt.Printf("  Shards Scanned:        %d\n", s.ShardsScanned.Load())
	fmt.Printf("  Pages Fetched:         %d\n", s.PagesScanned.Load())
	fmt.Printf("  Total Keys Scanned:    %d\n", total)
	fmt.Printf("  Keys Already Correct:  %d (%.1f%%)\n", correct, float64(correct)/float64(total)*100)
	fmt.Printf("  Keys To Migrate:       %d (%.1f%%)\n", toMigrate, float64(toMigrate)/float64(total)*100)
	fmt.Printf("  Keys Migrated:         %d / %d (%.1f%%)\n", migrated, toMigrate, progress)
	fmt.Printf("  Errors:                %d\n", errors)
	fmt.Printf("  Migration Rate:        %.0f keys/sec\n", rate)
	
	if toMigrate > 0 && migrated > 0 {
		remaining := toMigrate - migrated
		eta := time.Duration(float64(remaining)/rate) * time.Second
		fmt.Printf("  ETA:                   %s\n", eta.Round(time.Second))
	}
	fmt.Printf("═══════════════════════════════════════════════════════════════\n")
}

// ═══════════════════════════════════════════════════════════════
//  Paginated Scan Response
// ═══════════════════════════════════════════════════════════════

type ScanResponse struct {
	Data       map[string]string `json:"data"`
	NextCursor string            `json:"next_cursor"`
	HasMore    bool              `json:"has_more"`
	Count      int               `json:"count"`
}

// ═══════════════════════════════════════════════════════════════
//  Migration Tool
// ═══════════════════════════════════════════════════════════════

type ShardMigrator struct {
	cfg      MigrationConfig
	stats    *MigrationStats
	s3Client *s3.Client
	http     *http.Client
	
	// Migration buffer
	migrationBuffer map[int][]KeyMigration
	bufferMutex     sync.Mutex
}

type KeyMigration struct {
	Key      string
	Value    string
	OldShard int
	NewShard int
}

func NewShardMigrator(cfg MigrationConfig) (*ShardMigrator, error) {
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.S3Region),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.EndpointResolver = s3.EndpointResolverFunc(
			func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               cfg.S3Endpoint,
					HostnameImmutable: true,
					SigningRegion:     cfg.S3Region,
				}, nil
			},
		)
		o.UsePathStyle = true
	})

	return &ShardMigrator{
		cfg:             cfg,
		stats:           &MigrationStats{StartTime: time.Now()},
		s3Client:        s3Client,
		http:            &http.Client{Timeout: 30 * time.Second},
		migrationBuffer: make(map[int][]KeyMigration),
	}, nil
}

func (m *ShardMigrator) Migrate() error {
	log.Printf("\n╔═══════════════════════════════════════════════════════════════╗")
	log.Printf("║              SHARD MIGRATION TOOL (PAGINATED)                 ║")
	log.Printf("╚═══════════════════════════════════════════════════════════════╝\n")
	
	log.Printf("📋 Configuration:")
	log.Printf("   Old Shards:      %d", len(m.cfg.OldShards))
	log.Printf("   New Shards:      %d", len(m.cfg.NewShards))
	log.Printf("   S3 Bucket:       %s", m.cfg.S3Bucket)
	log.Printf("   Dry Run:         %v", m.cfg.DryRun)
	log.Printf("   Scan Page Size:  %d keys/page", m.cfg.ScanPageSize)
	log.Printf("   Migration Batch: %d keys", m.cfg.BatchSize)
	log.Printf("   Concurrency:     %d\n", m.cfg.Concurrency)

	if m.cfg.DryRun {
		log.Printf("⚠️  DRY RUN MODE - No data will be modified\n")
	}

	// Start progress reporter
	stopProgress := make(chan bool)
	go m.progressReporter(stopProgress)
	defer func() { stopProgress <- true }()

	// Start async migration workers
	stopMigration := make(chan bool)
	var migrationWg sync.WaitGroup
	
	if !m.cfg.DryRun {
		log.Printf("🚀 Starting %d migration workers...\n", m.cfg.Concurrency)
		for i := 0; i < m.cfg.Concurrency; i++ {
			migrationWg.Add(1)
			go m.migrationWorker(stopMigration, &migrationWg)
		}
	}

	// Scan and migrate concurrently
	log.Printf("═══════════════════════════════════════════════════════════════")
	log.Printf("PHASE 1: Scanning and Migrating Shards (Streaming)")
	log.Printf("═══════════════════════════════════════════════════════════════\n")

	var scanWg sync.WaitGroup
	sem := make(chan struct{}, m.cfg.Concurrency)

	for _, shard := range m.cfg.OldShards {
		scanWg.Add(1)
		sem <- struct{}{}

		go func(s ShardConfig) {
			defer scanWg.Done()
			defer func() { <-sem }()

			if err := m.scanAndMigrateShard(s); err != nil {
				log.Printf("❌ Shard %d scan failed: %v", s.ID, err)
			}
		}(shard)
	}

	scanWg.Wait()

	// Flush remaining buffer
	if !m.cfg.DryRun {
		log.Printf("\n📤 Flushing remaining migration buffer...")
		m.flushAllBuffers()
		
		// Stop migration workers
		close(stopMigration)
		migrationWg.Wait()
	}

	// Final verification
	if !m.cfg.DryRun {
		log.Printf("\n═══════════════════════════════════════════════════════════════")
		log.Printf("PHASE 2: Verifying Migration")
		log.Printf("═══════════════════════════════════════════════════════════════\n")

		if err := m.verifyMigration(); err != nil {
			return fmt.Errorf("verification failed: %w", err)
		}
	}

	m.printFinalReport()

	return nil
}

func (m *ShardMigrator) scanAndMigrateShard(shard ShardConfig) error {
	log.Printf("📖 Scanning shard %d (%s) with pagination...", shard.ID, shard.URL)

	cursor := ""
	pageNum := 0

	for {
		pageNum++
		
		// Fetch page
		scanResp, err := m.fetchPage(shard.URL, cursor, m.cfg.ScanPageSize)
		if err != nil {
			return fmt.Errorf("fetch page %d: %w", pageNum, err)
		}

		m.stats.PagesScanned.Add(1)

		// Process keys in this page
		for key, value := range scanResp.Data {
			m.stats.TotalKeys.Add(1)

			// Calculate correct shard
			refIDPart := extractRefIDForHashing(key)
			correctShard := consistentHash(refIDPart, len(m.cfg.NewShards))

			if correctShard != shard.ID {
				// Key needs migration
				m.stats.KeysToMigrate.Add(1)
				
				migration := KeyMigration{
					Key:      key,
					Value:    value,
					OldShard: shard.ID,
					NewShard: correctShard,
				}

				if m.cfg.DryRun {
					// In dry run, just count
					continue
				}

				// Add to buffer
				m.addToBuffer(migration)
			} else {
				m.stats.KeysAlreadyCorrect.Add(1)
			}
		}

		// Check if more pages
		if !scanResp.HasMore {
			break
		}

		cursor = scanResp.NextCursor
	}

	m.stats.ShardsScanned.Add(1)
	log.Printf("✓ Shard %d: scanned %d pages", shard.ID, pageNum)

	return nil
}

func (m *ShardMigrator) fetchPage(shardURL, cursor string, limit int) (*ScanResponse, error) {
	url := fmt.Sprintf("%s/scan_paginated?limit=%d", shardURL, limit)
	if cursor != "" {
		url += fmt.Sprintf("&cursor=%s", cursor)
	}

	resp, err := m.http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var scanResp ScanResponse
	if err := json.Unmarshal(body, &scanResp); err != nil {
		return nil, err
	}

	return &scanResp, nil
}

func (m *ShardMigrator) addToBuffer(migration KeyMigration) {
	m.bufferMutex.Lock()
	defer m.bufferMutex.Unlock()

	targetShard := migration.NewShard
	m.migrationBuffer[targetShard] = append(m.migrationBuffer[targetShard], migration)

	// Auto-flush if buffer reaches threshold
	if len(m.migrationBuffer[targetShard]) >= m.cfg.MigrationBuffer {
		toMigrate := m.migrationBuffer[targetShard]
		m.migrationBuffer[targetShard] = nil

		// Send to migration in background
		go m.migrateBatch(targetShard, toMigrate)
	}
}

func (m *ShardMigrator) flushAllBuffers() {
	m.bufferMutex.Lock()
	defer m.bufferMutex.Unlock()

	for targetShard, migrations := range m.migrationBuffer {
		if len(migrations) > 0 {
			log.Printf("   Flushing %d keys to shard %d", len(migrations), targetShard)
			if err := m.migrateBatch(targetShard, migrations); err != nil {
				log.Printf("   ❌ Flush failed for shard %d: %v", targetShard, err)
			}
		}
	}
}

func (m *ShardMigrator) migrationWorker(stop chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			// Check buffers and flush if needed
			m.checkAndFlushBuffers()
		}
	}
}

func (m *ShardMigrator) checkAndFlushBuffers() {
	m.bufferMutex.Lock()
	
	for targetShard, migrations := range m.migrationBuffer {
		if len(migrations) >= m.cfg.BatchSize {
			toMigrate := migrations[:m.cfg.BatchSize]
			m.migrationBuffer[targetShard] = migrations[m.cfg.BatchSize:]
			
			m.bufferMutex.Unlock()
			m.migrateBatch(targetShard, toMigrate)
			m.bufferMutex.Lock()
		}
	}
	
	m.bufferMutex.Unlock()
}

func (m *ShardMigrator) migrateBatch(targetShard int, migrations []KeyMigration) error {
	if targetShard >= len(m.cfg.NewShards) {
		return fmt.Errorf("invalid target shard %d", targetShard)
	}

	shardURL := m.cfg.NewShards[targetShard].URL

	type KV struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	kvs := make([]KV, len(migrations))
	for i, migration := range migrations {
		kvs[i] = KV{Key: migration.Key, Value: migration.Value}
	}

	data, err := json.Marshal(kvs)
	if err != nil {
		m.stats.Errors.Add(int64(len(migrations)))
		return err
	}

	url := shardURL + "/batch_put"
	resp, err := m.http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		m.stats.Errors.Add(int64(len(migrations)))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		m.stats.Errors.Add(int64(len(migrations)))
		return fmt.Errorf("shard returned status %d", resp.StatusCode)
	}

	m.stats.KeysMigrated.Add(int64(len(migrations)))
	return nil
}

func (m *ShardMigrator) verifyMigration() error {
	log.Printf("   Verifying all keys are on correct shards...")
	
	misplaced := 0
	totalChecked := 0

	for _, shard := range m.cfg.NewShards {
		log.Printf("   Verifying shard %d...", shard.ID)
		
		cursor := ""
		for {
			scanResp, err := m.fetchPage(shard.URL, cursor, m.cfg.ScanPageSize)
			if err != nil {
				log.Printf("   ⚠️  Could not verify shard %d: %v", shard.ID, err)
				break
			}

			for key := range scanResp.Data {
				totalChecked++
				refIDPart := extractRefIDForHashing(key)
				correctShard := consistentHash(refIDPart, len(m.cfg.NewShards))

				if correctShard != shard.ID {
					log.Printf("   ❌ Key %s is on shard %d but should be on shard %d", key, shard.ID, correctShard)
					misplaced++
				}
			}

			if !scanResp.HasMore {
				break
			}
			cursor = scanResp.NextCursor
		}
	}

	log.Printf("   Verified %d keys", totalChecked)

	if misplaced > 0 {
		return fmt.Errorf("%d keys are still misplaced", misplaced)
	}

	log.Printf("   ✅ All keys verified on correct shards!")
	return nil
}

func (m *ShardMigrator) progressReporter(stop chan bool) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			m.stats.PrintProgress()
		}
	}
}

func (m *ShardMigrator) printFinalReport() {
	elapsed := time.Since(m.stats.StartTime)
	
	fmt.Printf("\n\n")
	fmt.Printf("╔═══════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║              MIGRATION COMPLETE                               ║\n")
	fmt.Printf("╚═══════════════════════════════════════════════════════════════╝\n\n")
	
	fmt.Printf("⏱️  Total Time: %s\n\n", elapsed.Round(time.Second))
	
	fmt.Printf("📊 Final Statistics:\n")
	fmt.Printf("   Shards Scanned:       %d\n", m.stats.ShardsScanned.Load())
	fmt.Printf("   Pages Fetched:        %d\n", m.stats.PagesScanned.Load())
	fmt.Printf("   Total Keys:           %d\n", m.stats.TotalKeys.Load())
	fmt.Printf("   Keys Already Correct: %d\n", m.stats.KeysAlreadyCorrect.Load())
	fmt.Printf("   Keys Migrated:        %d\n", m.stats.KeysMigrated.Load())
	fmt.Printf("   Errors:               %d\n\n", m.stats.Errors.Load())
	
	rate := float64(m.stats.KeysMigrated.Load()) / elapsed.Seconds()
	fmt.Printf("   Migration Rate:       %.0f keys/sec\n", rate)
	
	if m.stats.Errors.Load() == 0 {
		fmt.Printf("\n✅ Migration completed successfully!\n")
	} else {
		fmt.Printf("\n⚠️  Migration completed with %d errors\n", m.stats.Errors.Load())
	}
}

// ═══════════════════════════════════════════════════════════════
//  Utilities
// ═══════════════════════════════════════════════════════════════

func consistentHash(key string, numShards int) int {
	return int(xxhash.Sum64String(key) % uint64(numShards))
}

func extractRefIDForHashing(key string) string {
	parts := strings.Split(key, ":")
	if len(parts) >= 2 && parts[0] == "ref" {
		return parts[0] + ":" + parts[1]
	}
	return key
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var i int
	fmt.Sscanf(v, "%d", &i)
	return i
}

func getEnvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v == "true" || v == "1" || v == "yes"
}

// ═══════════════════════════════════════════════════════════════
//  Main
// ═══════════════════════════════════════════════════════════════

func main() {
	cfg := MigrationConfig{
		OldShards: []ShardConfig{
			{ID: 0, URL: getEnv("OLD_SHARD_0", "http://localhost:4001")},
			{ID: 1, URL: getEnv("OLD_SHARD_1", "http://localhost:4002")},
			{ID: 2, URL: getEnv("OLD_SHARD_2", "http://localhost:4003")},
		},
		NewShards: []ShardConfig{
			{ID: 0, URL: getEnv("NEW_SHARD_0", "http://localhost:4001")},
			{ID: 1, URL: getEnv("NEW_SHARD_1", "http://localhost:4002")},
			{ID: 2, URL: getEnv("NEW_SHARD_2", "http://localhost:4003")},
			{ID: 3, URL: getEnv("NEW_SHARD_3", "http://localhost:4004")},
		},
		S3Bucket:        getEnv("S3_BUCKET", "events-bucket"),
		S3Endpoint:      getEnv("S3_ENDPOINT", "http://localhost:9002"),
		S3AccessKey:     getEnv("S3_ACCESS_KEY", "admin"),
		S3SecretKey:     getEnv("S3_SECRET_KEY", "strongpassword"),
		S3Region:        getEnv("S3_REGION", "us-east-1"),
		DryRun:          getEnvBool("DRY_RUN", true),
		BatchSize:       getEnvInt("BATCH_SIZE", 100),
		ScanPageSize:    getEnvInt("SCAN_PAGE_SIZE", 1000),
		MigrationBuffer: getEnvInt("MIGRATION_BUFFER", 500),
		Concurrency:     getEnvInt("CONCURRENCY", 3),
	}

	migrator, err := NewShardMigrator(cfg)
	if err != nil {
		log.Fatalf("Failed to create migrator: %v", err)
	}

	if err := migrator.Migrate(); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}
}