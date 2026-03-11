package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cespare/xxhash/v2"
)

type MetadataEntry struct {
	Key   string
	Value string
}

type SSTManifest struct {
	ShardID       int      `json:"shard_id"`
	BaselineKey   string   `json:"baseline_key"`
	BaselineTS    int64    `json:"baseline_ts"`
	DeltaKeys     []string `json:"delta_keys"`
	TotalKeys     int64    `json:"total_keys"`
	LastCompacted int64    `json:"last_compacted"`
	Bucket        string   `json:"bucket"`
}

type ReconstructionMetrics struct {
	ShardID              int
	BucketName           string
	TotalDuration        time.Duration
	ManifestLoadDuration time.Duration
	BaselineLoadDuration time.Duration
	DeltasLoadDuration   time.Duration
	MergeDuration        time.Duration
	VerificationDuration time.Duration
	BaselineKeys         int
	DeltaFilesCount      int
	DeltaKeysTotal       int
	TotalUniqueKeys      int
	DownloadedBytes      int64
	ThroughputMBps       float64
	KeysPerSecond        float64
	VerificationSample   int
	MatchedKeys          int
	MismatchedKeys       int
	MissingKeys          int
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run test_reconstruction.go <shard_id> <bucket_name>")
		fmt.Println("Example: go run test_reconstruction.go 0 events-bucket")
		os.Exit(1)
	}

	shardID := os.Args[1]
	bucketName := os.Args[2]

	log.Printf("=== RocksDB Shard Reconstruction Test ===")
	log.Printf("Shard ID: %s", shardID)
	log.Printf("Bucket: %s", bucketName)
	log.Printf("")

	metrics := &ReconstructionMetrics{
		ShardID:    parseShardID(shardID),
		BucketName: bucketName,
	}

	overallStart := time.Now()

	// Step 1: Connect to S3
	client, err := newS3Client(bucketName)
	if err != nil {
		log.Fatalf("Failed to create S3 client: %v", err)
	}
	log.Printf("✓ Connected to S3")

	// Step 2: Download manifest
	manifestStart := time.Now()
	manifest, err := downloadManifest(client, bucketName, shardID)
	if err != nil {
		log.Fatalf("Failed to download manifest: %v", err)
	}
	metrics.ManifestLoadDuration = time.Since(manifestStart)
	metrics.DeltaFilesCount = len(manifest.DeltaKeys)

	log.Printf("✓ Manifest loaded in %s", metrics.ManifestLoadDuration)
	log.Printf("  - Baseline: %s", manifest.BaselineKey)
	log.Printf("  - Deltas: %d files", len(manifest.DeltaKeys))
	log.Printf("  - Total Keys: %d", manifest.TotalKeys)
	log.Printf("")

	// Step 3: Download and merge all SST files
	log.Printf("Reconstructing shard state...")
	allEntries := make(map[string]string)
	var totalBytes int64

	// Load baseline
	if manifest.BaselineKey != "" {
		baselineStart := time.Now()
		entries, bytes, err := downloadSSTWithSize(client, bucketName, manifest.BaselineKey)
		if err != nil {
			log.Fatalf("Failed to download baseline: %v", err)
		}
		metrics.BaselineLoadDuration = time.Since(baselineStart)
		metrics.BaselineKeys = len(entries)
		totalBytes += bytes

		for _, e := range entries {
			allEntries[e.Key] = e.Value
		}
		log.Printf("✓ Baseline loaded: %d keys in %s (%.2f MB, %.2f MB/s)",
			len(entries),
			metrics.BaselineLoadDuration,
			float64(bytes)/(1024*1024),
			float64(bytes)/(1024*1024)/metrics.BaselineLoadDuration.Seconds())
	}

	// Load deltas
	deltasStart := time.Now()
	deltaKeysCount := 0
	for i, deltaKey := range manifest.DeltaKeys {
		entries, bytes, err := downloadSSTWithSize(client, bucketName, deltaKey)
		if err != nil {
			log.Printf("✗ Failed to load delta %d: %v", i, err)
			continue
		}
		deltaKeysCount += len(entries)
		totalBytes += bytes

		for _, e := range entries {
			allEntries[e.Key] = e.Value
		}
		log.Printf("✓ Delta %d loaded: %d keys (%.2f MB)", i, len(entries), float64(bytes)/(1024*1024))
	}
	metrics.DeltasLoadDuration = time.Since(deltasStart)
	metrics.DeltaKeysTotal = deltaKeysCount
	metrics.DownloadedBytes = totalBytes

	// Calculate merge time (subtract download times)
	mergeStart := time.Now()
	metrics.TotalUniqueKeys = len(allEntries)
	metrics.MergeDuration = time.Since(mergeStart)

	log.Printf("")
	log.Printf("=== Reconstruction Complete ===")
	log.Printf("Total unique keys: %d", len(allEntries))
	log.Printf("Total downloaded: %.2f MB", float64(totalBytes)/(1024*1024))
	log.Printf("Deltas loaded in: %s", metrics.DeltasLoadDuration)
	log.Printf("")

	// Step 4: Verify against live RocksDB
	log.Printf("=== Verification Against Live RocksDB ===")
	verifyStart := time.Now()
	port := 4001 + parseShardID(shardID)
	matches, mismatches, missing := verifyAgainstRocksDB(allEntries, port)
	metrics.VerificationDuration = time.Since(verifyStart)
	metrics.MatchedKeys = matches
	metrics.MismatchedKeys = mismatches
	metrics.MissingKeys = missing
	metrics.VerificationSample = matches + mismatches + missing

	log.Printf("")
	log.Printf("=== Verification Results ===")
	log.Printf("✓ Matches: %d", matches)
	log.Printf("✗ Mismatches: %d", mismatches)
	log.Printf("⚠ Missing in RocksDB: %d", missing)
	log.Printf("Verification time: %s", metrics.VerificationDuration)

	metrics.TotalDuration = time.Since(overallStart)
	metrics.ThroughputMBps = float64(totalBytes) / (1024 * 1024) / metrics.TotalDuration.Seconds()
	metrics.KeysPerSecond = float64(metrics.TotalUniqueKeys) / metrics.TotalDuration.Seconds()

	if mismatches > 0 || missing > 0 {
		log.Printf("")
		log.Printf("⚠️  WARNING: Reconstruction has inconsistencies!")
		printDetailedMetrics(metrics)
		os.Exit(1)
	}

	log.Printf("")
	log.Printf("✅ SUCCESS: Shard reconstruction verified!")
	log.Printf("")

	// Print detailed metrics
	printDetailedMetrics(metrics)

	// Step 5: Sample keys
	log.Printf("")
	log.Printf("=== Sample Reconstructed Data ===")
	printSampleKeys(allEntries, 5)
}

func printDetailedMetrics(m *ReconstructionMetrics) {
	log.Printf("")
	log.Printf("╔════════════════════════════════════════════════════════════════╗")
	log.Printf("║          RECONSTRUCTION PERFORMANCE METRICS                    ║")
	log.Printf("╠════════════════════════════════════════════════════════════════╣")
	log.Printf("║ Shard ID:              %-40d║", m.ShardID)
	log.Printf("║ Bucket:                %-40s║", m.BucketName)
	log.Printf("╠════════════════════════════════════════════════════════════════╣")
	log.Printf("║ TIMING BREAKDOWN                                               ║")
	log.Printf("╠════════════════════════════════════════════════════════════════╣")
	log.Printf("║ Total Duration:        %-40s║", m.TotalDuration.Round(time.Millisecond))
	log.Printf("║   ├─ Manifest Load:    %-40s║", m.ManifestLoadDuration.Round(time.Millisecond))
	log.Printf("║   ├─ Baseline Load:    %-40s║", m.BaselineLoadDuration.Round(time.Millisecond))
	log.Printf("║   ├─ Deltas Load:      %-40s║", m.DeltasLoadDuration.Round(time.Millisecond))
	log.Printf("║   ├─ Merge Time:       %-40s║", m.MergeDuration.Round(time.Millisecond))
	log.Printf("║   └─ Verification:     %-40s║", m.VerificationDuration.Round(time.Millisecond))
	log.Printf("╠════════════════════════════════════════════════════════════════╣")
	log.Printf("║ DATA VOLUME                                                    ║")
	log.Printf("╠════════════════════════════════════════════════════════════════╣")
	log.Printf("║ Baseline Keys:         %-40d║", m.BaselineKeys)
	log.Printf("║ Delta Files:           %-40d║", m.DeltaFilesCount)
	log.Printf("║ Delta Keys (total):    %-40d║", m.DeltaKeysTotal)
	log.Printf("║ Unique Keys (final):   %-40d║", m.TotalUniqueKeys)
	log.Printf("║ Downloaded:            %-37.2f MB║", float64(m.DownloadedBytes)/(1024*1024))
	log.Printf("╠════════════════════════════════════════════════════════════════╣")
	log.Printf("║ PERFORMANCE                                                    ║")
	log.Printf("╠════════════════════════════════════════════════════════════════╣")
	log.Printf("║ Throughput:            %-34.2f MB/s║", m.ThroughputMBps)
	log.Printf("║ Keys/Second:           %-37.0f k/s║", m.KeysPerSecond/1000)
	log.Printf("║ Avg Key Size:          %-37d bytes║", m.DownloadedBytes/int64(m.TotalUniqueKeys))
	log.Printf("╠════════════════════════════════════════════════════════════════╣")
	log.Printf("║ VERIFICATION                                                   ║")
	log.Printf("╠════════════════════════════════════════════════════════════════╣")
	log.Printf("║ Sample Size:           %-40d║", m.VerificationSample)
	log.Printf("║ Matches:               %-40d║", m.MatchedKeys)
	log.Printf("║ Mismatches:            %-40d║", m.MismatchedKeys)
	log.Printf("║ Missing:               %-40d║", m.MissingKeys)
	log.Printf("║ Success Rate:          %-37.2f%%║", float64(m.MatchedKeys)/float64(m.VerificationSample)*100)
	log.Printf("╠════════════════════════════════════════════════════════════════╣")
	log.Printf("║ EFFICIENCY METRICS                                             ║")
	log.Printf("╠════════════════════════════════════════════════════════════════╣")
	log.Printf("║ Time per 1K keys:      %-37.2f ms║", m.TotalDuration.Seconds()*1000/(float64(m.TotalUniqueKeys)/1000))
	log.Printf("║ Download efficiency:   %-37.2f%%║", float64(m.BaselineLoadDuration+m.DeltasLoadDuration)/float64(m.TotalDuration)*100)
	log.Printf("║ Deduplication ratio:   %-37.2f%%║", float64(m.BaselineKeys+m.DeltaKeysTotal-m.TotalUniqueKeys)/float64(m.BaselineKeys+m.DeltaKeysTotal)*100)
	log.Printf("╚════════════════════════════════════════════════════════════════╝")
	log.Printf("")

	// JSON output for programmatic use
	jsonData, _ := json.MarshalIndent(m, "", "  ")
	log.Printf("JSON Metrics:")
	log.Printf("%s", string(jsonData))
}

func newS3Client(bucketName string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(getEnv("S3_REGION", "us-east-1")),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				getEnv("S3_ACCESS_KEY", "admin"),
				getEnv("S3_SECRET_KEY", "strongpassword"),
				"",
			),
		),
	)
	if err != nil {
		return nil, err
	}

	endpoint := getEnv("S3_ENDPOINT", "http://localhost:9002")
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.EndpointResolver = s3.EndpointResolverFunc(
			func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               endpoint,
					HostnameImmutable: true,
					SigningRegion:     cfg.Region,
				}, nil
			},
		)
		o.UsePathStyle = true
	})
	return client, nil
}

func downloadManifest(client *s3.Client, bucket, shardID string) (*SSTManifest, error) {
	key := fmt.Sprintf("rocksdb-sst/shard-%s/MANIFEST.json", shardID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var manifest SSTManifest
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func downloadSST(client *s3.Client, bucket, key string) ([]MetadataEntry, error) {
	entries, _, err := downloadSSTWithSize(client, bucket, key)
	return entries, err
}

func downloadSSTWithSize(client *s3.Client, bucket, key string) ([]MetadataEntry, int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	entries, err := parseSSTData(data)
	return entries, int64(len(data)), err
}

func parseSSTData(data []byte) ([]MetadataEntry, error) {
	if len(data) < 12 {
		return nil, fmt.Errorf("SST data too short")
	}

	storedChecksum := uint64(data[0])<<56 | uint64(data[1])<<48 |
		uint64(data[2])<<40 | uint64(data[3])<<32 |
		uint64(data[4])<<24 | uint64(data[5])<<16 |
		uint64(data[6])<<8 | uint64(data[7])

	actualChecksum := xxhash.Sum64(data[8:])
	if storedChecksum != actualChecksum {
		return nil, fmt.Errorf("checksum mismatch")
	}

	count := uint32(data[8])<<24 | uint32(data[9])<<16 |
		uint32(data[10])<<8 | uint32(data[11])

	entries := make([]MetadataEntry, 0, count)
	pos := 12

	for i := uint32(0); i < count; i++ {
		kLen := int(uint32(data[pos])<<24 | uint32(data[pos+1])<<16 |
			uint32(data[pos+2])<<8 | uint32(data[pos+3]))
		pos += 4

		key := string(data[pos : pos+kLen])
		pos += kLen

		vLen := int(uint32(data[pos])<<24 | uint32(data[pos+1])<<16 |
			uint32(data[pos+2])<<8 | uint32(data[pos+3]))
		pos += 4

		value := string(data[pos : pos+vLen])
		pos += vLen

		entries = append(entries, MetadataEntry{Key: key, Value: value})
	}

	return entries, nil
}

func verifyAgainstRocksDB(reconstructed map[string]string, port int) (matches, mismatches, missing int) {
	url := fmt.Sprintf("http://localhost:%d/get", port)
	client := &http.Client{Timeout: 2 * time.Second}

	// Sample 100 keys to verify
	keys := make([]string, 0, len(reconstructed))
	for k := range reconstructed {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	sampleSize := 100
	if len(keys) < sampleSize {
		sampleSize = len(keys)
	}

	log.Printf("Sampling %d keys for verification...", sampleSize)

	for i := 0; i < sampleSize; i++ {
		key := keys[i*len(keys)/sampleSize]
		expectedValue := reconstructed[key]

		resp, err := client.Get(fmt.Sprintf("%s?key=%s", url, key))
		if err != nil {
			missing++
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			missing++
			continue
		}

		type GetResp struct {
			Status string `json:"status"`
			Value  string `json:"value"`
		}
		var gr GetResp
		if err := json.NewDecoder(resp.Body).Decode(&gr); err != nil {
			missing++
			continue
		}

		if gr.Value == expectedValue {
			matches++
		} else {
			mismatches++
			log.Printf("Mismatch for key %s: expected=%s, got=%s", key, expectedValue, gr.Value)
		}
	}

	return matches, mismatches, missing
}

func printSampleKeys(entries map[string]string, count int) {
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if len(keys) > count {
		keys = keys[:count]
	}

	for i, k := range keys {
		value := entries[k]
		if len(value) > 100 {
			value = value[:100] + "..."
		}
		fmt.Printf("%d. %s\n   → %s\n", i+1, k, value)
	}
}

func parseShardID(s string) int {
	var id int
	fmt.Sscanf(s, "%d", &id)
	return id
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}