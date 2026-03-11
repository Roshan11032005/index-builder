// Package builder contains the IndexBuilder — the main processing engine.
// It orchestrates S3 segment scanning, RocksDB index writes (via gRPC),
// SST snapshot uploads, and compaction.
package builder

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/cespare/xxhash/v2"

	"index-builder/config"
	"index-builder/internal/cursor"
	"index-builder/internal/s3store"
	"index-builder/internal/shard"
	"index-builder/internal/sst"
)

// ─── Domain types ─────────────────────────────────────────────────────────────

type event struct {
	eventID     string
	referenceID string
	timestamp   int64
	payload     string // pre-formatted RocksDB value
}

type segmentFile struct {
	bucket    string
	objectKey string
	timestamp int64
	writerID  string
	segmentID string
	size      int64
}

// SSTManifest tracks which SST files exist for a shard in one bucket.
type SSTManifest struct {
	ShardID       int      `json:"shard_id"`
	Bucket        string   `json:"bucket"`
	BaselineKey   string   `json:"baseline_key"`
	BaselineTS    int64    `json:"baseline_ts"`
	DeltaKeys     []string `json:"delta_keys"`
	TotalKeys     int64    `json:"total_keys"`
	LastCompacted int64    `json:"last_compacted"`
}

// ─── Metrics ──────────────────────────────────────────────────────────────────

// Metrics exposes atomic counters for the operational HTTP handler.
type Metrics struct {
	SegmentsProcessed atomic.Int64
	EventsIndexed     atomic.Int64
	Compactions       atomic.Int64
	SSTUploaded       atomic.Int64
	LastRunMs         atomic.Int64
	LastCompactionMs  atomic.Int64
}

// ─── IndexBuilder ─────────────────────────────────────────────────────────────

// IndexBuilder is the main service.
type IndexBuilder struct {
	cfg       *config.Config
	shards    *shard.Pool
	s3clients map[string]*s3store.Client // keyed by bucket name
	cur       *cursor.Manager
	Metrics   Metrics

	ctx    context.Context
	cancel context.CancelFunc
}

// New constructs an IndexBuilder from already-initialised dependencies.
func New(
	cfg *config.Config,
	shards *shard.Pool,
	s3clients map[string]*s3store.Client,
	cur *cursor.Manager,
) *IndexBuilder {
	ctx, cancel := context.WithCancel(context.Background())
	return &IndexBuilder{
		cfg:       cfg,
		shards:    shards,
		s3clients: s3clients,
		cur:       cur,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start launches background goroutines. Call Stop to shut down gracefully.
func (b *IndexBuilder) Start() {
	b.cur.Load(b.ctx)
	if err := b.cur.EnsureInS3(b.ctx); err != nil {
		log.Printf("[builder] cursor S3 init warning: %v", err)
	}
	go b.mainLoop()
}

// Stop signals the builder to stop after the current cycle.
func (b *IndexBuilder) Stop() {
	log.Printf("[builder] shutting down…")
	b.cancel()
}

// ─── Main loop ────────────────────────────────────────────────────────────────

func (b *IndexBuilder) mainLoop() {
	log.Printf("[builder] started (poll=%s)", b.cfg.PollInterval)

	for {
		select {
		case <-b.ctx.Done():
			log.Printf("[builder] main loop stopped")
			return
		default:
		}

		start := time.Now()

		log.Printf("[builder] ── Phase 1: Indexing ──")
		b.runIndexing()

		log.Printf("[builder] ── Phase 2: Compaction check ──")
		b.RunCompactionCheck()

		elapsed := time.Since(start)
		b.Metrics.LastRunMs.Store(elapsed.Milliseconds())
		log.Printf("[builder] cycle done in %s, sleeping %s", elapsed, b.cfg.PollInterval)

		select {
		case <-b.ctx.Done():
			return
		case <-time.After(b.cfg.PollInterval):
		}
	}
}

// ─── Phase 1: Indexing ───────────────────────────────────────────────────────

func (b *IndexBuilder) runIndexing() {
	var all []segmentFile
	for _, bc := range b.cfg.Buckets {
		files, err := b.listNewSegments(bc.Name)
		if err != nil {
			log.Printf("[builder] list %s: %v", bc.Name, err)
			continue
		}
		log.Printf("[builder] bucket %s: %d new segment(s)", bc.Name, len(files))
		all = append(all, files...)
	}

	if len(all) == 0 {
		log.Printf("[builder] no new segments")
		return
	}

	// Sort deterministically: timestamp → writerID → segmentID
	sort.Slice(all, func(i, j int) bool {
		a, bx := all[i], all[j]
		if a.timestamp != bx.timestamp {
			return a.timestamp < bx.timestamp
		}
		if a.writerID != bx.writerID {
			return a.writerID < bx.writerID
		}
		return a.segmentID < bx.segmentID
	})

	log.Printf("[builder] processing %d segment(s)", len(all))

	for _, seg := range all {
		if err := b.processSegment(seg); err != nil {
			log.Printf("[builder] %s/%s: %v — stopping this cycle", seg.bucket, seg.objectKey, err)
			return
		}
		if err := b.cur.Advance(b.ctx, seg.bucket, seg.objectKey); err != nil {
			log.Printf("[builder] CRITICAL: cursor save failed: %v — stopping", err)
			return
		}
		b.Metrics.SegmentsProcessed.Add(1)
	}
}

func (b *IndexBuilder) listNewSegments(bucketName string) ([]segmentFile, error) {
	cli := b.s3clients[bucketName]
	lastProcessed := b.cur.Get(bucketName)

	objects, err := cli.ListWithPrefix(b.ctx, "events/")
	if err != nil {
		return nil, err
	}

	var files []segmentFile
	for _, obj := range objects {
		key := aws.ToString(obj.Key)
		if !strings.HasSuffix(key, ".ndjson") {
			continue
		}
		if lastProcessed != "" && key <= lastProcessed {
			continue
		}
		ts, writerID, segID, err := parseSegmentPath(key)
		if err != nil {
			log.Printf("[builder] skipping malformed path %s: %v", key, err)
			continue
		}
		files = append(files, segmentFile{
			bucket:    bucketName,
			objectKey: key,
			timestamp: ts,
			writerID:  writerID,
			segmentID: segID,
			size:      aws.ToInt64(obj.Size),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].objectKey < files[j].objectKey
	})
	return files, nil
}

func parseSegmentPath(key string) (ts int64, writerID, segmentID string, err error) {
	path := strings.TrimPrefix(key, "events/")
	parts := strings.Split(path, "/")
	if len(parts) != 3 {
		return 0, "", "", fmt.Errorf("expected 3 path parts, got %d", len(parts))
	}
	ts, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", "", fmt.Errorf("invalid timestamp: %w", err)
	}
	return ts, parts[1], strings.TrimSuffix(parts[2], ".ndjson"), nil
}

// ─── Segment processing ───────────────────────────────────────────────────────

func (b *IndexBuilder) processSegment(seg segmentFile) error {
	log.Printf("[builder] processing %s/%s (%.1f KB)", seg.bucket, seg.objectKey, float64(seg.size)/1024)

	cli := b.s3clients[seg.bucket]
	body, err := cli.GetReader(b.ctx, seg.objectKey)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	defer body.Close()

	var events []event
	startByte := int64(0)

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 10<<20), 10<<20) // 10 MB buffer

	for scanner.Scan() {
		line := scanner.Bytes()
		lineLen := int64(len(line))

		if lineLen == 0 {
			startByte++
			continue
		}

		var raw map[string]any
		if err := json.Unmarshal(line, &raw); err != nil {
			log.Printf("[builder] skipping malformed line in %s: %v", seg.objectKey, err)
			startByte += lineLen + 1
			continue
		}

		eventID := strField(raw, "_event_id")
		refID := strField(raw, "reference_id")
		if eventID == "" || refID == "" {
			startByte += lineLen + 1
			continue
		}

		ts := seg.timestamp
		if tsStr := strField(raw, "_event_timestamp"); tsStr != "" {
			if parsed, err := parseTimestamp(tsStr); err == nil {
				ts = parsed
			}
		}

		endByte := startByte + lineLen
		value := fmt.Sprintf("%s|%s|%d|%d", seg.bucket, seg.objectKey, startByte, endByte)

		events = append(events, event{
			eventID:     eventID,
			referenceID: refID,
			timestamp:   ts,
			payload:     value,
		})
		startByte = endByte + 1
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan: %w", err)
	}
	if len(events) == 0 {
		log.Printf("[builder] no events in %s/%s", seg.bucket, seg.objectKey)
		return nil
	}

	entries := make([]sst.Entry, len(events))
	for i, e := range events {
		entries[i] = sst.Entry{
			Key:   fmt.Sprintf("ref:%s:time:%d:txn:%s", e.referenceID, e.timestamp, e.eventID),
			Value: e.payload,
		}
	}

	if err := b.writeToShards(entries); err != nil {
		return fmt.Errorf("shard write: %w", err)
	}

	if err := b.uploadSST(entries, seg); err != nil {
		log.Printf("[builder] SST upload warning (non-fatal): %v", err)
	}

	b.Metrics.EventsIndexed.Add(int64(len(events)))
	log.Printf("[builder] indexed %d events from %s/%s", len(events), seg.bucket, seg.objectKey)
	return nil
}

// ─── RocksDB writes via gRPC ─────────────────────────────────────────────────

func (b *IndexBuilder) writeToShards(entries []sst.Entry) error {
	// Group entries by shard — hash only the ref:REF_ID portion for consistency.
	buckets := make(map[int][]shard.KV)
	for _, e := range entries {
		id := b.shardFor(e.Key)
		buckets[id] = append(buckets[id], shard.KV{Key: e.Key, Value: e.Value})
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(buckets))

	for shardID, kvs := range buckets {
		wg.Add(1)
		go func(id int, pairs []shard.KV) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(b.ctx, 10*time.Second)
			defer cancel()
			if err := b.shards.BatchPut(ctx, id, pairs); err != nil {
				errCh <- fmt.Errorf("shard %d batch_put: %w", id, err)
			}
		}(shardID, kvs)
	}

	wg.Wait()
	close(errCh)

	var errs []string
	for e := range errCh {
		errs = append(errs, e.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("shard writes failed: %s", strings.Join(errs, "; "))
	}
	return nil
}

// shardFor returns the shard index for a RocksDB key.
// It hashes only the "ref:REF_ID" prefix so all events for the same reference
// land on the same shard.
func (b *IndexBuilder) shardFor(key string) int {
	parts := strings.SplitN(key, ":", 3) // ["ref", "REF_ID", ...]
	hashKey := key
	if len(parts) >= 2 && parts[0] == "ref" {
		hashKey = parts[0] + ":" + parts[1]
	}
	return int(xxhash.Sum64String(hashKey) % uint64(b.shards.Len()))
}

// ─── SST upload ───────────────────────────────────────────────────────────────

func (b *IndexBuilder) uploadSST(entries []sst.Entry, seg segmentFile) error {
	// Group by shard (same hashing as writeToShards).
	byShards := make(map[int][]sst.Entry)
	for _, e := range entries {
		id := b.shardFor(e.Key)
		byShards[id] = append(byShards[id], e)
	}

	var wg sync.WaitGroup
	for shardID, shardEntries := range byShards {
		if len(shardEntries) == 0 {
			continue
		}
		wg.Add(1)
		go func(id int, es []sst.Entry) {
			defer wg.Done()

			sort.Slice(es, func(i, j int) bool { return es[i].Key < es[j].Key })

			data, checksum := sst.Build(es)
			sstKey := fmt.Sprintf("rocksdb-sst/shard-%d/delta/%d_%s_%s.sst",
				id, seg.timestamp, seg.writerID, seg.segmentID)

			meta := map[string]string{
				"shard":     strconv.Itoa(id),
				"checksum":  strconv.FormatUint(checksum, 10),
				"key-count": strconv.Itoa(len(es)),
				"source":    seg.objectKey,
				"timestamp": strconv.FormatInt(seg.timestamp, 10),
				"writer":    seg.writerID,
				"segment":   seg.segmentID,
			}

			cli := b.s3clients[seg.bucket]
			ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
			defer cancel()

			if err := cli.PutBytes(ctx, sstKey, "application/octet-stream", data, meta); err != nil {
				log.Printf("[builder] SST upload shard %d: %v", id, err)
				return
			}

			if err := b.updateManifest(id, seg.bucket, sstKey, int64(len(es))); err != nil {
				log.Printf("[builder] manifest update shard %d: %v", id, err)
			}
			b.Metrics.SSTUploaded.Add(1)
		}(shardID, shardEntries)
	}
	wg.Wait()
	return nil
}

// ─── Compaction ───────────────────────────────────────────────────────────────

// RunCompactionCheck is exported so the HTTP handler can trigger it manually.
func (b *IndexBuilder) RunCompactionCheck() {
	for _, bc := range b.cfg.Buckets {
		for _, sh := range b.cfg.Shards {
			manifest, err := b.loadManifest(sh.ID, bc.Name)
			if err != nil {
				continue
			}

			deltaCount := len(manifest.DeltaKeys)
			age := time.Since(time.Unix(manifest.LastCompacted, 0))
			needsCompaction := deltaCount >= b.cfg.CompactionDeltaLimit || age >= b.cfg.CompactionMaxAge

			if needsCompaction {
				log.Printf("[builder] compaction triggered shard %d bucket %s (deltas=%d age=%s)",
					sh.ID, bc.Name, deltaCount, age.Round(time.Minute))
				b.compact(sh.ID, bc.Name, manifest)
			} else {
				log.Printf("[builder] shard %d bucket %s OK (deltas=%d)", sh.ID, bc.Name, deltaCount)
			}
		}
	}
}

func (b *IndexBuilder) compact(shardID int, bucketName string, manifest *SSTManifest) {
	start := time.Now()
	cli := b.s3clients[bucketName]

	merged := make(map[string]string)

	// Load existing baseline
	if manifest.BaselineKey != "" {
		baseEntries, err := b.downloadSSTEntries(bucketName, manifest.BaselineKey)
		if err != nil {
			log.Printf("[builder] compact: load baseline shard %d: %v", shardID, err)
			return
		}
		for _, e := range baseEntries {
			merged[e.Key] = e.Value
		}
		log.Printf("[builder] compact: loaded %d keys from baseline", len(baseEntries))
	}

	// Merge deltas (later writes overwrite earlier)
	for _, dk := range manifest.DeltaKeys {
		es, err := b.downloadSSTEntries(bucketName, dk)
		if err != nil {
			log.Printf("[builder] compact: load delta %s: %v", dk, err)
			return
		}
		for _, e := range es {
			merged[e.Key] = e.Value
		}
	}

	// Build sorted slice
	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	sortedEntries := make([]sst.Entry, len(keys))
	for i, k := range keys {
		sortedEntries[i] = sst.Entry{Key: k, Value: merged[k]}
	}

	data, checksum := sst.Build(sortedEntries)
	newBaseKey := fmt.Sprintf("rocksdb-sst/shard-%d/baseline/compacted-%d.sst",
		shardID, time.Now().Unix())

	meta := map[string]string{
		"shard":     strconv.Itoa(shardID),
		"checksum":  strconv.FormatUint(checksum, 10),
		"key-count": strconv.Itoa(len(sortedEntries)),
		"type":      "baseline",
	}

	ctx, cancel := context.WithTimeout(b.ctx, 120*time.Second)
	defer cancel()

	if err := cli.PutBytes(ctx, newBaseKey, "application/octet-stream", data, meta); err != nil {
		log.Printf("[builder] compact: upload failed shard %d: %v", shardID, err)
		return
	}

	oldDeltas := manifest.DeltaKeys
	oldBaseline := manifest.BaselineKey

	newManifest := &SSTManifest{
		ShardID:       shardID,
		Bucket:        bucketName,
		BaselineKey:   newBaseKey,
		BaselineTS:    time.Now().Unix(),
		DeltaKeys:     []string{},
		TotalKeys:     int64(len(sortedEntries)),
		LastCompacted: time.Now().Unix(),
	}
	if err := b.saveManifest(shardID, bucketName, newManifest); err != nil {
		log.Printf("[builder] compact: manifest save shard %d: %v", shardID, err)
		return
	}

	// Clean up old files (best-effort)
	toDelete := append(oldDeltas, oldBaseline)
	filtered := toDelete[:0]
	for _, k := range toDelete {
		if k != "" {
			filtered = append(filtered, k)
		}
	}
	if len(filtered) > 0 {
		if err := cli.DeleteMany(b.ctx, filtered); err != nil {
			log.Printf("[builder] compact: cleanup warning: %v", err)
		}
	}

	elapsed := time.Since(start)
	b.Metrics.Compactions.Add(1)
	b.Metrics.LastCompactionMs.Store(elapsed.Milliseconds())
	log.Printf("[builder] compact: shard %d done in %s (%d keys)", shardID, elapsed, len(sortedEntries))
}

func (b *IndexBuilder) downloadSSTEntries(bucketName, key string) ([]sst.Entry, error) {
	cli := b.s3clients[bucketName]
	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	defer cancel()

	data, err := cli.GetBytes(ctx, key)
	if err != nil {
		return nil, err
	}
	return sst.Parse(data)
}

// ─── Manifest helpers ─────────────────────────────────────────────────────────

func (b *IndexBuilder) manifestKey(shardID int) string {
	return fmt.Sprintf("rocksdb-sst/shard-%d/MANIFEST.json", shardID)
}

func (b *IndexBuilder) loadManifest(shardID int, bucketName string) (*SSTManifest, error) {
	cli := b.s3clients[bucketName]
	var m SSTManifest
	if err := cli.GetJSON(b.ctx, b.manifestKey(shardID), &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (b *IndexBuilder) saveManifest(shardID int, bucketName string, m *SSTManifest) error {
	cli := b.s3clients[bucketName]
	return cli.PutJSON(b.ctx, b.manifestKey(shardID), m, nil)
}

func (b *IndexBuilder) updateManifest(shardID int, bucketName, newDeltaKey string, keyCount int64) error {
	m, err := b.loadManifest(shardID, bucketName)
	if err != nil {
		m = &SSTManifest{ShardID: shardID, Bucket: bucketName}
	}
	m.DeltaKeys = append(m.DeltaKeys, newDeltaKey)
	m.TotalKeys += keyCount
	return b.saveManifest(shardID, bucketName, m)
}

// ─── Utilities ────────────────────────────────────────────────────────────────

func strField(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func parseTimestamp(s string) (int64, error) {
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z07:00",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t.Unix(), nil
		}
	}
	return 0, fmt.Errorf("cannot parse timestamp %q", s)
}



// ensure unused imports don't cause compile errors in this file