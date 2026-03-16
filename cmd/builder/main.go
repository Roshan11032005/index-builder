package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"index-builder/config"
	"index-builder/internal/builder"
	"index-builder/internal/cursor"
	"index-builder/internal/httpserver"
	"index-builder/internal/s3store"
	"index-builder/internal/shard"
)

func main() {
	// ── Config ─────────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	// ── S3 clients (one per bucket) ────────────────────────────────────────────
	s3clients := make(map[string]*s3store.Client, len(cfg.Buckets))
	s3list := make([]*s3store.Client, 0, len(cfg.Buckets))
	for _, bc := range cfg.Buckets {
		cli, err := s3store.New(bc)
		if err != nil {
			log.Fatalf("s3 init bucket %s: %v", bc.Name, err)
		}
		s3clients[bc.Name] = cli
		s3list = append(s3list, cli)
		log.Printf("[main] S3 ready: bucket=%s endpoint=%s", bc.Name, bc.Endpoint)
	}

	// ── Shard pool (gRPC connections) ──────────────────────────────────────────
	addrs := make(map[int]string, len(cfg.Shards))
	for _, s := range cfg.Shards {
		addrs[s.ID] = s.URL
	}
	pool, err := shard.NewPool(addrs)
	if err != nil {
		log.Fatalf("shard pool: %v", err)
	}
	defer pool.Close()

	// Connectivity check (non-fatal — shards may not be ready yet at startup)
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := pool.PingAll(pingCtx); err != nil {
		log.Printf("[main] ⚠️  shard ping warning: %v", err)
	}
	pingCancel()

	// ── Cursor ─────────────────────────────────────────────────────────────────
	cur := cursor.New(pool, cfg.CursorShard().ID, s3list)

	// ── Builder ────────────────────────────────────────────────────────────────
	b := builder.New(cfg, pool, s3clients, cur)
	b.Start()
	defer b.Stop()

	// ── Operational HTTP server ────────────────────────────────────────────────
	srv := httpserver.New(cfg, b, cur)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalf("http server: %v", err)
		}
	}()

	log.Printf("🚀  index-builder started")
	log.Printf("    shards=%d  buckets=%d  poll=%s  http=:%s",
		len(cfg.Shards), len(cfg.Buckets), cfg.PollInterval, cfg.HTTPPort)

	// ── Graceful shutdown ──────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Printf("[main] shutting down…")
}