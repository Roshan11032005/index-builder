// Package httpserver exposes the internal operational endpoints:
// health, metrics, force-compact, cursor status, and shard list.
package httpserver

import (
	"encoding/json"
	"log"
	"net/http"

	"index-builder/config"
	"index-builder/internal/builder"
	"index-builder/internal/cursor"
)

// Server is the operational HTTP server.
type Server struct {
	cfg     *config.Config
	builder *builder.IndexBuilder
	cursor  *cursor.Manager
}

// New returns a configured Server.
func New(cfg *config.Config, b *builder.IndexBuilder, cur *cursor.Manager) *Server {
	return &Server{cfg: cfg, builder: b, cursor: cur}
}

// ListenAndServe starts the server on the configured port (blocks).
func (s *Server) ListenAndServe() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/health", s.handleHealth)
	mux.HandleFunc("/internal/metrics", s.handleMetrics)
	mux.HandleFunc("/internal/compact", s.handleForceCompact)
	mux.HandleFunc("/internal/cursor", s.handleCursor)
	mux.HandleFunc("/internal/shards", s.handleShards)

	addr := ":" + s.cfg.HTTPPort
	log.Printf("[http] operational server on %s", addr)
	return http.ListenAndServe(addr, mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"status":  "ok",
		"buckets": len(s.cfg.Buckets),
		"shards":  len(s.cfg.Shards),
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	m := &s.builder.Metrics
	writeJSON(w, map[string]any{
		"segments_processed": m.SegmentsProcessed.Load(),
		"events_indexed":     m.EventsIndexed.Load(),
		"compactions":        m.Compactions.Load(),
		"sst_uploaded":       m.SSTUploaded.Load(),
		"last_run_ms":        m.LastRunMs.Load(),
		"last_compaction_ms": m.LastCompactionMs.Load(),
	})
}

func (s *Server) handleForceCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	log.Printf("[http] force compaction triggered")
	go s.builder.RunCompactionCheck()
	writeJSON(w, map[string]string{"status": "compaction started"})
}

func (s *Server) handleCursor(w http.ResponseWriter, _ *http.Request) {
	// Expose per-bucket last-processed keys.
	state := make(map[string]string)
	for _, bc := range s.cfg.Buckets {
		state[bc.Name] = s.cursor.Get(bc.Name)
	}
	writeJSON(w, state)
}

func (s *Server) handleShards(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{
		"total": len(s.cfg.Shards),
		"shards": s.cfg.Shards,
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[http] encode: %v", err)
	}
}