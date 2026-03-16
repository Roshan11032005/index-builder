// Package shard provides a gRPC client pool for the RocksDB shard nodes.
// It is the only package that imports the generated proto bindings.
package shard

import (
	"context"
	"fmt"
	"log"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "index-builder/proto/rocksdb"
)

// ─── entry ────────────────────────────────────────────────────────────────────

// entry holds the live connection and derived client for one shard.
type entry struct {
	conn   *grpc.ClientConn
	client pb.RocksDBServiceClient
}

// ─── Pool ─────────────────────────────────────────────────────────────────────

// Pool manages one gRPC connection + client per shard.
// Shard IDs are arbitrary integers (assigned by the consistent ring) rather
// than contiguous slice indices, and the membership set can change at runtime
// via AddShard / RemoveShard.
type Pool struct {
	mu    sync.RWMutex
	nodes map[int]*entry // keyed by shard ID
}

// NewPool dials all shards described by addrs and returns a ready Pool.
// addrs maps shard ID → "host:port".
func NewPool(addrs map[int]string) (*Pool, error) {
	p := &Pool{nodes: make(map[int]*entry, len(addrs))}

	for id, addr := range addrs {
		if err := p.dial(id, addr); err != nil {
			p.Close()
			return nil, err
		}
	}
	return p, nil
}

// dial opens one gRPC connection for shardID and stores it.
// The caller must hold mu (write) or be in a single-goroutine context (NewPool).
func (p *Pool) dial(shardID int, addr string) error {
	// Do not use grpc.WithBlock() — connections are established lazily on the
	// first RPC call. Each call site already carries its own context deadline,
	// so a blocking dial here only delays startup and masks real errors.
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("shard %d (%s): dial: %w", shardID, addr, err)
	}
	p.nodes[shardID] = &entry{
		conn:   conn,
		client: pb.NewRocksDBServiceClient(conn),
	}
	log.Printf("[shard] connected to shard %d at %s", shardID, addr)
	return nil
}

// AddShard dials addr and adds it to the pool under shardID.
// Returns an error if shardID is already present or the dial fails.
func (p *Pool) AddShard(shardID int, addr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.nodes[shardID]; exists {
		return fmt.Errorf("shard %d already in pool", shardID)
	}
	return p.dial(shardID, addr)
}

// RemoveShard closes the connection for shardID and removes it from the pool.
// Returns an error if shardID is not present.
func (p *Pool) RemoveShard(shardID int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	e, exists := p.nodes[shardID]
	if !exists {
		return fmt.Errorf("shard %d not in pool", shardID)
	}
	if err := e.conn.Close(); err != nil {
		log.Printf("[shard] warning: closing shard %d connection: %v", shardID, err)
	}
	delete(p.nodes, shardID)
	log.Printf("[shard] removed shard %d", shardID)
	return nil
}

// Close releases all connections in the pool.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for id, e := range p.nodes {
		if err := e.conn.Close(); err != nil {
			log.Printf("[shard] warning: closing shard %d: %v", id, err)
		}
	}
	p.nodes = make(map[int]*entry)
}

// Len returns the number of shards currently in the pool.
func (p *Pool) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.nodes)
}

// client returns the gRPC client for shardID under a read-lock.
func (p *Pool) client(shardID int) (pb.RocksDBServiceClient, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	e, ok := p.nodes[shardID]
	if !ok {
		return nil, fmt.Errorf("shard %d not in pool", shardID)
	}
	return e.client, nil
}

// ─── Operations ───────────────────────────────────────────────────────────────

// Ping checks liveness of a single shard.
func (p *Pool) Ping(ctx context.Context, shardID int) error {
	c, err := p.client(shardID)
	if err != nil {
		return err
	}
	if _, err := c.Ping(ctx, &pb.PingRequest{}); err != nil {
		return fmt.Errorf("shard %d ping: %w", shardID, err)
	}
	return nil
}

// PingAll pings every shard concurrently and returns a combined error if any fail.
func (p *Pool) PingAll(ctx context.Context) error {
	// Snapshot the IDs under the read-lock so we don't hold it during RPCs.
	p.mu.RLock()
	ids := make([]int, 0, len(p.nodes))
	for id := range p.nodes {
		ids = append(ids, id)
	}
	p.mu.RUnlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(ids))

	for _, id := range ids {
		wg.Add(1)
		go func(shardID int) {
			defer wg.Done()
			if err := p.Ping(ctx, shardID); err != nil {
				errCh <- err
			} else {
				log.Printf("[shard] ✓ shard %d healthy", shardID)
			}
		}(id)
	}
	wg.Wait()
	close(errCh)

	var errs []string
	for e := range errCh {
		errs = append(errs, e.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("unhealthy shards: %v", errs)
	}
	log.Printf("[shard] all %d shards healthy", len(ids))
	return nil
}

// Put writes a single key to the given shard.
func (p *Pool) Put(ctx context.Context, shardID int, key, value string) error {
	c, err := p.client(shardID)
	if err != nil {
		return err
	}
	_, err = c.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	return err
}

// BatchPut writes a batch of key-value pairs to the given shard atomically.
func (p *Pool) BatchPut(ctx context.Context, shardID int, pairs []KV) error {
	if len(pairs) == 0 {
		return nil
	}
	c, err := p.client(shardID)
	if err != nil {
		return err
	}

	items := make([]*pb.PutRequest, len(pairs))
	for i, kv := range pairs {
		items[i] = &pb.PutRequest{Key: kv.Key, Value: kv.Value}
	}
	_, err = c.BatchPut(ctx, &pb.BatchPutRequest{Items: items})
	return err
}

// Get retrieves a single key from the given shard.
// Returns ("", ErrNotFound) when the key does not exist.
func (p *Pool) Get(ctx context.Context, shardID int, key string) (string, error) {
	c, err := p.client(shardID)
	if err != nil {
		return "", err
	}
	resp, err := c.Get(ctx, &pb.GetRequest{Key: key})
	if err != nil {
		return "", err
	}
	return resp.Value, nil
}

// ─── KV ───────────────────────────────────────────────────────────────────────

// KV is a simple key-value pair.
type KV struct {
	Key   string
	Value string
}