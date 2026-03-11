// Package shard provides a gRPC client pool for the RocksDB shard nodes.
// It is the only package that imports the generated proto bindings.
package shard

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "index-builder/proto/rocksdb"
)

// ─── Pool ─────────────────────────────────────────────────────────────────────

// Pool manages one gRPC connection + client per shard.
type Pool struct {
	mu      sync.RWMutex
	conns   []*grpc.ClientConn
	clients []pb.RocksDBServiceClient
}

// NewPool dials all shard addresses and returns a ready Pool.
// addresses is an ordered slice of "host:port" strings, one per shard.
func NewPool(addresses []string) (*Pool, error) {
	p := &Pool{
		conns:   make([]*grpc.ClientConn, len(addresses)),
		clients: make([]pb.RocksDBServiceClient, len(addresses)),
	}

	for i, addr := range addresses {
		conn, err := grpc.Dial(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithTimeout(5*time.Second),
		)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("shard %d (%s): dial: %w", i, addr, err)
		}
		p.conns[i] = conn
		p.clients[i] = pb.NewRocksDBServiceClient(conn)
		log.Printf("[shard] connected to shard %d at %s", i, addr)
	}
	return p, nil
}

// Close releases all connections in the pool.
func (p *Pool) Close() {
	for _, c := range p.conns {
		if c != nil {
			c.Close()
		}
	}
}

// client returns the gRPC client for shardID, or an error if out of range.
func (p *Pool) client(shardID int) (pb.RocksDBServiceClient, error) {
	if shardID < 0 || shardID >= len(p.clients) {
		return nil, fmt.Errorf("shard %d out of range (pool has %d shards)", shardID, len(p.clients))
	}
	return p.clients[shardID], nil
}

// Len returns the number of shards in the pool.
func (p *Pool) Len() int { return len(p.clients) }

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
	var wg sync.WaitGroup
	errCh := make(chan error, len(p.clients))

	for i := range p.clients {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if err := p.Ping(ctx, id); err != nil {
				errCh <- err
			} else {
				log.Printf("[shard] ✓ shard %d healthy", id)
			}
		}(i)
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
	log.Printf("[shard] all %d shards healthy", len(p.clients))
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

// ─── KV is a key-value pair passed to BatchPut ───────────────────────────────

// KV is a simple key-value pair.
type KV struct {
	Key   string
	Value string
}