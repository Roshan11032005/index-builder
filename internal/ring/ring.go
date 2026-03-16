// Package ring implements a consistent hash ring for shard routing.
//
// Each shard is represented by [VNodes] virtual nodes spread uniformly around
// a 2^64 ring. A key is routed to the first virtual node whose hash is ≥ the
// key's hash (clockwise walk). This means adding or removing a shard only
// re-routes the keys that were previously owned by that shard's neighbours —
// all other keys are unaffected.
package ring

import (
	"fmt"
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2"
)

// DefaultVNodes is the number of virtual nodes per shard.
// Higher values give better key distribution but increase memory use.
const DefaultVNodes = 150

// point is a single position on the ring.
type point struct {
	hash    uint64
	shardID int
}

// Ring is a consistent hash ring. All methods are safe for concurrent use.
type Ring struct {
	mu     sync.RWMutex
	vnodes int
	points []point // sorted ascending by hash
}

// New builds a ring containing the given shard IDs.
// vnodes controls how many virtual nodes each shard gets;
// pass 0 to use DefaultVNodes.
func New(shardIDs []int, vnodes int) *Ring {
	if vnodes <= 0 {
		vnodes = DefaultVNodes
	}
	r := &Ring{vnodes: vnodes}
	for _, id := range shardIDs {
		r.insertVNodes(id)
	}
	r.sort()
	return r
}

// Get returns the shard ID responsible for key.
// It panics if the ring is empty.
func (r *Ring) Get(key string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.points) == 0 {
		panic("ring: Get called on an empty ring")
	}

	h := xxhash.Sum64String(key)
	idx := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= h
	})
	// Wrap around: if we walked off the end, land on the first node.
	if idx == len(r.points) {
		idx = 0
	}
	return r.points[idx].shardID
}

// AddShard inserts a new shard into the ring.
// Only the subset of keys whose hash falls between the new shard's virtual
// nodes and their clockwise predecessors will be re-routed.
func (r *Ring) AddShard(shardID int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Reject duplicates to keep the ring consistent.
	for _, p := range r.points {
		if p.shardID == shardID {
			return fmt.Errorf("ring: shard %d already exists", shardID)
		}
	}
	r.insertVNodes(shardID)
	r.sort()
	return nil
}

// RemoveShard removes a shard and all of its virtual nodes from the ring.
// Keys previously owned by this shard will be redistributed to the next
// shard clockwise.
func (r *Ring) RemoveShard(shardID int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	before := len(r.points)
	kept := r.points[:0]
	for _, p := range r.points {
		if p.shardID != shardID {
			kept = append(kept, p)
		}
	}
	if len(kept) == before {
		return fmt.Errorf("ring: shard %d not found", shardID)
	}
	r.points = kept
	return nil
}

// Shards returns the deduplicated list of shard IDs currently in the ring,
// in ascending order.
func (r *Ring) Shards() []int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[int]struct{}, len(r.points)/r.vnodes+1)
	for _, p := range r.points {
		seen[p.shardID] = struct{}{}
	}
	ids := make([]int, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids
}

// Len returns the number of shards (not virtual nodes) in the ring.
func (r *Ring) Len() int {
	return len(r.Shards())
}

// ── internal helpers ──────────────────────────────────────────────────────────

// insertVNodes adds vnodes virtual-node entries for shardID.
// The caller must hold mu (write) and call sort() afterwards.
func (r *Ring) insertVNodes(shardID int) {
	for i := 0; i < r.vnodes; i++ {
		// Key format: "<shardID>#<vnode>" — simple, collision-resistant.
		vkey := fmt.Sprintf("%d#%d", shardID, i)
		r.points = append(r.points, point{
			hash:    xxhash.Sum64String(vkey),
			shardID: shardID,
		})
	}
}

func (r *Ring) sort() {
	sort.Slice(r.points, func(i, j int) bool {
		return r.points[i].hash < r.points[j].hash
	})
}