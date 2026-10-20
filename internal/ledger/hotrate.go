package ledger

import (
	"math"
	"sync"
	"time"
)

// RateTracker estimates the event rate per key (a merchant's currency
// account) with a sliding window of two one-second buckets. It decides how
// many shards a merchant's payable account gets: a cold merchant stays a
// single row and a hot one spreads over several.
//
// The estimate is per process. With several API instances each sees only
// its own share of a merchant's traffic, so each shards a little later than
// a global view would. That is safe: the choice of shard never affects
// correctness, only which row a posting locks (see ShardFor).
type RateTracker struct {
	now func() time.Time

	mu   sync.Mutex
	keys map[string]*window
}

type window struct {
	start     time.Time // start of the current bucket
	cur, prev float64
}

const (
	rateBucket = time.Second
	// The map holds one entry per active key; once it is this large, entries
	// idle for more than two buckets are dropped.
	rateSweepAt = 4096
)

func NewRateTracker() *RateTracker { return &RateTracker{now: time.Now, keys: map[string]*window{}} }

// SetClock replaces the time source (tests).
func (r *RateTracker) SetClock(now func() time.Time) { r.now = now }

// Hit records one event for key and returns the estimated events per second
// over the last full bucket plus the elapsed part of the current one.
func (r *RateTracker) Hit(key string) float64 {
	t := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	w := r.keys[key]
	if w == nil {
		if len(r.keys) >= rateSweepAt {
			r.sweep(t)
		}
		w = &window{start: t}
		r.keys[key] = w
	}
	r.roll(w, t)
	w.cur++
	// Weight the previous bucket by how much of it still lies inside the
	// one-second look-back window.
	frac := float64(t.Sub(w.start)) / float64(rateBucket)
	return w.prev*(1-frac) + w.cur
}

func (r *RateTracker) roll(w *window, t time.Time) {
	elapsed := t.Sub(w.start)
	switch {
	case elapsed < 0: // clock stepped back
		w.start = t
	case elapsed >= 2*rateBucket:
		w.prev, w.cur, w.start = 0, 0, t
	case elapsed >= rateBucket:
		w.prev, w.cur, w.start = w.cur, 0, w.start.Add(rateBucket)
	}
}

func (r *RateTracker) sweep(t time.Time) {
	for k, w := range r.keys {
		if t.Sub(w.start) >= 2*rateBucket {
			delete(r.keys, k)
		}
	}
}

// ShardsForRate returns how many shards an account needs at the given event
// rate: one per perShardRate events/s, between 1 and max. perShardRate <= 0
// or max <= 1 disables sharding (always 1).
func ShardsForRate(rate, perShardRate float64, max int) int {
	if perShardRate <= 0 || max <= 1 {
		return 1
	}
	n := int(math.Ceil(rate / perShardRate))
	switch {
	case n < 1:
		return 1
	case n > max:
		return max
	}
	return n
}

// Len is the number of keys currently tracked.
func (r *RateTracker) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keys)
}
