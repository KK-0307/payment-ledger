package ledger_test

import (
	"fmt"
	"testing"
	"time"

	"paymentledger/internal/ledger"
)

func TestShardsForRate(t *testing.T) {
	for _, c := range []struct {
		rate, per float64
		max, want int
	}{
		{0, 25, 16, 1},
		{1, 25, 16, 1},
		{25, 25, 16, 1},   // exactly one shard's worth
		{25.5, 25, 16, 2}, // just over
		{100, 25, 16, 4},
		{756, 25, 16, 16}, // capped
		{756, 25, 1, 1},   // max 1 = off
		{756, 0, 16, 1},   // rate 0 = off
		{756, -1, 16, 1},
	} {
		if got := ledger.ShardsForRate(c.rate, c.per, c.max); got != c.want {
			t.Errorf("ShardsForRate(%v, %v, %d) = %d, want %d", c.rate, c.per, c.max, got, c.want)
		}
	}
}

func TestRateTracker(t *testing.T) {
	now := time.Unix(1_000, 0)
	r := ledger.NewRateTracker()
	r.SetClock(func() time.Time { return now })

	// 100 events spread over the first second: the estimate climbs to ~100.
	var last float64
	for i := 0; i < 100; i++ {
		now = now.Add(10 * time.Millisecond)
		last = r.Hit("m|EUR")
	}
	if last < 95 || last > 105 {
		t.Fatalf("rate after 100 events in 1s = %.1f, want ~100", last)
	}

	// Keys are independent.
	if got := r.Hit("other|EUR"); got != 1 {
		t.Fatalf("first hit of another key = %v, want 1", got)
	}

	// Steady 100/s for several seconds stays near 100 across bucket rolls.
	for i := 0; i < 500; i++ {
		now = now.Add(10 * time.Millisecond)
		last = r.Hit("m|EUR")
		if i > 100 && (last < 90 || last > 110) {
			t.Fatalf("steady-state estimate %.1f at step %d, want ~100", last, i)
		}
	}

	// After a quiet spell the history is forgotten.
	now = now.Add(10 * time.Second)
	if got := r.Hit("m|EUR"); got != 1 {
		t.Fatalf("rate after 10s idle = %v, want 1", got)
	}

	// A clock that steps backwards must not panic or go negative.
	now = now.Add(-time.Hour)
	if got := r.Hit("m|EUR"); got < 1 {
		t.Fatalf("rate after clock step back = %v", got)
	}
}

func TestRateTrackerSweepsIdleKeys(t *testing.T) {
	now := time.Unix(1_000, 0)
	r := ledger.NewRateTracker()
	r.SetClock(func() time.Time { return now })
	for i := 0; i < 5000; i++ {
		r.Hit(fmt.Sprintf("m%d|EUR", i))
	}
	now = now.Add(5 * time.Second)
	// The sweep runs when a new key arrives at the size limit; the table
	// must not grow without bound across a long run.
	for i := 0; i < 5000; i++ {
		r.Hit(fmt.Sprintf("n%d|EUR", i))
	}
	if n := r.Len(); n > 5100 {
		t.Fatalf("tracker holds %d keys, idle ones were not dropped", n)
	}
}
