// Package faults holds fault-injection settings and the deterministic random
// functions that decide when a fault fires.
//
// Decisions are pure functions of (seed, request key, per-key attempt
// number), not of wall-clock time or goroutine scheduling. With the same
// seed and dataset, the same logical requests hit the same faults on every
// run, and an ablation run (e.g. idempotency disabled) sees the same faults
// as the run it is compared against.
package faults

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"time"
)

// Uniform returns a deterministic value in [0, 1).
func Uniform(seed uint64, parts ...string) float64 {
	h := fnv.New64a()
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], seed)
	h.Write(b[:])
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return float64(splitmix64(h.Sum64())>>11) / (1 << 53)
}

// Normal returns a deterministic standard normal variate (Box-Muller).
func Normal(seed uint64, parts ...string) float64 {
	u1 := Uniform(seed, append(parts, "n1")...)
	u2 := Uniform(seed, append(parts, "n2")...)
	if u1 < 1e-12 {
		u1 = 1e-12
	}
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
}

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// HTTP describes faults injected between the API and the processor.
// Probabilities are per request attempt and are evaluated in this order:
// rate limit, error before, reset before (none forward the request, so they
// have no side effect), then latency, then drop after / error after (both
// forward the request — the processor acts — and then lose the response).
type HTTP struct {
	LatencyMedianMs float64 `json:"latency_median_ms"`
	LatencySigma    float64 `json:"latency_sigma"` // log-normal shape; 0 = constant
	RateLimitProb   float64 `json:"rate_limit_prob"`
	ErrorBeforeProb float64 `json:"error_before_prob"`
	ResetBeforeProb float64 `json:"reset_before_prob"`
	DropAfterProb   float64 `json:"drop_after_prob"`
	ErrorAfterProb  float64 `json:"error_after_prob"`
	// HangMs: after a drop, hold the connection this long before resetting
	// it, so the client usually hits its own timeout first.
	HangMs int `json:"hang_ms"`
}

func (f *HTTP) Latency(seed uint64, key string, attempt int) time.Duration {
	if f.LatencyMedianMs <= 0 {
		return 0
	}
	ms := f.LatencyMedianMs * math.Exp(f.LatencySigma*Normal(seed, "latency", key, itoa(attempt)))
	return time.Duration(ms * float64(time.Millisecond))
}

// TCP describes faults injected between the API and PostgreSQL.
type TCP struct {
	LatencyMs float64 `json:"latency_ms"` // added to every server->client chunk
	// ResetRatePerSec: each open connection is reset (TCP RST) with this
	// Poisson rate.
	ResetRatePerSec float64 `json:"reset_rate_per_sec"`
}

func itoa(n int) string {
	var b [20]byte
	i := len(b)
	if n == 0 {
		return "0"
	}
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
