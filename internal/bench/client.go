package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"time"
)

// client is a well-behaved API client: it retries transport errors, 409,
// 429 and 5xx with the SAME Idempotency-Key, using exponential backoff with
// full jitter, and rotates across API instances between attempts.
type client struct {
	http        *http.Client
	instances   []*apiProc
	apiKey      string
	maxAttempts int
	base, cap   time.Duration
}

type attemptResult struct {
	status    int // 0 = transport error
	body      []byte
	latencyMs float64
	err       error
}

type callResult struct {
	final     attemptResult
	attempts  []attemptResult
	gaveUp    bool
	latencyMs float64 // first attempt start -> final response
}

func retryable(status int) bool {
	return status == 0 || status == 409 || status == 429 || status >= 500
}

func (c *client) call(ctx context.Context, rng *rand.Rand, method, path, key string, body any, startIdx int) callResult {
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	var res callResult
	start := time.Now()
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		if attempt > 0 {
			ceil := min(c.cap, c.base<<min(attempt-1, 20))
			select {
			case <-ctx.Done():
				res.gaveUp = true
				res.latencyMs = msSince(start)
				return res
			case <-time.After(time.Duration(rng.Int64N(int64(ceil) + 1))):
			}
		}
		inst := c.instances[(startIdx+attempt)%len(c.instances)]
		ar := c.once(ctx, method, inst.URL()+path, key, raw)
		res.attempts = append(res.attempts, ar)
		res.final = ar
		if !retryable(ar.status) {
			res.latencyMs = msSince(start)
			return res
		}
	}
	res.gaveUp = true
	res.latencyMs = msSince(start)
	return res
}

func (c *client) once(ctx context.Context, method, url, key string, raw []byte) attemptResult {
	t0 := time.Now()
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(raw))
	if err != nil {
		return attemptResult{err: err}
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return attemptResult{err: err, latencyMs: msSince(t0)}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return attemptResult{err: err, latencyMs: msSince(t0)}
	}
	return attemptResult{status: resp.StatusCode, body: b, latencyMs: msSince(t0)}
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }
