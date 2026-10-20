package bench

import (
	"bytes"
	"context"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Quantiles summarises a latency sample in milliseconds. Exact (sorted
// sample), not approximated.
type Quantiles struct {
	N    int     `json:"n"`
	Mean float64 `json:"mean"`
	P50  float64 `json:"p50"`
	P90  float64 `json:"p90"`
	P95  float64 `json:"p95"`
	P99  float64 `json:"p99"`
	P999 float64 `json:"p99_9"`
	Max  float64 `json:"max"`
}

func quantiles(ms []float64) Quantiles {
	q := Quantiles{N: len(ms)}
	if len(ms) == 0 {
		return q
	}
	s := append([]float64(nil), ms...)
	sort.Float64s(s)
	var sum float64
	for _, v := range s {
		sum += v
	}
	at := func(p float64) float64 {
		i := int(p * float64(len(s)-1))
		return round3(s[i])
	}
	q.Mean = round3(sum / float64(len(s)))
	q.P50, q.P90, q.P95, q.P99, q.P999, q.Max = at(0.5), at(0.9), at(0.95), at(0.99), at(0.999), round3(s[len(s)-1])
	return q
}

func round3(v float64) float64 { return float64(int64(v*1000+0.5)) / 1000 }

// Environment records where a result was produced.
type Environment struct {
	GoVersion    string            `json:"go_version"`
	OS           string            `json:"os"`
	Arch         string            `json:"arch"`
	NumCPU       int               `json:"num_cpu"`
	CPUModel     string            `json:"cpu_model"`
	MemoryBytes  string            `json:"memory_bytes"`
	Postgres     string            `json:"postgres"`
	PGSettings   map[string]string `json:"pg_settings"`
	GitCommit    string            `json:"git_commit"`
	GitDirty     bool              `json:"git_dirty"`
	APIBinSHA256 string            `json:"api_bin_sha256"`
	CapturedAt   time.Time         `json:"captured_at"`
}

func captureEnv(ctx context.Context, pool *pgxpool.Pool, apiBinSHA string) Environment {
	e := Environment{GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, NumCPU: runtime.NumCPU(),
		APIBinSHA256: apiBinSHA, CapturedAt: time.Now().UTC(), PGSettings: map[string]string{}}
	switch runtime.GOOS {
	case "darwin":
		e.CPUModel = cmdOut("sysctl", "-n", "machdep.cpu.brand_string")
		e.MemoryBytes = cmdOut("sysctl", "-n", "hw.memsize")
	case "linux":
		for _, line := range strings.Split(cmdOut("cat", "/proc/cpuinfo"), "\n") {
			if strings.HasPrefix(line, "model name") {
				e.CPUModel = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
				break
			}
		}
		e.MemoryBytes = cmdOut("sh", "-c", "awk '/MemTotal/ {print $2*1024}' /proc/meminfo")
	}
	pool.QueryRow(ctx, `SELECT version()`).Scan(&e.Postgres) //nolint:errcheck
	for _, name := range []string{"fsync", "synchronous_commit", "wal_sync_method", "shared_buffers", "max_connections", "full_page_writes"} {
		var v string
		if pool.QueryRow(ctx, `SELECT current_setting($1)`, name).Scan(&v) == nil {
			e.PGSettings[name] = v
		}
	}
	e.GitCommit = cmdOut("git", "rev-parse", "HEAD")
	// Result files are written into the tree during a sweep; they must not
	// make later runs of the same sweep look "dirty".
	e.GitDirty = cmdOut("git", "status", "--porcelain", "--", ".", ":!bench/results", ":!bench/archive", ":!bench/sweeps") != ""
	return e
}

func cmdOut(name string, args ...string) string {
	var out bytes.Buffer
	cmd := exec.Command(name, args...)
	cmd.Stdout = &out
	if cmd.Run() != nil {
		return ""
	}
	return strings.TrimSpace(out.String())
}
