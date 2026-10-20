package bench

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DBStats describes what PostgreSQL was doing during the load phase.
//
// WaitEvents comes from sampling pg_stat_activity every SampleIntervalMs:
// each sample counts every active backend of the benchmark database once,
// under its wait event ("Lock/tuple", "Lock/transactionid", "IO/WALWrite",
// "CPU/none" for a backend that is running). The counts are samples, not
// time. RowLockWaitShare is the share of those backend samples spent waiting
// for another transaction's row lock. The counters are deltas of
// pg_stat_database over the load and drain phases.
type DBStats struct {
	SampleIntervalMs int            `json:"sample_interval_ms"`
	Samples          int            `json:"samples"`
	ActiveSamples    int            `json:"active_backend_samples"`
	WaitEvents       map[string]int `json:"wait_events"`
	RowLockWaitShare float64        `json:"row_lock_wait_share"`
	Deadlocks        int64          `json:"deadlocks"`
	TxCommitted      int64          `json:"tx_committed"`
	TxRolledBack     int64          `json:"tx_rolled_back"`
}

const dbSampleInterval = 25 * time.Millisecond

type dbCounters struct{ deadlocks, commit, rollback int64 }

func readDBCounters(ctx context.Context, pool *pgxpool.Pool) (dbCounters, error) {
	var c dbCounters
	err := pool.QueryRow(ctx, `
		SELECT deadlocks, xact_commit, xact_rollback FROM pg_stat_database WHERE datname = current_database()`).
		Scan(&c.deadlocks, &c.commit, &c.rollback)
	return c, err
}

// dbSampler samples backend wait events until stop is called.
type dbSampler struct {
	pool   *pgxpool.Pool
	before dbCounters

	mu      sync.Mutex
	events  map[string]int
	samples int
	active  int

	cancel context.CancelFunc
	done   chan struct{}
}

func startDBSampler(ctx context.Context, pool *pgxpool.Pool) *dbSampler {
	s := &dbSampler{pool: pool, events: map[string]int{}, done: make(chan struct{})}
	s.before, _ = readDBCounters(ctx, pool)
	ctx, s.cancel = context.WithCancel(ctx)
	go func() {
		defer close(s.done)
		t := time.NewTicker(dbSampleInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.sample(ctx)
			}
		}
	}()
	return s
}

func (s *dbSampler) sample(ctx context.Context) {
	rows, err := s.pool.Query(ctx, `
		SELECT coalesce(wait_event_type, 'CPU') || '/' || coalesce(wait_event, 'none')
		FROM pg_stat_activity
		WHERE datname = current_database() AND state = 'active' AND pid <> pg_backend_pid()`)
	if err != nil {
		return
	}
	defer rows.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples++
	for rows.Next() {
		var ev string
		if rows.Scan(&ev) == nil {
			s.events[ev]++
			s.active++
		}
	}
}

// stop ends sampling.
func (s *dbSampler) stop() {
	s.cancel()
	<-s.done
}

// result combines the samples with the counter deltas. Backends report
// their statistics to the cumulative-statistics system with a delay of up to
// about a second, so the caller waits briefly before this read.
func (s *dbSampler) result(ctx context.Context) *DBStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &DBStats{SampleIntervalMs: int(dbSampleInterval / time.Millisecond), Samples: s.samples,
		ActiveSamples: s.active, WaitEvents: map[string]int{}}
	// Keep the result files small: events seen in at least 0.1 % of samples.
	keys := make([]string, 0, len(s.events))
	for k := range s.events {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if s.active > 0 && float64(s.events[k])/float64(s.active) >= 0.001 {
			out.WaitEvents[k] = s.events[k]
		}
	}
	if s.active > 0 {
		out.RowLockWaitShare = round3(float64(s.events["Lock/tuple"]+s.events["Lock/transactionid"]) / float64(s.active))
	}
	if after, err := readDBCounters(ctx, s.pool); err == nil {
		out.Deadlocks = after.deadlocks - s.before.deadlocks
		out.TxCommitted = after.commit - s.before.commit
		out.TxRolledBack = after.rollback - s.before.rollback
	}
	return out
}

// apiAppName is the application_name of the API processes' connections.
const apiAppName = "bench-api"

// waitForBackendsToExit waits until no API backend is connected to the
// database, so their statistics have been flushed.
func waitForBackendsToExit(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var n int
		err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND application_name = $1`, apiAppName).Scan(&n)
		if err == nil && n == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}
