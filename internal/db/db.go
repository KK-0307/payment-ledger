// Package db holds the PostgreSQL connection pool, embedded migrations and
// transaction helpers.
package db

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"paymentledger/internal/metrics"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Querier is satisfied by *pgxpool.Pool, *pgxpool.Conn and pgx.Tx.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Connect opens a pool and verifies connectivity.
func Connect(ctx context.Context, url string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	cfg.MaxConnIdleTime = time.Minute
	cfg.HealthCheckPeriod = 5 * time.Second
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// migrationLockID is an arbitrary constant for pg_advisory_lock so that
// several instances starting at once apply migrations exactly once.
const migrationLockID = 727_001

// Migrate applies embedded migrations that have not been applied yet.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationLockID) //nolint:errcheck

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}

	names, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		version := strings.TrimSuffix(strings.TrimPrefix(name, "migrations/"), ".sql")
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		body, err := migrationFS.ReadFile(name)
		if err != nil {
			return err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			tx.Rollback(ctx) //nolint:errcheck
			return fmt.Errorf("migration %s: %w", version, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			tx.Rollback(ctx) //nolint:errcheck
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", version, err)
		}
	}
	return nil
}

// WithTx runs fn in a READ COMMITTED transaction. Serialization failures and
// deadlocks are retried a few times; any other error rolls back and is
// returned unchanged. fn must be safe to re-run.
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	const maxAttempts = 4
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = runTx(ctx, pool, fn)
		if err == nil || !IsRetryable(err) || ctx.Err() != nil {
			return err
		}
		txRetries.Add(1)
		metrics.TxRetries.WithLabelValues(retryCode(err)).Inc()
		time.Sleep(time.Duration(attempt*attempt) * 5 * time.Millisecond)
	}
	return err
}

func runTx(ctx context.Context, pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback(context.Background()) //nolint:errcheck
		return err
	}
	return tx.Commit(ctx)
}

var txRetries atomic.Int64

// TxRetries is the number of transactions this process rolled back and ran
// again after a serialization failure or deadlock (also exported as the
// ledger_tx_retries_total metric). Tests use it to assert that a code path
// does not deadlock.
func TxRetries() int64 { return txRetries.Load() }

func retryCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
		return "deadlock"
	}
	return "serialization_failure"
}

// IsRetryable reports whether err is a transient transaction conflict.
func IsRetryable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	return false
}

// IsUniqueViolation reports whether err is a unique constraint violation,
// optionally on the named constraint.
func IsUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return constraint == "" || pgErr.ConstraintName == constraint
	}
	return false
}

// CreateDatabase creates and migrates a database named <prefix>_<random> on
// the server in adminURL, returning a pool and the new database's URL.
func CreateDatabase(ctx context.Context, adminURL, prefix string, maxConns int32) (*pgxpool.Pool, string, error) {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return nil, "", err
	}
	name := prefix + "_" + hex.EncodeToString(b)
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return nil, "", err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		return nil, "", err
	}
	u, err := url.Parse(adminURL)
	if err != nil {
		return nil, "", err
	}
	u.Path = "/" + name
	pool, err := Connect(ctx, u.String(), maxConns)
	if err != nil {
		return nil, "", err
	}
	if err := Migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, "", err
	}
	return pool, u.String(), nil
}

// DropDatabase drops a database, terminating its connections.
func DropDatabase(ctx context.Context, adminURL, name string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	return err
}
