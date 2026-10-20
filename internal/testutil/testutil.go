// Package testutil creates throwaway PostgreSQL databases for integration
// tests. Set TEST_DATABASE_URL (e.g. the output of `scripts/devdb.sh url`);
// tests that need a database are skipped without it.
package testutil

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"paymentledger/internal/db"
)

// NewDB creates a fresh, migrated database and drops it when the test ends.
func NewDB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	admin := os.Getenv("TEST_DATABASE_URL")
	if admin == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	pool, _, err := db.CreateDatabase(context.Background(), admin, "test", 32)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		name := pool.Config().ConnConfig.Database
		pool.Close()
		if err := db.DropDatabase(context.Background(), admin, name); err != nil {
			t.Logf("drop %s: %v", name, err)
		}
	})
	return pool
}
