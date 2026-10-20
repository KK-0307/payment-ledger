package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"paymentledger/internal/audit"
	"paymentledger/internal/db"
	"paymentledger/internal/ledger"
	"paymentledger/internal/testutil"
)

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		j    ledger.Journal
		ok   bool
	}{
		{"balanced", ledger.Journal{Kind: "k", Reference: "r", Currency: "EUR", Entries: []ledger.Entry{{1, 100}, {2, -100}}}, true},
		{"unbalanced", ledger.Journal{Kind: "k", Reference: "r", Currency: "EUR", Entries: []ledger.Entry{{1, 100}, {2, -99}}}, false},
		{"single leg", ledger.Journal{Kind: "k", Reference: "r", Currency: "EUR", Entries: []ledger.Entry{{1, 0}}}, false},
		{"zero amount", ledger.Journal{Kind: "k", Reference: "r", Currency: "EUR", Entries: []ledger.Entry{{1, 0}, {2, 0}}}, false},
		{"duplicate account", ledger.Journal{Kind: "k", Reference: "r", Currency: "EUR", Entries: []ledger.Entry{{1, 100}, {1, -100}}}, false},
		{"missing currency", ledger.Journal{Kind: "k", Reference: "r", Entries: []ledger.Entry{{1, 100}, {2, -100}}}, false},
	}
	for _, c := range cases {
		if err := c.j.Validate(); (err == nil) != c.ok {
			t.Errorf("%s: Validate() = %v, want ok=%v", c.name, err, c.ok)
		}
	}
}

func TestPostAndIdempotency(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewDB(t)
	l := ledger.New()
	a, err := l.EnsureAccountCommitted(ctx, pool, "cash:EUR", ledger.Asset, "EUR", nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := l.EnsureAccountCommitted(ctx, pool, "payable:EUR", ledger.Liability, "EUR", nil)
	if err != nil {
		t.Fatal(err)
	}
	// A fresh Ledger has no cache, so this goes to the database.
	if _, err := ledger.New().EnsureAccount(ctx, pool, "cash:EUR", ledger.Liability, "EUR", nil); !errors.Is(err, ledger.ErrAccountMismatch) {
		t.Fatalf("want ErrAccountMismatch, got %v", err)
	}

	j := ledger.Journal{Kind: "test", Reference: "ref-1", Currency: "EUR", Entries: []ledger.Entry{{a, 1000}, {b, -1000}}}
	for i := 0; i < 3; i++ {
		var created bool
		err := db.WithTx(ctx, pool, func(tx pgx.Tx) (err error) {
			_, created, err = ledger.Post(ctx, tx, j)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if created != (i == 0) {
			t.Fatalf("post #%d: created=%v", i, created)
		}
	}
	acct, err := ledger.GetAccount(ctx, pool, "cash:EUR")
	if err != nil {
		t.Fatal(err)
	}
	if acct.Balance != 1000 {
		t.Fatalf("balance = %d, want 1000 (journal must post once)", acct.Balance)
	}
	rep, err := audit.Run(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Passed {
		t.Fatalf("audit failed: %+v", rep.Checks)
	}
}

// The database itself must refuse unbalanced entries and mutations, even
// if application code is bypassed.
func TestDatabaseEnforcesInvariants(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewDB(t)
	l := ledger.New()
	a, _ := l.EnsureAccountCommitted(ctx, pool, "a:EUR", ledger.Asset, "EUR", nil)
	b, _ := l.EnsureAccountCommitted(ctx, pool, "b:EUR", ledger.Liability, "EUR", nil)
	usd, _ := l.EnsureAccountCommitted(ctx, pool, "c:USD", ledger.Liability, "USD", nil)

	rawEntry := func(entries ...[2]int64) error {
		return db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			var id int64
			if err := tx.QueryRow(ctx, `INSERT INTO journal_entries (kind, reference, currency) VALUES ('raw', $1, 'EUR') RETURNING id`,
				fmt.Sprint(rand.Int64())).Scan(&id); err != nil {
				return err
			}
			for _, e := range entries {
				if _, err := tx.Exec(ctx, `INSERT INTO postings (journal_entry_id, account_id, amount) VALUES ($1, $2, $3)`, id, e[0], e[1]); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := rawEntry([2]int64{a, 100}, [2]int64{b, -99}); err == nil || !strings.Contains(err.Error(), "unbalanced") {
		t.Fatalf("unbalanced entry committed: %v", err)
	}
	if err := rawEntry(); err == nil || !strings.Contains(err.Error(), "unbalanced") {
		t.Fatalf("empty entry committed: %v", err)
	}
	if err := rawEntry([2]int64{a, 100}, [2]int64{usd, -100}); err == nil || !strings.Contains(err.Error(), "currency mismatch") {
		t.Fatalf("cross-currency entry committed: %v", err)
	}
	if err := rawEntry([2]int64{a, 100}, [2]int64{b, -100}); err != nil {
		t.Fatalf("balanced raw entry rejected: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE postings SET amount = amount + 1`); err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("posting update allowed: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM journal_entries`); err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Fatalf("journal delete allowed: %v", err)
	}
}

// Many concurrent transfers over a small set of accounts must not deadlock
// and must leave balances equal to the sum of postings.
func TestConcurrentPosting(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewDB(t)
	l := ledger.New()
	var ids []int64
	for i := 0; i < 5; i++ {
		id, err := l.EnsureAccountCommitted(ctx, pool, fmt.Sprintf("acct%d:EUR", i), ledger.Asset, "EUR", nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	const workers, per = 16, 50
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 7))
			for i := 0; i < per; i++ {
				perm := r.Perm(len(ids))
				amt := int64(r.IntN(1000) + 1)
				j := ledger.Journal{Kind: "transfer", Reference: fmt.Sprintf("w%d-%d", w, i), Currency: "EUR",
					Entries: []ledger.Entry{{ids[perm[0]], amt}, {ids[perm[1]], -amt / 2}, {ids[perm[2]], -(amt - amt/2)}}}
				if amt/2 == 0 {
					j.Entries = []ledger.Entry{{ids[perm[0]], amt}, {ids[perm[1]], -amt}}
				}
				if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error { _, _, err := ledger.Post(ctx, tx, j); return err }); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	rep, err := audit.Run(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Passed {
		t.Fatalf("audit failed: %+v", rep.Checks)
	}
	if rep.Counts["journal_entries"] != workers*per {
		t.Fatalf("journal entries = %d", rep.Counts["journal_entries"])
	}
}
