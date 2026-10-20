package audit_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"paymentledger/internal/audit"
	"paymentledger/internal/db"
	"paymentledger/internal/ledger"
	"paymentledger/internal/testutil"
)

func failed(rep *audit.Report) map[string]int64 {
	out := map[string]int64{}
	for _, c := range rep.Checks {
		if !c.Passed {
			out[c.Name] = c.Violations
		}
	}
	return out
}

// ClearingBalances must add up every shard of processor_clearing, and the
// audit must catch each way the shard model can be corrupted.
func TestShardedAudit(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewDB(t)
	l := ledger.New()
	var clearing []int64
	for i := 0; i < 3; i++ {
		id, err := l.EnsureAccountCommitted(ctx, pool, ledger.ShardCode(ledger.ProcessorClearing("EUR"), i), ledger.Asset, "EUR", nil)
		if err != nil {
			t.Fatal(err)
		}
		clearing = append(clearing, id)
	}
	payable, _ := l.EnsureAccountCommitted(ctx, pool, "merchant_payable:m:EUR", ledger.Liability, "EUR", nil)
	for i, amt := range []int64{100, 250, -40} {
		j := ledger.Journal{Kind: "t", Reference: string(rune('a' + i)), Currency: "EUR",
			Entries: []ledger.Entry{{AccountID: clearing[i], Amount: amt}, {AccountID: payable, Amount: -amt}}}
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error { _, _, err := ledger.Post(ctx, tx, j); return err }); err != nil {
			t.Fatal(err)
		}
	}

	bal, err := audit.ClearingBalances(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(bal) != 1 || bal["EUR"] != 310 {
		t.Fatalf("ClearingBalances = %v, want EUR=310 summed over 3 shards", bal)
	}
	rep, err := audit.Run(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if f := failed(rep); len(f) != 0 {
		t.Fatalf("clean sharded ledger failed audit: %v", f)
	}

	// A shard whose cached balance drifts from its postings is caught per
	// row and for the logical account.
	if _, err := pool.Exec(ctx, `UPDATE accounts SET balance = balance + 1 WHERE id = $1`, clearing[1]); err != nil {
		t.Fatal(err)
	}
	rep, _ = audit.Run(ctx, pool)
	f := failed(rep)
	if f["account_balances_match_postings"] != 1 || f["logical_balances_match_postings"] != 1 || f["ledger_sums_to_zero_per_currency"] == 0 {
		t.Fatalf("drift not caught: %v", f)
	}
	if _, err := pool.Exec(ctx, `UPDATE accounts SET balance = balance - 1 WHERE id = $1`, clearing[1]); err != nil {
		t.Fatal(err)
	}

	// Shards that disagree on currency are not one account.
	if _, err := pool.Exec(ctx, `INSERT INTO accounts (code, type, currency) VALUES ('processor_clearing:EUR#9', 'asset', 'USD')`); err != nil {
		t.Fatal(err)
	}
	rep, _ = audit.Run(ctx, pool)
	if f := failed(rep); f["account_shards_consistent"] != 1 {
		t.Fatalf("mixed-currency shards not caught: %v", f)
	}
	// ... and a malformed suffix is rejected.
	if _, err := pool.Exec(ctx, `DELETE FROM accounts WHERE code = 'processor_clearing:EUR#9'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO accounts (code, type, currency) VALUES ('processor_clearing:EUR#x', 'asset', 'EUR')`); err != nil {
		t.Fatal(err)
	}
	rep, _ = audit.Run(ctx, pool)
	if f := failed(rep); f["account_shards_consistent"] != 1 {
		t.Fatalf("malformed shard suffix not caught: %v", f)
	}
}

// Shards of one merchant's payable account must all belong to that merchant.
func TestMerchantPayableShardsAudit(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewDB(t)
	for _, id := range []string{"m1", "m2"} {
		if _, err := pool.Exec(ctx, `INSERT INTO merchants (id, name) VALUES ($1, $1)`, id); err != nil {
			t.Fatal(err)
		}
	}
	l := ledger.New()
	m1, m2 := "m1", "m2"
	for i := 0; i < 3; i++ {
		if _, err := l.EnsureAccountCommitted(ctx, pool, ledger.ShardCode(ledger.MerchantPayable(m1, "EUR"), i), ledger.Liability, "EUR", &m1); err != nil {
			t.Fatal(err)
		}
	}
	rep, err := audit.Run(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if f := failed(rep); len(f) != 0 {
		t.Fatalf("sharded merchant payable failed audit: %v", f)
	}
	// A shard attributed to another merchant is caught.
	if _, err := pool.Exec(ctx, `UPDATE accounts SET merchant_id = $1 WHERE code = $2`, m2, ledger.ShardCode(ledger.MerchantPayable(m1, "EUR"), 2)); err != nil {
		t.Fatal(err)
	}
	rep, _ = audit.Run(ctx, pool)
	if f := failed(rep); f["account_shards_consistent"] != 1 {
		t.Fatalf("shard owned by another merchant not caught: %v", f)
	}
}
