package ledger_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"paymentledger/internal/audit"
	"paymentledger/internal/db"
	"paymentledger/internal/ledger"
	"paymentledger/internal/testutil"
)

func TestShardCodeAndShardFor(t *testing.T) {
	if got := ledger.ShardCode("processor_clearing:EUR", 0); got != "processor_clearing:EUR" {
		t.Fatalf("shard 0 must keep the plain code (the pre-sharding account), got %q", got)
	}
	if got := ledger.ShardCode("processor_clearing:EUR", 7); got != "processor_clearing:EUR#7" {
		t.Fatalf("got %q", got)
	}
	const shards, n = 16, 16000
	counts := make([]int, shards)
	for i := 0; i < n; i++ {
		ref := fmt.Sprintf("pay_%026d", i*7919)
		s := ledger.ShardFor(ref, shards)
		if s < 0 || s >= shards {
			t.Fatalf("shard %d out of range", s)
		}
		if s != ledger.ShardFor(ref, shards) {
			t.Fatal("ShardFor is not deterministic")
		}
		counts[s]++
	}
	// Expected 1000 per shard; a sane hash stays well within 20%.
	for s, c := range counts {
		if c < 800 || c > 1200 {
			t.Errorf("shard %d got %d of %d references", s, c, n)
		}
	}
	if ledger.ShardFor("anything", 1) != 0 || ledger.ShardFor("anything", 0) != 0 {
		t.Fatal("one shard must always be shard 0")
	}
}

// shardedFixture creates shards of a "clearing" asset and one counter-party
// account, and returns their ids.
func shardedFixture(t *testing.T, pool db.Querier, nShards int) (shards []int64, other int64) {
	t.Helper()
	ctx := context.Background()
	l := ledger.New()
	for i := 0; i < nShards; i++ {
		id, err := l.EnsureAccountCommitted(ctx, pool, ledger.ShardCode("clearing:EUR", i), ledger.Asset, "EUR", nil)
		if err != nil {
			t.Fatal(err)
		}
		shards = append(shards, id)
	}
	other, err := l.EnsureAccountCommitted(ctx, pool, "payable:EUR", ledger.Liability, "EUR", nil)
	if err != nil {
		t.Fatal(err)
	}
	return shards, other
}

func post(t *testing.T, tx pgx.Tx, j ledger.Journal) {
	t.Helper()
	if _, _, err := ledger.Post(context.Background(), tx, j); err != nil {
		t.Fatal(err)
	}
}

// The logical balance of a sharded account is the sum of its shards; reads
// by code see the whole account, reads by id see one shard.
func TestShardedAccountBalanceAndPostings(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewDB(t)
	shards, payable := shardedFixture(t, pool, 4)

	var total int64
	for i := 0; i < 20; i++ {
		amt := int64(100 + i)
		total += amt
		j := ledger.Journal{Kind: "t", Reference: fmt.Sprintf("r%d", i), Currency: "EUR",
			Entries: []ledger.Entry{{AccountID: shards[i%4], Amount: amt}, {AccountID: payable, Amount: -amt}}}
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error { post(t, tx, j); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	// A refund-like entry hitting a different shard than its capture drives
	// that shard negative; only the sum is meaningful.
	j := ledger.Journal{Kind: "t", Reference: "refund", Currency: "EUR",
		Entries: []ledger.Entry{{AccountID: payable, Amount: 50}, {AccountID: shards[3], Amount: -50}}}
	if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error { post(t, tx, j); return nil }); err != nil {
		t.Fatal(err)
	}
	total -= 50

	a, err := ledger.GetAccount(ctx, pool, "clearing:EUR")
	if err != nil {
		t.Fatal(err)
	}
	if a.Balance != total || a.Code != "clearing:EUR" || len(a.ShardIDs) != 4 || a.ID != shards[0] {
		t.Fatalf("logical account = %+v, want balance %d over 4 shards", a, total)
	}
	one, err := ledger.GetAccount(ctx, pool, fmt.Sprint(shards[1]))
	if err != nil {
		t.Fatal(err)
	}
	if one.Code != "clearing:EUR#1" || len(one.ShardIDs) != 0 {
		t.Fatalf("shard by id = %+v", one)
	}
	var sum int64
	for _, id := range shards {
		s, _ := ledger.GetAccount(ctx, pool, fmt.Sprint(id))
		sum += s.Balance
	}
	if sum != total {
		t.Fatalf("shard balances sum to %d, want %d", sum, total)
	}

	ps, err := ledger.AccountPostings(ctx, pool, a, 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(ps) != 21 {
		t.Fatalf("postings across shards = %d, want 21", len(ps))
	}
	for i := 1; i < len(ps); i++ {
		if ps[i].ID >= ps[i-1].ID {
			t.Fatal("postings not newest first")
		}
	}
	if ps2, _ := ledger.AccountPostings(ctx, pool, one, 0, 500); len(ps2) != 5 {
		t.Fatalf("one shard's postings = %d, want 5", len(ps2))
	}

	pl, err := ledger.GetAccount(ctx, pool, "payable:EUR")
	if err != nil || len(pl.ShardIDs) != 0 || pl.ID != payable {
		t.Fatalf("unsharded account = %+v, %v", pl, err)
	}
	if _, err := ledger.GetAccount(ctx, pool, "nope:EUR"); err == nil {
		t.Fatal("missing account found")
	}

	rep, err := audit.Run(ctx, pool)
	if err != nil || !rep.Passed {
		t.Fatalf("audit: %v %+v", err, rep.Checks)
	}
}

// The point of sharding: a transaction holding one shard must not block a
// posting to another shard, while a posting to the same shard still waits.
func TestShardsDoNotBlockEachOther(t *testing.T) {
	ctx := context.Background()
	pool := testutil.NewDB(t)
	shards, _ := shardedFixture(t, pool, 2)
	l := ledger.New()
	pay1, _ := l.EnsureAccountCommitted(ctx, pool, "p1:EUR", ledger.Liability, "EUR", nil)
	pay2, _ := l.EnsureAccountCommitted(ctx, pool, "p2:EUR", ledger.Liability, "EUR", nil)

	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(ctx) //nolint:errcheck
	post(t, holder, ledger.Journal{Kind: "t", Reference: "hold", Currency: "EUR",
		Entries: []ledger.Entry{{AccountID: shards[0], Amount: 10}, {AccountID: pay1, Amount: -10}}})

	run := func(ref string, shard, payable int64) <-chan error {
		ch := make(chan error, 1)
		go func() {
			ch <- db.WithTx(ctx, pool, func(tx pgx.Tx) error {
				_, _, err := ledger.Post(ctx, tx, ledger.Journal{Kind: "t", Reference: ref, Currency: "EUR",
					Entries: []ledger.Entry{{AccountID: shard, Amount: 5}, {AccountID: payable, Amount: -5}}})
				return err
			})
		}()
		return ch
	}

	other := run("other-shard", shards[1], pay2)
	select {
	case err := <-other:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("posting to a different shard blocked behind an open transaction")
	}

	same := run("same-shard", shards[0], pay2)
	select {
	case err := <-same:
		t.Fatalf("posting to the locked shard did not wait (err=%v)", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-same:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("posting to the shard did not proceed after the holder committed")
	}
}
