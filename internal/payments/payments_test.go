package payments_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"paymentledger/internal/audit"
	"paymentledger/internal/db"
	"paymentledger/internal/faultproxy"
	"paymentledger/internal/faults"
	"paymentledger/internal/httpapi"
	"paymentledger/internal/ledger"
	"paymentledger/internal/payments"
	"paymentledger/internal/processor"
	"paymentledger/internal/stripe"
	"paymentledger/internal/stripesim"
	"paymentledger/internal/testutil"
)

const scope = "test-scope"

type env struct {
	t      *testing.T
	pool   *pgxpool.Pool
	sim    *stripesim.Server
	proxy  *faultproxy.HTTPProxy
	svc    *payments.Service
	worker *payments.RecoveryWorker
	mer    string
}

type envOpts struct {
	simCfg stripesim.Config
	faults faults.HTTP
	mutate func(*payments.Config)
}

func newEnv(t *testing.T, o envOpts) *env {
	t.Helper()
	pool := testutil.NewDB(t)
	sim := stripesim.New(o.simCfg)
	simSrv := httptest.NewServer(sim.Handler())
	t.Cleanup(func() { simSrv.Close(); sim.Close() })
	proxy, err := faultproxy.NewHTTP(simSrv.URL, 42, o.faults)
	if err != nil {
		t.Fatal(err)
	}
	proxySrv := httptest.NewServer(proxy)
	t.Cleanup(proxySrv.Close)

	sc, err := stripe.New(proxySrv.URL, "sk_test_sim", &http.Client{Timeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	cfg := payments.DefaultConfig()
	cfg.Lease = 2 * time.Second
	cfg.RetryBase = 10 * time.Millisecond
	cfg.RetryMax = 50 * time.Millisecond
	cfg.InlineRetries = 0
	cfg.LookupAfter = 2
	if o.mutate != nil {
		o.mutate(&cfg)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := payments.New(pool, ledger.New(), &processor.Stripe{Client: sc}, cfg, log)
	e := &env{t: t, pool: pool, sim: sim, proxy: proxy, svc: svc,
		worker: &payments.RecoveryWorker{Svc: svc, Batch: 100, Concurrency: 8, KeyMaxAge: time.Hour}}

	resp, err := svc.CreateMerchant(context.Background(), scope, "", payments.CreateMerchantRequest{Name: "Test merchant"})
	if err != nil {
		t.Fatal(err)
	}
	var m payments.Merchant
	mustUnmarshal(t, resp.Body, &m)
	e.mer = m.ID
	return e
}

func (e *env) pay(key string, amount int64, pm string) (*payments.Response, payments.Payment) {
	e.t.Helper()
	resp, err := e.svc.CreatePayment(context.Background(), scope, key, payments.CreatePaymentRequest{
		MerchantID: e.mer, Amount: amount, Currency: "eur", PaymentMethod: pm, Metadata: map[string]string{"client_ref": key},
	})
	if err != nil {
		e.t.Fatal(err)
	}
	var p payments.Payment
	json.Unmarshal(resp.Body, &p) //nolint:errcheck
	return resp, p
}

func (e *env) audit() *audit.Report {
	e.t.Helper()
	rep, err := audit.Run(context.Background(), e.pool)
	if err != nil {
		e.t.Fatal(err)
	}
	if !rep.Passed {
		for _, c := range rep.Checks {
			if !c.Passed {
				e.t.Errorf("audit check %s failed: %d violations", c.Name, c.Violations)
			}
		}
	}
	return rep
}

// succeededPIs counts succeeded PaymentIntents at the processor per
// client_ref: the ground truth for "how many times was the card charged".
func (e *env) succeededPIs() map[string]int {
	out := map[string]int{}
	for _, pi := range e.sim.Snapshot().PaymentIntents {
		if pi.Status == "succeeded" {
			out[pi.Metadata["client_ref"]]++
		}
	}
	return out
}

func mustUnmarshal(t *testing.T, b []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
}

func TestHappyPathAndLedger(t *testing.T) {
	e := newEnv(t, envOpts{})
	resp, p := e.pay("k1", 10_000, "pm_card_visa")
	if resp.Status != 201 || p.Status != payments.StatusSucceeded {
		t.Fatalf("got %d %s", resp.Status, resp.Body)
	}
	// 2.9% + 30 = 290 + 30
	if p.Fee == nil || *p.Fee != 320 {
		t.Fatalf("fee = %v, want 320", p.Fee)
	}
	ctx := context.Background()
	clearing, _ := ledger.GetAccount(ctx, e.pool, ledger.ProcessorClearing("EUR"))
	payable, _ := ledger.GetAccount(ctx, e.pool, ledger.MerchantPayable(e.mer, "EUR"))
	fees, _ := ledger.GetAccount(ctx, e.pool, ledger.FeeRevenue("EUR"))
	if clearing.Balance != 10_000 || payable.Balance != -9_680 || fees.Balance != -320 {
		t.Fatalf("balances clearing=%d payable=%d fees=%d", clearing.Balance, payable.Balance, fees.Balance)
	}
	rep := e.audit()
	if rep.Counts["outbox_events"] != 1 {
		t.Fatalf("outbox events = %d", rep.Counts["outbox_events"])
	}
}

func TestIdempotentReplayAndMismatch(t *testing.T) {
	e := newEnv(t, envOpts{})
	r1, p1 := e.pay("same-key", 5_000, "pm_card_visa")
	r2, p2 := e.pay("same-key", 5_000, "pm_card_visa")
	if !r2.Replayed || !bytes.Equal(r1.Body, r2.Body) || p1.ID != p2.ID {
		t.Fatalf("replay mismatch:\n%s\n%s", r1.Body, r2.Body)
	}
	r3, _ := e.pay("same-key", 6_000, "pm_card_visa")
	if r3.Status != 422 {
		t.Fatalf("reuse with different params: got %d %s", r3.Status, r3.Body)
	}
	if n := len(e.sim.Snapshot().PaymentIntents); n != 1 {
		t.Fatalf("processor saw %d PaymentIntents, want 1", n)
	}
	e.audit()
}

func TestConcurrentSameKey(t *testing.T) {
	e := newEnv(t, envOpts{faults: faults.HTTP{LatencyMedianMs: 50}})
	var wg sync.WaitGroup
	codes := make(chan int, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, _ := e.pay("racy", 1_000, "pm_card_visa")
			codes <- r.Status
		}()
	}
	wg.Wait()
	close(codes)
	hist := map[int]int{}
	for c := range codes {
		hist[c]++
	}
	if hist[201] < 1 || hist[201]+hist[409] != 20 {
		t.Fatalf("status histogram %v", hist)
	}
	if got := e.succeededPIs()["racy"]; got != 1 {
		t.Fatalf("card charged %d times", got)
	}
	e.audit()
}

func TestDecline(t *testing.T) {
	e := newEnv(t, envOpts{})
	resp, p := e.pay("decl", 2_000, "pm_card_chargeDeclinedInsufficientFunds")
	if resp.Status != 201 || p.Status != payments.StatusFailed || p.FailureCode == nil || *p.FailureCode != "insufficient_funds" {
		t.Fatalf("got %d %s", resp.Status, resp.Body)
	}
	rep := e.audit()
	if rep.Counts["journal_entries"] != 0 {
		t.Fatal("declined payment wrote a journal entry")
	}
}

func TestValidation(t *testing.T) {
	e := newEnv(t, envOpts{})
	for _, tc := range []struct {
		key    string
		amount int64
		pm     string
		want   int
	}{
		{"", 1000, "pm_card_visa", 400},
		{"v1", 10, "pm_card_visa", 400},
		{"v2", 1000, "not-a-pm", 400},
	} {
		r, _ := e.pay(tc.key, tc.amount, tc.pm)
		if r.Status != tc.want {
			t.Errorf("%+v: got %d %s", tc, r.Status, r.Body)
		}
	}
	r, err := e.svc.CreatePayment(context.Background(), scope, "nomer", payments.CreatePaymentRequest{
		MerchantID: "mer_missing", Amount: 1000, Currency: "EUR", PaymentMethod: "pm_card_visa"})
	if err != nil || r.Status != 404 {
		t.Fatalf("missing merchant: %v %d", err, r.Status)
	}
}

// The response is lost after the processor charged the card. The retry must
// return the original charge, not create a second one.
func TestAmbiguousTimeoutThenRetry(t *testing.T) {
	e := newEnv(t, envOpts{faults: faults.HTTP{DropAfterProb: 1, HangMs: 2000}})
	r1, _ := e.pay("amb", 3_000, "pm_card_visa")
	if r1.Status != 503 || !r1.Retryable {
		t.Fatalf("first attempt: %d %s", r1.Status, r1.Body)
	}
	if got := e.succeededPIs()["amb"]; got != 1 {
		t.Fatalf("processor should have charged once already, got %d", got)
	}
	e.proxy.SetFaults(faults.HTTP{})
	r2, p := e.pay("amb", 3_000, "pm_card_visa")
	if r2.Status != 201 || p.Status != payments.StatusSucceeded {
		t.Fatalf("retry: %d %s", r2.Status, r2.Body)
	}
	if got := e.succeededPIs()["amb"]; got != 1 {
		t.Fatalf("card charged %d times", got)
	}
	e.audit()
}

// The client gives up after the ambiguous failure; the recovery worker must
// finish the payment on its own.
func TestRecoveryWorkerResolves(t *testing.T) {
	e := newEnv(t, envOpts{faults: faults.HTTP{DropAfterProb: 1}})
	r1, _ := e.pay("orphan", 4_000, "pm_card_visa")
	if r1.Status != 503 {
		t.Fatalf("first attempt: %d", r1.Status)
	}
	e.proxy.SetFaults(faults.HTTP{})
	waitFor(t, func() bool {
		e.worker.RunOnce(context.Background()) //nolint:errcheck
		rep := e.audit()
		return rep.Counts["payments_succeeded"] == 1
	})
	if got := e.succeededPIs()["orphan"]; got != 1 {
		t.Fatalf("card charged %d times", got)
	}
	// The client comes back later: it gets the final result.
	r2, p := e.pay("orphan", 4_000, "pm_card_visa")
	if r2.Status != 201 || p.Status != payments.StatusSucceeded {
		t.Fatalf("late retry: %d %s", r2.Status, r2.Body)
	}
}

// Stripe caches a 500 under the idempotency key, so re-sending never
// succeeds; recovery must fall back to looking the PaymentIntent up.
func TestCachedProcessorErrorUsesLookup(t *testing.T) {
	e := newEnv(t, envOpts{simCfg: stripesim.Config{InternalErrorProb: 1}})
	r1, _ := e.pay("cached500", 2_500, "pm_card_visa")
	if r1.Status != 503 {
		t.Fatalf("first attempt: %d %s", r1.Status, r1.Body)
	}
	waitFor(t, func() bool {
		e.worker.RunOnce(context.Background()) //nolint:errcheck
		return e.audit().Counts["payments_succeeded"] == 1
	})
	if got := e.succeededPIs()["cached500"]; got != 1 {
		t.Fatalf("card charged %d times", got)
	}
}

// A processor webhook resolves a payment whose response was lost.
func TestWebhookResolves(t *testing.T) {
	const secret = "whsec_test"
	var e *env
	api := httptest.NewUnstartedServer(nil)
	e = newEnv(t, envOpts{
		simCfg: stripesim.Config{WebhookURL: "http://" + api.Listener.Addr().String() + "/v1/webhooks/stripe", WebhookSecret: secret,
			WebhookDelay: 300 * time.Millisecond},
		faults: faults.HTTP{DropAfterProb: 1},
		mutate: func(c *payments.Config) { c.RetryBase = time.Hour; c.RetryMax = time.Hour }, // keep the worker out of it
	})
	api.Config.Handler = (&httpapi.Server{Svc: e.svc, Pool: e.pool, APIKeys: []string{"k"}, WebhookSecrets: []string{secret},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Handler()
	api.Start()
	t.Cleanup(api.Close)

	r1, _ := e.pay("wh", 7_000, "pm_card_visa")
	if r1.Status != 503 {
		t.Fatalf("first attempt: %d", r1.Status)
	}
	waitFor(t, func() bool { return e.audit().Counts["payments_succeeded"] == 1 })

	// Bad signature is rejected.
	req, _ := http.NewRequest("POST", api.URL+"/v1/webhooks/stripe", bytes.NewReader([]byte(`{"id":"evt_x","type":"payment_intent.succeeded"}`)))
	req.Header.Set("Stripe-Signature", "t=1,v1=deadbeef")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("bad signature accepted: %d", resp.StatusCode)
	}
}

func TestRefunds(t *testing.T) {
	e := newEnv(t, envOpts{})
	ctx := context.Background()
	_, p := e.pay("rp", 10_000, "pm_card_visa")

	refund := func(key string, amount int64) (*payments.Response, payments.Refund) {
		r, err := e.svc.CreateRefund(ctx, scope, key, p.ID, payments.CreateRefundRequest{Amount: amount})
		if err != nil {
			t.Fatal(err)
		}
		var rf payments.Refund
		json.Unmarshal(r.Body, &rf) //nolint:errcheck
		return r, rf
	}
	if r, rf := refund("r1", 3_000); r.Status != 201 || rf.Status != payments.StatusSucceeded {
		t.Fatalf("partial refund: %d %s", r.Status, r.Body)
	}
	if r, _ := refund("r2", 8_000); r.Status != 400 {
		t.Fatalf("over-refund allowed: %d %s", r.Status, r.Body)
	}

	// Concurrent full refunds of the remainder: exactly one may win.
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, rf := refund(fmt.Sprintf("rc%d", i), 0)
			if r.Status == 201 && rf.Status == payments.StatusSucceeded {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d concurrent refunds succeeded", wins)
	}
	clearing, _ := ledger.GetAccount(ctx, e.pool, ledger.ProcessorClearing("EUR"))
	if clearing.Balance != 0 {
		t.Fatalf("clearing balance after full refund = %d", clearing.Balance)
	}
	var simRefunded int64
	for _, rf := range e.sim.Snapshot().Refunds {
		simRefunded += rf.Amount
	}
	if simRefunded != 10_000 {
		t.Fatalf("processor refunded %d", simRefunded)
	}
	e.audit()
}

// The unsafe baseline exists to show what the safe path prevents: with the
// same fault, it charges the card twice and records a failed payment for a
// successful charge.
func TestUnsafeModeDoubleCharges(t *testing.T) {
	e := newEnv(t, envOpts{faults: faults.HTTP{DropAfterProb: 1}, mutate: func(c *payments.Config) { c.Unsafe = true }})
	r1, _ := e.pay("naive", 3_000, "pm_card_visa")
	if r1.Status != 502 {
		t.Fatalf("first attempt: %d", r1.Status)
	}
	e.proxy.SetFaults(faults.HTTP{})
	r2, _ := e.pay("naive", 3_000, "pm_card_visa")
	if r2.Status != 201 {
		t.Fatalf("retry: %d", r2.Status)
	}
	if got := e.succeededPIs()["naive"]; got != 2 {
		t.Fatalf("expected the unsafe baseline to double charge, got %d charges", got)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("condition not met within 10s")
}

// Captures and refunds spread over the shard accounts of the hot system
// accounts; the logical balances are the sums and still match the processor.
func TestHotAccountsAreSharded(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, envOpts{mutate: func(c *payments.Config) { c.LedgerShards = 4 }})
	const n = 40
	var captured, fees int64
	var ids []string
	for i := 0; i < n; i++ {
		resp, p := e.pay(fmt.Sprintf("shard-%d", i), int64(1_000+i*13), "pm_card_visa")
		if resp.Status != 201 || p.Status != payments.StatusSucceeded {
			t.Fatalf("payment %d: %d %s", i, resp.Status, resp.Body)
		}
		captured += p.Amount
		fees += *p.Fee
		ids = append(ids, p.ID)
	}
	var refunded int64
	for i := 0; i < 8; i++ {
		resp, err := e.svc.CreateRefund(ctx, scope, fmt.Sprintf("rf-%d", i), ids[i], payments.CreateRefundRequest{Amount: 100})
		if err != nil || resp.Status != 201 {
			t.Fatalf("refund %d: %v %v", i, err, resp)
		}
		refunded += 100
	}

	clearing, err := ledger.GetAccount(ctx, e.pool, ledger.ProcessorClearing("EUR"))
	if err != nil {
		t.Fatal(err)
	}
	feeAcct, _ := ledger.GetAccount(ctx, e.pool, ledger.FeeRevenue("EUR"))
	if clearing.Balance != captured-refunded || feeAcct.Balance != -fees {
		t.Fatalf("clearing=%d want %d; fees=%d want %d", clearing.Balance, captured-refunded, feeAcct.Balance, -fees)
	}
	if len(clearing.ShardIDs) < 2 || len(clearing.ShardIDs) > 4 || len(feeAcct.ShardIDs) < 2 {
		t.Fatalf("payments did not spread: clearing shards %v, fee shards %v", clearing.ShardIDs, feeAcct.ShardIDs)
	}
	bal, err := audit.ClearingBalances(ctx, e.pool)
	if err != nil || bal["EUR"] != captured-refunded {
		t.Fatalf("ClearingBalances = %v, %v", bal, err)
	}
	// Retrying a capture is idempotent: still exactly one entry per payment.
	e.audit()
}

// With one shard the system accounts are single rows named as before
// sharding, and the data written under a larger shard count stays readable.
func TestSingleShardKeepsLegacyAccountCodes(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, envOpts{mutate: func(c *payments.Config) { c.LedgerShards = 1 }})
	for i := 0; i < 5; i++ {
		e.pay(fmt.Sprintf("one-%d", i), 5_000, "pm_card_visa")
	}
	a, err := ledger.GetAccount(ctx, e.pool, ledger.ProcessorClearing("EUR"))
	if err != nil || a.Balance != 25_000 || len(a.ShardIDs) != 0 {
		t.Fatalf("clearing = %+v, %v", a, err)
	}
	var n int
	e.pool.QueryRow(ctx, `SELECT count(*) FROM accounts WHERE code LIKE '%#%'`).Scan(&n) //nolint:errcheck
	if n != 0 {
		t.Fatalf("%d shard accounts created with LedgerShards=1", n)
	}
	e.audit()
}

func payableRows(t *testing.T, e *env, merchant string) (rows int, shardCodes int) {
	t.Helper()
	ctx := context.Background()
	if err := e.pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE code LIKE '%#%') FROM accounts WHERE merchant_id = $1`, merchant).Scan(&rows, &shardCodes); err != nil {
		t.Fatal(err)
	}
	return rows, shardCodes
}

// A merchant above the configured rate gets its payable account spread over
// shards. The merchant's logical balance, its API view and the audit all
// stay exact, including refunds that land on different shards than their
// captures.
func TestHotMerchantPayableIsSharded(t *testing.T) {
	ctx := context.Background()
	// Any traffic at all counts as "hot" at this rate: ceil(1 / 0.001) hits
	// the cap of 4 shards.
	e := newEnv(t, envOpts{mutate: func(c *payments.Config) { c.PayableShardRate = 0.001; c.PayableMaxShards = 4 }})
	const n = 40
	var net, refunded int64
	var ids []string
	for i := 0; i < n; i++ {
		resp, p := e.pay(fmt.Sprintf("hot-%d", i), int64(1_000+i*13), "pm_card_visa")
		if resp.Status != 201 || p.Status != payments.StatusSucceeded {
			t.Fatalf("payment %d: %d %s", i, resp.Status, resp.Body)
		}
		net += p.Amount - *p.Fee
		ids = append(ids, p.ID)
	}
	for i := 0; i < 8; i++ {
		resp, err := e.svc.CreateRefund(ctx, scope, fmt.Sprintf("hot-rf-%d", i), ids[i], payments.CreateRefundRequest{Amount: 100})
		if err != nil || resp.Status != 201 {
			t.Fatalf("refund %d: %v %v", i, err, resp)
		}
		refunded += 100
	}

	payable, err := ledger.GetAccount(ctx, e.pool, ledger.MerchantPayable(e.mer, "EUR"))
	if err != nil {
		t.Fatal(err)
	}
	if want := -(net - refunded); payable.Balance != want {
		t.Fatalf("logical payable balance = %d, want %d", payable.Balance, want)
	}
	if len(payable.ShardIDs) < 2 || len(payable.ShardIDs) > 4 {
		t.Fatalf("payable spread over %v, want 2-4 shard rows", payable.ShardIDs)
	}
	if payable.MerchantID == nil || *payable.MerchantID != e.mer {
		t.Fatalf("payable merchant = %v", payable.MerchantID)
	}

	// The merchant endpoint shows one logical payable account, not shards.
	resp, err := e.svc.GetMerchant(ctx, e.mer)
	if err != nil || resp.Status != 200 {
		t.Fatalf("GetMerchant: %v %v", err, resp)
	}
	var view struct {
		Accounts []ledger.Account `json:"accounts"`
	}
	mustUnmarshal(t, resp.Body, &view)
	if len(view.Accounts) != 1 || view.Accounts[0].Code != ledger.MerchantPayable(e.mer, "EUR") || view.Accounts[0].Balance != payable.Balance {
		t.Fatalf("merchant accounts = %+v, want one logical payable with balance %d", view.Accounts, payable.Balance)
	}
	posts, err := ledger.AccountPostings(ctx, e.pool, payable, 0, 500)
	if err != nil || len(posts) != n+8 {
		t.Fatalf("payable postings = %d (%v), want %d across shards", len(posts), err, n+8)
	}
	e.audit()
}

// Merchants below the rate keep the single legacy row; so does everyone when
// the feature is off.
func TestColdMerchantPayableStaysOneRow(t *testing.T) {
	for name, mutate := range map[string]func(*payments.Config){
		"below rate": func(c *payments.Config) { c.PayableShardRate = 1e9 },
		"disabled":   func(c *payments.Config) { c.PayableShardRate = 0 },
		"max one":    func(c *payments.Config) { c.PayableShardRate = 0.001; c.PayableMaxShards = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, envOpts{mutate: mutate})
			var net int64
			for i := 0; i < 10; i++ {
				_, p := e.pay(fmt.Sprintf("cold-%d", i), 5_000, "pm_card_visa")
				net += p.Amount - *p.Fee
			}
			rows, shards := payableRows(t, e, e.mer)
			if rows != 1 || shards != 0 {
				t.Fatalf("merchant has %d account rows (%d shard codes), want the one legacy row", rows, shards)
			}
			a, err := ledger.GetAccount(context.Background(), e.pool, ledger.MerchantPayable(e.mer, "EUR"))
			if err != nil || a.Balance != -net {
				t.Fatalf("payable = %+v, %v", a, err)
			}
			e.audit()
		})
	}
}

// Concurrent captures on one hot merchant, with the rate tracker flipping the
// shard count while they run, must not deadlock, lose an update or double
// post, and must leave the audit clean.
func TestHotMerchantConcurrentCaptures(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, envOpts{mutate: func(c *payments.Config) { c.PayableShardRate = 50; c.PayableMaxShards = 8 }})
	const workers, per = 16, 12
	var wg sync.WaitGroup
	var mu sync.Mutex
	var net int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				resp, p := e.pay(fmt.Sprintf("cc-%d-%d", w, i), int64(2_000+w*100+i), "pm_card_visa")
				if resp.Status != 201 || p.Status != payments.StatusSucceeded {
					t.Errorf("payment: %d %s", resp.Status, resp.Body)
					return
				}
				mu.Lock()
				net += p.Amount - *p.Fee
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	payable, err := ledger.GetAccount(ctx, e.pool, ledger.MerchantPayable(e.mer, "EUR"))
	if err != nil {
		t.Fatal(err)
	}
	if payable.Balance != -net {
		t.Fatalf("payable = %d, want %d", payable.Balance, -net)
	}
	if len(payable.ShardIDs) < 2 {
		t.Fatalf("the load never pushed the merchant over the shard rate (rows %v)", payable.ShardIDs)
	}
	rep := e.audit()
	if rep.Counts["journal_entries"] != workers*per {
		t.Fatalf("journal entries = %d, want %d", rep.Counts["journal_entries"], workers*per)
	}
	bal, err := audit.ClearingBalances(ctx, e.pool)
	if err != nil || bal["EUR"] == 0 {
		t.Fatalf("ClearingBalances = %v, %v", bal, err)
	}
}

// The processor may deliver the same webhook event several times, even
// concurrently. The event is applied once: one captured payment, one journal
// entry, one stored event, and every delivery is acknowledged. A delivery
// signed too long ago is refused.
func TestWebhookDuplicateDelivery(t *testing.T) {
	const secret = "whsec_dup"
	ctx := context.Background()
	e := newEnv(t, envOpts{
		faults: faults.HTTP{DropAfterProb: 1}, // the charge happens, the response is lost
		mutate: func(c *payments.Config) { c.RetryBase = time.Hour; c.RetryMax = time.Hour },
	})
	srv := httptest.NewServer((&httpapi.Server{Svc: e.svc, Pool: e.pool, APIKeys: []string{"k"}, WebhookSecrets: []string{secret},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}).Handler())
	t.Cleanup(srv.Close)

	if r, _ := e.pay("dup", 9_000, "pm_card_visa"); r.Status != 503 {
		t.Fatalf("first attempt: %d", r.Status)
	}
	pis := e.sim.Snapshot().PaymentIntents
	if len(pis) != 1 {
		t.Fatalf("processor has %d PaymentIntents", len(pis))
	}
	payload, err := json.Marshal(map[string]any{
		"id": "evt_dup_1", "object": "event", "type": "payment_intent.succeeded", "created": time.Now().Unix(),
		"data": map[string]any{"object": pis[0].PaymentIntent},
	})
	if err != nil {
		t.Fatal(err)
	}
	deliver := func(sigTime time.Time) int {
		req, _ := http.NewRequest("POST", srv.URL+"/v1/webhooks/stripe", bytes.NewReader(payload))
		req.Header.Set("Stripe-Signature", stripe.SignatureHeader(payload, secret, sigTime))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if code := deliver(time.Now().Add(-10 * time.Minute)); code != 400 {
		t.Fatalf("stale signature accepted: %d", code)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if code := deliver(time.Now()); code != 200 {
				t.Errorf("duplicate delivery returned %d", code)
			}
		}()
	}
	wg.Wait()
	if code := deliver(time.Now()); code != 200 { // a late redelivery
		t.Fatalf("redelivery returned %d", code)
	}

	rep := e.audit()
	if rep.Counts["payments_succeeded"] != 1 || rep.Counts["journal_entries"] != 1 || rep.Counts["processor_events"] != 1 {
		t.Fatalf("after 9 deliveries: succeeded=%d journal=%d stored events=%d, want 1 of each",
			rep.Counts["payments_succeeded"], rep.Counts["journal_entries"], rep.Counts["processor_events"])
	}
	payable, err := ledger.GetAccount(ctx, e.pool, ledger.MerchantPayable(e.mer, "EUR"))
	if err != nil || payable.Balance != -(9_000-(9_000*290+5_000)/10_000-30) {
		t.Fatalf("payable = %+v, %v", payable, err)
	}
}

// Two requests with one Idempotency-Key arriving together must not deadlock
// each other. One finishes the payment (locking the payment row, then the
// key row) while the other re-reads the payment under the key's lock; if the
// two paths lock in opposite orders PostgreSQL aborts one after
// deadlock_timeout (1 s) and the request is retried, which is correct but
// shows up as a one-second tail and as db.TxRetries.
func TestConcurrentDuplicatesDoNotDeadlock(t *testing.T) {
	e := newEnv(t, envOpts{})
	// A short timeout makes an inversion fail fast instead of stalling a
	// second each time.
	if _, err := e.pool.Exec(context.Background(), `ALTER DATABASE `+quoteIdent(e.pool.Config().ConnConfig.Database)+` SET deadlock_timeout = '20ms'`); err != nil {
		t.Fatal(err)
	}
	e.pool.Reset() // new connections pick the setting up
	before := db.TxRetries()

	const keys, copies = 120, 3
	sem := make(chan struct{}, 12)
	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		for c := 0; c < copies; c++ {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				if resp, _ := e.pay(fmt.Sprintf("dup-%d", k), 5_000+int64(k), "pm_card_visa"); resp.Status != 201 {
					t.Errorf("key %d: status %d %s", k, resp.Status, resp.Body)
				}
			}(k)
		}
	}
	wg.Wait()
	if n := db.TxRetries() - before; n != 0 {
		t.Fatalf("%d transactions were rolled back and retried after a deadlock or serialization failure", n)
	}
	rep := e.audit()
	if rep.Counts["payments_succeeded"] != keys || rep.Counts["journal_entries"] != keys {
		t.Fatalf("succeeded=%d journal=%d, want %d of each (one payment per key)", rep.Counts["payments_succeeded"], rep.Counts["journal_entries"], keys)
	}
}

func quoteIdent(s string) string { return `"` + s + `"` }

// The same for refunds: concurrent requests with one Idempotency-Key create
// one refund and do not deadlock.
func TestConcurrentDuplicateRefundsDoNotDeadlock(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, envOpts{})
	if _, err := e.pool.Exec(ctx, `ALTER DATABASE `+quoteIdent(e.pool.Config().ConnConfig.Database)+` SET deadlock_timeout = '20ms'`); err != nil {
		t.Fatal(err)
	}
	e.pool.Reset()
	const n, copies = 60, 3
	ids := make([]string, n)
	for i := range ids {
		_, p := e.pay(fmt.Sprintf("rfd-pay-%d", i), 8_000, "pm_card_visa")
		ids[i] = p.ID
	}
	before := db.TxRetries()
	sem := make(chan struct{}, 12)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		for c := 0; c < copies; c++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				resp, err := e.svc.CreateRefund(ctx, scope, fmt.Sprintf("rfd-%d", i), ids[i], payments.CreateRefundRequest{Amount: 1_000})
				if err != nil || resp.Status != 201 {
					t.Errorf("refund %d: %v %v", i, err, resp)
				}
			}(i)
		}
	}
	wg.Wait()
	if k := db.TxRetries() - before; k != 0 {
		t.Fatalf("%d transactions were retried after a deadlock or serialization failure", k)
	}
	rep := e.audit()
	if rep.Counts["refunds_succeeded"] != n {
		t.Fatalf("refunds succeeded = %d, want %d (one per key)", rep.Counts["refunds_succeeded"], n)
	}
}
