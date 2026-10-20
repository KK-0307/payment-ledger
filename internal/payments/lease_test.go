package payments

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"paymentledger/internal/db"
	"paymentledger/internal/idempotency"
	"paymentledger/internal/ledger"
	"paymentledger/internal/processor"
	"paymentledger/internal/testutil"
)

// stubProc is a processor whose Charge takes a fixed time and is counted.
type stubProc struct {
	delay   time.Duration
	charges atomic.Int32
}

func (s *stubProc) Charge(_ context.Context, req processor.ChargeRequest) (processor.Outcome, error) {
	s.charges.Add(1)
	time.Sleep(s.delay)
	return processor.Outcome{Result: processor.Succeeded, ProcessorID: "pi_stub_" + req.PaymentID}, nil
}
func (s *stubProc) LookupCharge(context.Context, string, string) (processor.Outcome, bool, error) {
	return processor.Outcome{}, false, nil
}
func (s *stubProc) Refund(context.Context, processor.RefundRequest) (processor.Outcome, error) {
	return processor.Outcome{}, &processor.UnknownError{Reason: "stub", Err: io.EOF}
}
func (s *stubProc) LookupRefund(context.Context, string, string, string) (processor.Outcome, bool, error) {
	return processor.Outcome{}, false, nil
}

func newLeaseSvc(t *testing.T, proc processor.Processor, lease, wait time.Duration) (*Service, *pgxpool.Pool, string) {
	t.Helper()
	pool := testutil.NewDB(t)
	cfg := DefaultConfig()
	cfg.Lease, cfg.InflightWait = lease, wait
	cfg.RetryBase, cfg.RetryMax = 10*time.Millisecond, 50*time.Millisecond
	svc := New(pool, ledger.New(), proc, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	resp, err := svc.CreateMerchant(context.Background(), "s", "", CreateMerchantRequest{Name: "m"})
	if err != nil {
		t.Fatal(err)
	}
	var m Merchant
	if err := json.Unmarshal(resp.Body, &m); err != nil {
		t.Fatal(err)
	}
	return svc, pool, m.ID
}

func payReq(merchant string) CreatePaymentRequest {
	return CreatePaymentRequest{MerchantID: merchant, Amount: 2_000, Currency: "EUR", PaymentMethod: "pm_card_visa"}
}

func statusOf(t *testing.T, r *Response) (int, Payment) {
	t.Helper()
	var p Payment
	json.Unmarshal(r.Body, &p) //nolint:errcheck
	return r.Status, p
}

// A process that dies right after phase 1 leaves the payment leased. A retry
// must not bounce with 409 for the whole lease: it waits for the lease to
// lapse, takes over, and finishes the payment in the same request.
func TestDeadHolderTakeoverInsideOneRequest(t *testing.T) {
	proc := &stubProc{}
	svc, pool, mer := newLeaseSvc(t, proc, time.Second, 4*time.Second)
	ctx := context.Background()
	req := payReq(mer)

	// What phase 1 of a request that then crashed leaves behind.
	var crashedID string
	norm := req
	norm.normalize()
	if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
		k, err := idempotency.Acquire(ctx, tx, "s", "k-dead", "POST", "/v1/payments", idempotency.HashRequest(norm))
		if err != nil {
			return err
		}
		p, err := svc.insertPayment(ctx, tx, norm, true)
		if err != nil {
			return err
		}
		crashedID = p.ID
		return idempotency.SetRecoveryPoint(ctx, tx, k.ID, PointPaymentCreated, p.ID)
	}); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	resp, err := svc.CreatePayment(ctx, "s", "k-dead", req)
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	code, p := statusOf(t, resp)
	if code != 201 || p.Status != StatusSucceeded || p.ID != crashedID {
		t.Fatalf("got %d %s", code, resp.Body)
	}
	if elapsed < 700*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("takeover took %s; expected about the 1s lease", elapsed)
	}
	if n := proc.charges.Load(); n != 1 {
		t.Fatalf("charged %d times", n)
	}
}

// A live holder with a slow processor call must keep its lease: neither the
// recovery worker nor a concurrent retry may preempt it, and the retry gets
// the holder's result instead of 409.
func TestHeartbeatKeepsSlowHolder(t *testing.T) {
	proc := &stubProc{delay: 2500 * time.Millisecond}
	svc, _, mer := newLeaseSvc(t, proc, 900*time.Millisecond, 5*time.Second)
	ctx := context.Background()
	req := payReq(mer)

	var wg sync.WaitGroup
	var first *Response
	wg.Add(1)
	go func() {
		defer wg.Done()
		first, _ = svc.CreatePayment(ctx, "s", "k-slow", req)
	}()
	time.Sleep(1500 * time.Millisecond) // well past the 900ms lease

	// Without a heartbeat the lease and next_attempt_at have both lapsed here.
	w := &RecoveryWorker{Svc: svc, Batch: 10, Concurrency: 2, KeyMaxAge: time.Hour}
	if n, err := w.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("recovery worker claimed %d items from a live holder (err %v)", n, err)
	}
	second, err := svc.CreatePayment(ctx, "s", "k-slow", req) // waits for the holder
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	c1, p1 := statusOf(t, first)
	c2, p2 := statusOf(t, second)
	if c1 != 201 || c2 != 201 || p1.ID != p2.ID || p2.Status != StatusSucceeded {
		t.Fatalf("holder %d %s / retry %d %s", c1, first.Body, c2, second.Body)
	}
	if n := proc.charges.Load(); n != 1 {
		t.Fatalf("charged %d times; the live holder was preempted", n)
	}
}

// The wait is bounded: past INFLIGHT_WAIT the client gets a retryable 409.
func TestInflightWaitIsBounded(t *testing.T) {
	proc := &stubProc{delay: 1500 * time.Millisecond}
	svc, _, mer := newLeaseSvc(t, proc, 5*time.Second, 200*time.Millisecond)
	ctx := context.Background()
	req := payReq(mer)

	done := make(chan struct{})
	go func() { svc.CreatePayment(ctx, "s", "k-bounded", req); close(done) }() //nolint:errcheck
	time.Sleep(300 * time.Millisecond)

	start := time.Now()
	resp, err := svc.CreatePayment(ctx, "s", "k-bounded", req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 409 || !resp.Retryable {
		t.Fatalf("got %d %s", resp.Status, resp.Body)
	}
	if d := time.Since(start); d < 150*time.Millisecond || d > time.Second {
		t.Fatalf("waited %s, want about 200ms", d)
	}
	<-done
}
