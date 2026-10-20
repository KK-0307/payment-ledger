// Package payments implements payment and refund flows on top of the ledger.
//
// A payment request runs in three atomic phases:
//
//  1. One transaction acquires the client's idempotency key, inserts the
//     payment as 'processing' with a lease, and records recovery point
//     payment_created.
//  2. Outside any transaction, the processor is called with a processor
//     idempotency key derived from the payment id (stable across retries).
//  3. One transaction applies the outcome: status update, outbox event,
//     idempotency response, and (for captures) the ledger journal entry.
//
// If phase 2's outcome is unknown, the payment stays 'processing'. A client
// retry with the same key, the recovery worker, or a processor webhook
// resolves it later; all of them converge on the same guarded transition
// (UPDATE ... WHERE status = 'processing'), and the ledger's unique
// (kind, reference) makes double-posting impossible.
package payments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"paymentledger/internal/db"
	"paymentledger/internal/id"
	"paymentledger/internal/idempotency"
	"paymentledger/internal/ledger"
	"paymentledger/internal/metrics"
	"paymentledger/internal/outbox"
	"paymentledger/internal/processor"
)

type Config struct {
	FeeBps   int64 // fee in basis points of the amount
	FeeFixed int64 // fixed fee per successful payment, minor units

	// Lease is how long a holder owns an in-flight payment/refund without
	// renewing it. Holders renew every Lease/3 while they work (heartbeat), so
	// Lease only bounds how long a *dead* holder blocks others; it need not
	// exceed the processor timeout.
	Lease time.Duration
	// InflightWait: a request that finds its payment in flight waits up to
	// this long for it to finish or its lease to expire (then takes over)
	// instead of immediately answering 409.
	InflightWait  time.Duration
	RetryBase     time.Duration // recovery backoff base
	RetryMax      time.Duration // recovery backoff cap
	MaxAttempts   int           // unknown outcomes before needs_review
	LookupAfter   int           // attempts after which recovery also looks the object up
	InlineRetries int           // immediate processor retries inside a request
	InlineDelay   time.Duration
	// Processor idempotency keys expire (Stripe: 24h). Never re-send a
	// create older than this; only look it up.
	KeyMaxAge time.Duration
	// RequestBudget bounds phases 2-3 of a request, detached from the
	// client's context so a client disconnect does not abandon work.
	RequestBudget time.Duration

	// LedgerShards is how many shard accounts processor_clearing and
	// fee_revenue are split into (see ledger.ShardFor). 1 means a single
	// row per currency. It can be changed at any time: existing shards keep
	// their balances and the logical balance is always the sum.
	LedgerShards int

	// A merchant's payable account is one row until the merchant's capture
	// and refund rate (per API instance) passes PayableShardRate events per
	// second; from then on it is spread over ceil(rate / PayableShardRate)
	// shards, at most PayableMaxShards. 0 disables it: every merchant keeps a
	// single payable row.
	PayableShardRate float64
	PayableMaxShards int

	// Unsafe disables every idempotency mechanism. It exists only as the
	// benchmark baseline that shows what the mechanisms prevent.
	Unsafe bool
}

func DefaultConfig() Config {
	return Config{
		FeeBps: 290, FeeFixed: 30,
		Lease: 10 * time.Second, InflightWait: 3 * time.Second, RetryBase: time.Second, RetryMax: time.Minute,
		MaxAttempts: 20, LookupAfter: 3, InlineRetries: 1, InlineDelay: 100 * time.Millisecond,
		KeyMaxAge: 23 * time.Hour, RequestBudget: 30 * time.Second,
		LedgerShards:     ledger.DefaultShards,
		PayableShardRate: DefaultPayableShardRate, PayableMaxShards: ledger.DefaultShards,
	}
}

// DefaultPayableShardRate is the events per second one shard of a merchant's
// payable account is sized for. A row lock held through the commit's WAL
// flush serialises its transactions, so a row cannot do much more than a few
// hundred per second; the default leaves a wide margin.
const DefaultPayableShardRate = 25

type Service struct {
	pool   *pgxpool.Pool
	ledger *ledger.Ledger
	proc   processor.Processor
	cfg    Config
	log    *slog.Logger
	rates  *ledger.RateTracker // per merchant+currency, for payable sharding
}

func New(pool *pgxpool.Pool, l *ledger.Ledger, p processor.Processor, cfg Config, log *slog.Logger) *Service {
	return &Service{pool: pool, ledger: l, proc: p, cfg: cfg, log: log, rates: ledger.NewRateTracker()}
}

func (s *Service) Config() Config { return s.cfg }

// ---------------------------------------------------------------- merchants

type CreateMerchantRequest struct {
	Name string `json:"name"`
}

func (s *Service) CreateMerchant(ctx context.Context, scope, key string, req CreateMerchantRequest) (*Response, error) {
	if req.Name == "" || len(req.Name) > 200 {
		return errorResponse(400, "invalid_request_error", "name_invalid", "name must be 1-200 characters"), nil
	}
	create := func(tx pgx.Tx) (*Response, error) {
		m := Merchant{ID: id.New("mer"), Object: "merchant", Name: req.Name}
		if err := tx.QueryRow(ctx, `INSERT INTO merchants (id, name) VALUES ($1, $2) RETURNING created_at`, m.ID, m.Name).Scan(&m.CreatedAt); err != nil {
			return nil, err
		}
		return jsonResponse(http.StatusCreated, m), nil
	}
	if key == "" {
		var resp *Response
		err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) (err error) { resp, err = create(tx); return })
		return resp, err
	}
	if err := idempotency.ValidateKey(key); err != nil {
		return errorResponse(400, "invalid_request_error", "idempotency_key_invalid", err.Error()), nil
	}
	var resp *Response
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		k, err := idempotency.Acquire(ctx, tx, scope, key, "POST", "/v1/merchants", idempotency.HashRequest(req))
		if errors.Is(err, idempotency.ErrMismatch) {
			resp = keyMismatch()
			return nil
		}
		if err != nil {
			return err
		}
		if k.Finished() {
			resp = replay(k)
			return nil
		}
		if resp, err = create(tx); err != nil {
			return err
		}
		return idempotency.Finish(ctx, tx, k.ID, resp.Status, resp.Body)
	})
	return resp, err
}

func (s *Service) GetMerchant(ctx context.Context, merchantID string) (*Response, error) {
	var m Merchant
	err := s.pool.QueryRow(ctx, `SELECT id, name, created_at FROM merchants WHERE id = $1`, merchantID).Scan(&m.ID, &m.Name, &m.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return notFound("merchant"), nil
	}
	if err != nil {
		return nil, err
	}
	m.Object = "merchant"
	accounts, err := ledger.MerchantAccounts(ctx, s.pool, merchantID)
	if err != nil {
		return nil, err
	}
	return jsonResponse(200, map[string]any{"merchant": m, "accounts": accounts}), nil
}

// ----------------------------------------------------------------- payments

func (s *Service) CreatePayment(ctx context.Context, scope, key string, req CreatePaymentRequest) (*Response, error) {
	if bad := req.normalize(); bad != nil {
		return bad, nil
	}
	if s.cfg.Unsafe {
		return s.createPaymentUnsafe(ctx, req)
	}
	if err := idempotency.ValidateKey(key); err != nil {
		return errorResponse(400, "invalid_request_error", "idempotency_key_required", err.Error()), nil
	}

	var (
		resp     *Response
		p        *Payment
		keyID    int64
		inflight bool
	)
	phase1 := func(tx pgx.Tx) error {
		resp, p, inflight = nil, nil, false
		k, err := idempotency.Acquire(ctx, tx, scope, key, "POST", "/v1/payments", idempotency.HashRequest(req))
		if errors.Is(err, idempotency.ErrMismatch) {
			resp = keyMismatch()
			return nil
		}
		if err != nil {
			return err
		}
		keyID = k.ID
		if k.Finished() {
			resp = replay(k)
			return nil
		}
		if k.RecoveryPoint == idempotency.PointStarted {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM merchants WHERE id = $1)`, req.MerchantID).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				resp = errorResponse(404, "invalid_request_error", "merchant_not_found", "no such merchant: "+req.MerchantID)
				return idempotency.Finish(ctx, tx, k.ID, resp.Status, resp.Body)
			}
			if p, err = s.insertPayment(ctx, tx, req, true); err != nil {
				return err
			}
			return idempotency.SetRecoveryPoint(ctx, tx, k.ID, PointPaymentCreated, p.ID)
		}
		// Resuming a request that did not finish (crash, unknown outcome).
		if k.ResourceID == nil {
			return fmt.Errorf("idempotency key %d at %s without resource", k.ID, k.RecoveryPoint)
		}
		if p, err = lockPayment(ctx, tx, *k.ResourceID); err != nil {
			return err
		}
		if p.Status != StatusProcessing {
			return nil
		}
		if p.LeaseLeft > 0 {
			inflight = true
			return nil
		}
		return takePaymentLease(ctx, tx, p.ID, s.cfg.Lease)
	}
	deadline := time.Now().Add(s.cfg.InflightWait)
	for {
		if err := db.WithTx(ctx, s.pool, phase1); err != nil {
			return nil, err
		}
		if !inflight {
			break
		}
		// Another request, the recovery worker, or a dead process holds the
		// payment. Wait for it to finish, or for its lease to lapse, then
		// continue (taking over) instead of bouncing the client with 409.
		if !time.Now().Before(deadline) {
			return inProgress(), nil
		}
		if err := sleepCtx(ctx, s.pollInterval()); err != nil {
			return nil, err
		}
	}
	if resp != nil {
		return resp, nil
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.RequestBudget)
	defer cancel()
	if p.Status != StatusProcessing {
		return s.finishPaymentKey(ctx, p.ID, keyID)
	}
	stop := s.holdLease("payments", p.ID)
	o, perr := s.resolveCharge(ctx, p)
	stop() // before phase 3, so a late heartbeat cannot re-lease what phase 3 releases
	_, resp, err := s.applyCharge(ctx, p, o, perr, keyID, "request")
	return resp, err
}

func (s *Service) pollInterval() time.Duration {
	return min(100*time.Millisecond, max(10*time.Millisecond, s.cfg.Lease/20))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// holdLease renews the lease of a payment or refund every Lease/3 until the
// returned stop function is called. table is "payments" or "refunds"
// (internal constants, never user input). A holder that dies stops renewing,
// so its lease lapses within Lease and others can take over; a live holder
// is never preempted just because its processor call is slow.
//
// The lease is an optimisation, not a safety mechanism: if two parties ever
// do overlap, the processor idempotency key and the guarded status update
// keep the outcome single.
func (s *Service) holdLease(table, id string) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	q := `UPDATE ` + table + ` SET lease_until = now() + make_interval(secs => $2) WHERE id = $1 AND status = 'processing'`
	every := max(s.cfg.Lease/3, 10*time.Millisecond)
	go func() {
		defer close(done)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				cctx, c := context.WithTimeout(ctx, every)
				_, err := s.pool.Exec(cctx, q, id, s.cfg.Lease.Seconds())
				c()
				if err != nil && ctx.Err() == nil {
					s.log.Warn("renew lease", "table", table, "id", id, "err", err)
				}
			}
		}
	}()
	return func() { cancel(); <-done }
}

const paymentCols = `id, merchant_id, amount, currency, payment_method, description, metadata, status,
	processor_id, processor_idempotency_key, failure_code, failure_message, fee, amount_refunded,
	attempts, coalesce(greatest(extract(epoch FROM lease_until - now()), 0), 0)::float8, created_at, updated_at`

func scanPayment(row pgx.Row) (*Payment, error) {
	var p Payment
	var md []byte
	var leaseLeft float64
	err := row.Scan(&p.ID, &p.MerchantID, &p.Amount, &p.Currency, &p.PaymentMethod, &p.Description, &md, &p.Status,
		&p.ProcessorID, &p.ProcessorKey, &p.FailureCode, &p.FailureMessage, &p.Fee, &p.AmountRefunded,
		&p.Attempts, &leaseLeft, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return nil, err
	}
	p.LeaseLeft = time.Duration(leaseLeft * float64(time.Second))
	p.Object = "payment"
	if err := json.Unmarshal(md, &p.Metadata); err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *Service) insertPayment(ctx context.Context, tx pgx.Tx, req CreatePaymentRequest, safe bool) (*Payment, error) {
	pid := id.New("pay")
	var procKey *string
	var lease any // NULL interval when unsafe
	if safe {
		k := "pl-pay-" + pid
		procKey = &k
		lease = s.cfg.Lease.Seconds()
	}
	md, _ := json.Marshal(req.Metadata)
	return scanPayment(tx.QueryRow(ctx, `
		INSERT INTO payments (id, merchant_id, amount, currency, payment_method, description, metadata, status,
			processor_idempotency_key, lease_until, next_attempt_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 'processing', $8,
			now() + make_interval(secs => $9), now() + make_interval(secs => $9))
		RETURNING `+paymentCols,
		pid, req.MerchantID, req.Amount, req.Currency, req.PaymentMethod, req.Description, md, procKey, lease))
}

func lockPayment(ctx context.Context, tx pgx.Tx, paymentID string) (*Payment, error) {
	return scanPayment(tx.QueryRow(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1 FOR UPDATE`, paymentID))
}

func takePaymentLease(ctx context.Context, tx pgx.Tx, paymentID string, lease time.Duration) error {
	_, err := tx.Exec(ctx, `UPDATE payments SET lease_until = now() + make_interval(secs => $2) WHERE id = $1`, paymentID, lease.Seconds())
	return err
}

// resolveCharge asks the processor for the outcome of payment p, creating
// the charge if the processor has not seen it. Safe to call any number of
// times: creates are deduplicated by the processor idempotency key.
func (s *Service) resolveCharge(ctx context.Context, p *Payment) (processor.Outcome, error) {
	if p.ProcessorID != nil {
		return s.lookupCharge(ctx, p, *p.ProcessorID)
	}
	if time.Since(p.CreatedAt) > s.cfg.KeyMaxAge {
		return s.lookupCharge(ctx, p, "")
	}
	o, err := s.charge(ctx, p)
	if err != nil && processor.IsUnknown(err) && p.Attempts >= s.cfg.LookupAfter {
		// Stripe stores the result of a request that failed mid-execution
		// (a 500) under its idempotency key, so retrying returns the same
		// 500 forever. Look the PaymentIntent up instead.
		if lo, found, lerr := s.proc.LookupCharge(ctx, p.ID, ""); lerr == nil && found {
			metrics.ProcessorCalls.WithLabelValues("lookup_charge", string(lo.Result)).Inc()
			return lo, nil
		}
	}
	return o, err
}

func (s *Service) lookupCharge(ctx context.Context, p *Payment, processorID string) (processor.Outcome, error) {
	start := time.Now()
	o, found, err := s.proc.LookupCharge(ctx, p.ID, processorID)
	metrics.ProcessorDuration.WithLabelValues("lookup_charge").Observe(time.Since(start).Seconds())
	switch {
	case err != nil:
		metrics.ProcessorCalls.WithLabelValues("lookup_charge", "unknown").Inc()
		return o, &processor.UnknownError{Reason: "lookup", Err: err}
	case !found:
		metrics.ProcessorCalls.WithLabelValues("lookup_charge", "not_found").Inc()
		return o, &processor.UnknownError{Reason: "not_found", Err: errors.New("processor has no record of payment " + p.ID)}
	}
	metrics.ProcessorCalls.WithLabelValues("lookup_charge", string(o.Result)).Inc()
	return o, nil
}

func (s *Service) charge(ctx context.Context, p *Payment) (processor.Outcome, error) {
	req := processor.ChargeRequest{
		PaymentID: p.ID, MerchantID: p.MerchantID, Amount: p.Amount, Currency: p.Currency,
		PaymentMethod: p.PaymentMethod, Description: p.Description, Metadata: p.Metadata,
	}
	if p.ProcessorKey != nil {
		req.IdempotencyKey = *p.ProcessorKey
	}
	var (
		o   processor.Outcome
		err error
	)
	for attempt := 0; attempt <= s.cfg.InlineRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return o, &processor.UnknownError{Reason: "context", Err: ctx.Err()}
			case <-time.After(s.cfg.InlineDelay * time.Duration(attempt)):
			}
		}
		start := time.Now()
		o, err = s.proc.Charge(ctx, req)
		metrics.ProcessorDuration.WithLabelValues("charge").Observe(time.Since(start).Seconds())
		if err == nil {
			metrics.ProcessorCalls.WithLabelValues("charge", string(o.Result)).Inc()
			return o, nil
		}
		metrics.ProcessorCalls.WithLabelValues("charge", "unknown").Inc()
		if !processor.IsUnknown(err) {
			return o, &processor.UnknownError{Reason: "unexpected", Err: err}
		}
	}
	return o, err
}

// applyCharge records the result of resolveCharge. keyID != 0 also finishes
// (or releases) the client's idempotency key and returns the HTTP response.
func (s *Service) applyCharge(ctx context.Context, p *Payment, o processor.Outcome, perr error, keyID int64, source string) (*Payment, *Response, error) {
	var accts *captureAccounts
	if perr == nil && o.Result == processor.Succeeded {
		var err error
		if accts, err = s.captureAccounts(ctx, p.MerchantID, p.Currency, p.ID); err != nil {
			return nil, nil, err
		}
	}
	var (
		out  *Payment
		resp *Response
	)
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) (err error) {
		out, resp, err = s.applyChargeTx(ctx, tx, p.ID, o, perr, keyID, accts, source)
		return err
	})
	return out, resp, err
}

type captureAccounts struct{ clearing, payable, fees int64 }

// captureAccounts resolves the accounts for a posting. The hot system
// accounts are sharded: shardKey (the payment or refund id) picks the shard,
// so concurrent postings lock different rows. Shards are created on first use.
// A merchant's payable account is sharded too once the merchant is busy
// enough (payableShards).
func (s *Service) captureAccounts(ctx context.Context, merchantID, currency, shardKey string) (*captureAccounts, error) {
	var a captureAccounts
	var err error
	shard := ledger.ShardFor(shardKey, s.cfg.LedgerShards)
	if a.clearing, err = s.ledger.EnsureAccountCommitted(ctx, s.pool, ledger.ShardCode(ledger.ProcessorClearing(currency), shard), ledger.Asset, currency, nil); err != nil {
		return nil, err
	}
	if a.fees, err = s.ledger.EnsureAccountCommitted(ctx, s.pool, ledger.ShardCode(ledger.FeeRevenue(currency), shard), ledger.Revenue, currency, nil); err != nil {
		return nil, err
	}
	payable := ledger.ShardCode(ledger.MerchantPayable(merchantID, currency), ledger.ShardFor(shardKey, s.payableShards(merchantID, currency)))
	if a.payable, err = s.ledger.EnsureAccountCommitted(ctx, s.pool, payable, ledger.Liability, currency, &merchantID); err != nil {
		return nil, err
	}
	return &a, nil
}

// payableShards counts this posting towards the merchant's rate and returns
// how many shards its payable account should currently be spread over. The
// answer can change between a retry and the original attempt; that is safe
// because a business event is posted at most once (journal entries are unique
// per kind and reference), so only the row it locks differs, never the
// logical balance.
func (s *Service) payableShards(merchantID, currency string) int {
	if s.cfg.PayableShardRate <= 0 {
		return 1
	}
	rate := s.rates.Hit(merchantID + "|" + currency)
	return ledger.ShardsForRate(rate, s.cfg.PayableShardRate, s.cfg.PayableMaxShards)
}

func (s *Service) applyChargeTx(ctx context.Context, tx pgx.Tx, paymentID string, o processor.Outcome, perr error,
	keyID int64, accts *captureAccounts, source string) (*Payment, *Response, error) {

	// Lock order: idempotency key, payment, accounts (see idempotency.Lock).
	if keyID != 0 {
		if err := idempotency.Lock(ctx, tx, keyID); err != nil {
			return nil, nil, err
		}
	}
	var procID *string
	if o.ProcessorID != "" {
		procID = &o.ProcessorID
	}
	var row pgx.Row
	switch {
	case perr == nil && o.Result == processor.Succeeded:
		row = tx.QueryRow(ctx, `
			UPDATE payments SET status = 'succeeded', processor_id = coalesce($2, processor_id),
				fee = least(amount, (amount * $3 + 5000) / 10000 + $4),
				lease_until = NULL, next_attempt_at = NULL, last_error = NULL, resolved_by = $5, updated_at = now()
			WHERE id = $1 AND status = 'processing' RETURNING `+paymentCols,
			paymentID, procID, s.cfg.FeeBps, s.cfg.FeeFixed, source)
	case perr == nil && o.Result == processor.Declined:
		row = tx.QueryRow(ctx, `
			UPDATE payments SET status = 'failed', processor_id = coalesce($2, processor_id),
				failure_code = $3, failure_message = $4,
				lease_until = NULL, next_attempt_at = NULL, last_error = NULL, resolved_by = $5, updated_at = now()
			WHERE id = $1 AND status = 'processing' RETURNING `+paymentCols,
			paymentID, procID, o.Code, o.Message, source)
	default: // pending or unknown: schedule another attempt
		reason := "pending at processor"
		if perr != nil {
			reason = perr.Error()
		}
		row = tx.QueryRow(ctx, `
			UPDATE payments SET processor_id = coalesce($2, processor_id), attempts = attempts + 1,
				last_error = $3, lease_until = NULL,
				next_attempt_at = now() + make_interval(secs => least($5::float8, $4::float8 * power(2, least(attempts, 30))) * $6::float8),
				status = CASE WHEN attempts + 1 >= $7 THEN 'needs_review' ELSE status END,
				resolved_by = CASE WHEN attempts + 1 >= $7 THEN $8 ELSE resolved_by END,
				updated_at = now()
			WHERE id = $1 AND status = 'processing' RETURNING `+paymentCols,
			paymentID, procID, truncate(reason, 500), s.cfg.RetryBase.Seconds(), s.cfg.RetryMax.Seconds(),
			0.5+rand.Float64()/2, s.cfg.MaxAttempts, source)
	}
	p, err := scanPayment(row)
	transitioned := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		// Someone else (webhook, worker, concurrent retry) already moved it.
		p, err = lockPayment(ctx, tx, paymentID)
	}
	if err != nil {
		return nil, nil, err
	}

	if transitioned && p.Status != StatusProcessing {
		metrics.Transitions.WithLabelValues("payment", p.Status, source).Inc()
		if err := outbox.Enqueue(ctx, tx, "payment."+p.Status, p.ID, p); err != nil {
			return nil, nil, err
		}
	}

	var resp *Response
	if keyID != 0 {
		switch {
		case p.Terminal():
			resp = jsonResponse(http.StatusCreated, p)
			if err := idempotency.Finish(ctx, tx, keyID, resp.Status, resp.Body); err != nil {
				return nil, nil, err
			}
		case p.Status == StatusNeedsReview:
			// Not stored: an operator may still resolve it.
			resp = jsonResponse(http.StatusAccepted, p)
		default:
			resp = retryableResponse(http.StatusServiceUnavailable, "processor_unavailable",
				"the payment outcome is not yet known; retry with the same Idempotency-Key", time.Second)
		}
	}

	// Ledger last: it locks the hot system accounts, so keep that window
	// as close to COMMIT as possible.
	if transitioned && p.Status == StatusSucceeded {
		if accts == nil {
			return nil, nil, errors.New("payments: capture without accounts")
		}
		if err := postCapture(ctx, tx, p, accts); err != nil {
			return nil, nil, err
		}
	}
	return p, resp, nil
}

func postCapture(ctx context.Context, tx pgx.Tx, p *Payment, a *captureAccounts) error {
	fee := *p.Fee
	entries := []ledger.Entry{{AccountID: a.clearing, Amount: p.Amount}}
	if net := p.Amount - fee; net > 0 {
		entries = append(entries, ledger.Entry{AccountID: a.payable, Amount: -net})
	}
	if fee > 0 {
		entries = append(entries, ledger.Entry{AccountID: a.fees, Amount: -fee})
	}
	_, created, err := ledger.Post(ctx, tx, ledger.Journal{
		Kind: ledger.KindPaymentCapture, Reference: p.ID, Currency: p.Currency,
		Description: "capture " + p.ID, Entries: entries,
	})
	if created {
		metrics.JournalEntries.WithLabelValues(ledger.KindPaymentCapture).Inc()
	}
	return err
}

// finishPaymentKey stores the response for a key whose payment was
// completed by another path while the client was away.
func (s *Service) finishPaymentKey(ctx context.Context, paymentID string, keyID int64) (*Response, error) {
	var resp *Response
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := idempotency.Lock(ctx, tx, keyID); err != nil { // before the payment row
			return err
		}
		p, err := lockPayment(ctx, tx, paymentID)
		if err != nil {
			return err
		}
		if !p.Terminal() {
			resp = jsonResponse(http.StatusAccepted, p)
			return nil
		}
		resp = jsonResponse(http.StatusCreated, p)
		return idempotency.Finish(ctx, tx, keyID, resp.Status, resp.Body)
	})
	return resp, err
}

// createPaymentUnsafe is the benchmark baseline: no client idempotency key,
// no processor idempotency key, and an unknown outcome is assumed to be a
// failure. This is how a naive integration behaves; do not use it.
func (s *Service) createPaymentUnsafe(ctx context.Context, req CreatePaymentRequest) (*Response, error) {
	var p *Payment
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM merchants WHERE id = $1)`, req.MerchantID).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return errMerchantNotFound
		}
		var err error
		p, err = s.insertPayment(ctx, tx, req, false)
		return err
	})
	if errors.Is(err, errMerchantNotFound) {
		return errorResponse(404, "invalid_request_error", "merchant_not_found", "no such merchant"), nil
	}
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.cfg.RequestBudget)
	defer cancel()
	o, perr := s.charge(ctx, p)
	unknown := perr != nil
	if unknown {
		o, perr = processor.Outcome{Result: processor.Declined, Code: "processor_error", Message: perr.Error()}, nil
	}
	p, _, err = s.applyCharge(ctx, p, o, perr, 0, "request")
	if err != nil {
		return nil, err
	}
	if unknown {
		return retryableResponse(http.StatusBadGateway, "processor_error", "payment processor error", 0), nil
	}
	return jsonResponse(http.StatusCreated, p), nil
}

var errMerchantNotFound = errors.New("merchant not found")

func (s *Service) GetPayment(ctx context.Context, paymentID string) (*Response, error) {
	p, err := scanPayment(s.pool.QueryRow(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1`, paymentID))
	if errors.Is(err, pgx.ErrNoRows) {
		return notFound("payment"), nil
	}
	if err != nil {
		return nil, err
	}
	return jsonResponse(200, p), nil
}

// PaymentLedger returns all postings for a payment and its refunds.
func (s *Service) PaymentLedger(ctx context.Context, paymentID string) (*Response, error) {
	refs := []string{paymentID}
	rows, err := s.pool.Query(ctx, `SELECT id FROM refunds WHERE payment_id = $1`, paymentID)
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	postings, err := ledger.ReferencePostings(ctx, s.pool, append(refs, ids...))
	if err != nil {
		return nil, err
	}
	return jsonResponse(200, map[string]any{"object": "list", "data": postings}), nil
}

// --------------------------------------------------------------- helpers

func replay(k *idempotency.Key) *Response {
	return &Response{Status: *k.ResponseCode, Body: k.ResponseBody, Replayed: true}
}

func keyMismatch() *Response {
	return errorResponse(http.StatusUnprocessableEntity, "idempotency_error", "idempotency_key_reused",
		"this Idempotency-Key was already used with different request parameters")
}

func inProgress() *Response {
	return retryableResponse(http.StatusConflict, "request_in_progress",
		"a request with this Idempotency-Key is in progress; retry later", time.Second)
}

func notFound(what string) *Response {
	return errorResponse(http.StatusNotFound, "invalid_request_error", "resource_missing", "no such "+what)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
