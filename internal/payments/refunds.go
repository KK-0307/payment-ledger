package payments

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"paymentledger/internal/db"
	"paymentledger/internal/id"
	"paymentledger/internal/idempotency"
	"paymentledger/internal/ledger"
	"paymentledger/internal/metrics"
	"paymentledger/internal/outbox"
	"paymentledger/internal/processor"
)

// Refunds follow the same three phases as payments. Creating a refund
// reserves its amount on the payment (payments.amount_refunded, guarded by a
// CHECK constraint) under the payment's row lock, so concurrent refunds can
// never exceed the captured amount. A failed refund releases the
// reservation.

const refundCols = `id, payment_id, amount, currency, status, processor_id, processor_idempotency_key,
	failure_code, failure_message, attempts, coalesce(greatest(extract(epoch FROM lease_until - now()), 0), 0)::float8, created_at, updated_at`

func scanRefund(row pgx.Row) (*Refund, error) {
	var r Refund
	var leaseLeft float64
	err := row.Scan(&r.ID, &r.PaymentID, &r.Amount, &r.Currency, &r.Status, &r.ProcessorID, &r.ProcessorKey,
		&r.FailureCode, &r.FailureMessage, &r.Attempts, &leaseLeft, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return nil, err
	}
	r.LeaseLeft = time.Duration(leaseLeft * float64(time.Second))
	r.Object = "refund"
	return &r, nil
}

func lockRefund(ctx context.Context, tx pgx.Tx, refundID string) (*Refund, error) {
	return scanRefund(tx.QueryRow(ctx, `SELECT `+refundCols+` FROM refunds WHERE id = $1 FOR UPDATE`, refundID))
}

func (s *Service) CreateRefund(ctx context.Context, scope, key, paymentID string, req CreateRefundRequest) (*Response, error) {
	if err := idempotency.ValidateKey(key); err != nil {
		return errorResponse(400, "invalid_request_error", "idempotency_key_required", err.Error()), nil
	}
	if req.Amount < 0 {
		return errorResponse(400, "invalid_request_error", "amount_invalid", "amount must be positive"), nil
	}
	path := "/v1/payments/" + paymentID + "/refunds"
	var (
		resp     *Response
		r        *Refund
		p        *Payment
		keyID    int64
		inflight bool
	)
	phase1 := func(tx pgx.Tx) error {
		resp, r, p, inflight = nil, nil, nil, false
		k, err := idempotency.Acquire(ctx, tx, scope, key, "POST", path, idempotency.HashRequest(req))
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
		finish := func(rsp *Response) error {
			resp = rsp
			return idempotency.Finish(ctx, tx, k.ID, rsp.Status, rsp.Body)
		}
		if k.RecoveryPoint == idempotency.PointStarted {
			p, err = lockPayment(ctx, tx, paymentID)
			if errors.Is(err, pgx.ErrNoRows) {
				return finish(notFound("payment"))
			}
			if err != nil {
				return err
			}
			if p.Status != StatusSucceeded || p.ProcessorID == nil {
				return finish(errorResponse(400, "invalid_request_error", "payment_not_refundable",
					"only succeeded payments can be refunded (status is "+p.Status+")"))
			}
			available := p.Amount - p.AmountRefunded
			amount := req.Amount
			if amount == 0 {
				amount = available
			}
			if amount <= 0 || amount > available {
				return finish(errorResponse(400, "invalid_request_error", "amount_too_large",
					fmt.Sprintf("refund of %d exceeds refundable amount %d", amount, available)))
			}
			rid := id.New("re")
			r, err = scanRefund(tx.QueryRow(ctx, `
				INSERT INTO refunds (id, payment_id, amount, currency, status, processor_idempotency_key, lease_until, next_attempt_at)
				VALUES ($1, $2, $3, $4, 'processing', $5, now() + make_interval(secs => $6), now() + make_interval(secs => $6))
				RETURNING `+refundCols, rid, p.ID, amount, p.Currency, "pl-ref-"+rid, s.cfg.Lease.Seconds()))
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE payments SET amount_refunded = amount_refunded + $2, updated_at = now() WHERE id = $1`, p.ID, amount); err != nil {
				return err
			}
			return idempotency.SetRecoveryPoint(ctx, tx, k.ID, PointRefundCreated, r.ID)
		}
		if k.ResourceID == nil {
			return fmt.Errorf("idempotency key %d at %s without resource", k.ID, k.RecoveryPoint)
		}
		if r, err = lockRefund(ctx, tx, *k.ResourceID); err != nil {
			return err
		}
		if p, err = scanPayment(tx.QueryRow(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1`, r.PaymentID)); err != nil {
			return err
		}
		if r.Status != StatusProcessing {
			return nil
		}
		if r.LeaseLeft > 0 {
			inflight = true
			return nil
		}
		_, err = tx.Exec(ctx, `UPDATE refunds SET lease_until = now() + make_interval(secs => $2) WHERE id = $1`, r.ID, s.cfg.Lease.Seconds())
		return err
	}
	deadline := time.Now().Add(s.cfg.InflightWait)
	for {
		if err := db.WithTx(ctx, s.pool, phase1); err != nil {
			return nil, err
		}
		if !inflight {
			break
		}
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
	if r.Status != StatusProcessing {
		return s.finishRefundKey(ctx, r.ID, keyID)
	}
	stop := s.holdLease("refunds", r.ID)
	o, perr := s.resolveRefund(ctx, r, p)
	stop()
	_, resp, err := s.applyRefund(ctx, r, p, o, perr, keyID, "request")
	return resp, err
}

func (s *Service) resolveRefund(ctx context.Context, r *Refund, p *Payment) (processor.Outcome, error) {
	procPay := ""
	if p.ProcessorID != nil {
		procPay = *p.ProcessorID
	}
	lookup := func(procRefundID string) (processor.Outcome, error) {
		o, found, err := s.proc.LookupRefund(ctx, r.ID, procPay, procRefundID)
		switch {
		case err != nil:
			metrics.ProcessorCalls.WithLabelValues("lookup_refund", "unknown").Inc()
			return o, &processor.UnknownError{Reason: "lookup", Err: err}
		case !found:
			metrics.ProcessorCalls.WithLabelValues("lookup_refund", "not_found").Inc()
			return o, &processor.UnknownError{Reason: "not_found", Err: errors.New("processor has no record of refund " + r.ID)}
		}
		metrics.ProcessorCalls.WithLabelValues("lookup_refund", string(o.Result)).Inc()
		return o, nil
	}
	if r.ProcessorID != nil {
		return lookup(*r.ProcessorID)
	}
	if time.Since(r.CreatedAt) > s.cfg.KeyMaxAge {
		return lookup("")
	}
	req := processor.RefundRequest{RefundID: r.ID, PaymentID: r.PaymentID, ProcessorPayID: procPay, Amount: r.Amount, Metadata: p.Metadata}
	if r.ProcessorKey != nil {
		req.IdempotencyKey = *r.ProcessorKey
	}
	var (
		o   processor.Outcome
		err error
	)
	for attempt := 0; attempt <= s.cfg.InlineRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(s.cfg.InlineDelay * time.Duration(attempt))
		}
		start := time.Now()
		o, err = s.proc.Refund(ctx, req)
		metrics.ProcessorDuration.WithLabelValues("refund").Observe(time.Since(start).Seconds())
		if err == nil {
			metrics.ProcessorCalls.WithLabelValues("refund", string(o.Result)).Inc()
			return o, nil
		}
		metrics.ProcessorCalls.WithLabelValues("refund", "unknown").Inc()
	}
	if r.Attempts >= s.cfg.LookupAfter {
		if lo, lerr := lookup(""); lerr == nil {
			return lo, nil
		}
	}
	return o, err
}

func (s *Service) refundAccounts(ctx context.Context, p *Payment, refundID string) (*captureAccounts, error) {
	return s.captureAccounts(ctx, p.MerchantID, p.Currency, refundID)
}

func (s *Service) applyRefund(ctx context.Context, r *Refund, p *Payment, o processor.Outcome, perr error, keyID int64, source string) (*Refund, *Response, error) {
	var accts *captureAccounts
	if perr == nil && o.Result == processor.Succeeded {
		var err error
		if accts, err = s.refundAccounts(ctx, p, r.ID); err != nil {
			return nil, nil, err
		}
	}
	var (
		out  *Refund
		resp *Response
	)
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) (err error) {
		out, resp, err = s.applyRefundTx(ctx, tx, r.ID, p.MerchantID, o, perr, keyID, accts, source)
		return err
	})
	return out, resp, err
}

func (s *Service) applyRefundTx(ctx context.Context, tx pgx.Tx, refundID, merchantID string, o processor.Outcome, perr error,
	keyID int64, accts *captureAccounts, source string) (*Refund, *Response, error) {

	// Lock order: idempotency key, refund and payment, accounts (see idempotency.Lock).
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
			UPDATE refunds SET status = 'succeeded', processor_id = coalesce($2, processor_id),
				lease_until = NULL, next_attempt_at = NULL, last_error = NULL, resolved_by = $3, updated_at = now()
			WHERE id = $1 AND status = 'processing' RETURNING `+refundCols, refundID, procID, source)
	case perr == nil && o.Result == processor.Declined:
		row = tx.QueryRow(ctx, `
			UPDATE refunds SET status = 'failed', processor_id = coalesce($2, processor_id),
				failure_code = $3, failure_message = $4,
				lease_until = NULL, next_attempt_at = NULL, last_error = NULL, resolved_by = $5, updated_at = now()
			WHERE id = $1 AND status = 'processing' RETURNING `+refundCols, refundID, procID, o.Code, o.Message, source)
	default:
		reason := "pending at processor"
		if perr != nil {
			reason = perr.Error()
		}
		row = tx.QueryRow(ctx, `
			UPDATE refunds SET processor_id = coalesce($2, processor_id), attempts = attempts + 1,
				last_error = $3, lease_until = NULL,
				next_attempt_at = now() + make_interval(secs => least($5::float8, $4::float8 * power(2, least(attempts, 30))) * $6::float8),
				status = CASE WHEN attempts + 1 >= $7 THEN 'needs_review' ELSE status END,
				resolved_by = CASE WHEN attempts + 1 >= $7 THEN $8 ELSE resolved_by END,
				updated_at = now()
			WHERE id = $1 AND status = 'processing' RETURNING `+refundCols,
			refundID, procID, truncate(reason, 500), s.cfg.RetryBase.Seconds(), s.cfg.RetryMax.Seconds(),
			0.5+rand.Float64()/2, s.cfg.MaxAttempts, source)
	}
	r, err := scanRefund(row)
	transitioned := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		r, err = lockRefund(ctx, tx, refundID)
	}
	if err != nil {
		return nil, nil, err
	}

	if transitioned && r.Status != StatusProcessing {
		metrics.Transitions.WithLabelValues("refund", r.Status, source).Inc()
		if r.Status == StatusFailed {
			if _, err := tx.Exec(ctx, `UPDATE payments SET amount_refunded = amount_refunded - $2, updated_at = now() WHERE id = $1`, r.PaymentID, r.Amount); err != nil {
				return nil, nil, err
			}
		}
		if err := outbox.Enqueue(ctx, tx, "refund."+r.Status, r.ID, r); err != nil {
			return nil, nil, err
		}
	}

	var resp *Response
	if keyID != 0 {
		switch {
		case r.Terminal():
			resp = jsonResponse(http.StatusCreated, r)
			if err := idempotency.Finish(ctx, tx, keyID, resp.Status, resp.Body); err != nil {
				return nil, nil, err
			}
		case r.Status == StatusNeedsReview:
			resp = jsonResponse(http.StatusAccepted, r)
		default:
			resp = retryableResponse(http.StatusServiceUnavailable, "processor_unavailable",
				"the refund outcome is not yet known; retry with the same Idempotency-Key", time.Second)
		}
	}

	if transitioned && r.Status == StatusSucceeded {
		if accts == nil {
			return nil, nil, errors.New("payments: refund without accounts")
		}
		_, created, err := ledger.Post(ctx, tx, ledger.Journal{
			Kind: ledger.KindRefund, Reference: r.ID, Currency: r.Currency,
			Description: "refund " + r.ID + " of " + r.PaymentID,
			Entries: []ledger.Entry{
				{AccountID: accts.payable, Amount: r.Amount},
				{AccountID: accts.clearing, Amount: -r.Amount},
			},
		})
		if err != nil {
			return nil, nil, err
		}
		if created {
			metrics.JournalEntries.WithLabelValues(ledger.KindRefund).Inc()
		}
	}
	return r, resp, nil
}

func (s *Service) finishRefundKey(ctx context.Context, refundID string, keyID int64) (*Response, error) {
	var resp *Response
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := idempotency.Lock(ctx, tx, keyID); err != nil { // before the refund row
			return err
		}
		r, err := lockRefund(ctx, tx, refundID)
		if err != nil {
			return err
		}
		if !r.Terminal() {
			resp = jsonResponse(http.StatusAccepted, r)
			return nil
		}
		resp = jsonResponse(http.StatusCreated, r)
		return idempotency.Finish(ctx, tx, keyID, resp.Status, resp.Body)
	})
	return resp, err
}

func (s *Service) GetRefund(ctx context.Context, refundID string) (*Response, error) {
	r, err := scanRefund(s.pool.QueryRow(ctx, `SELECT `+refundCols+` FROM refunds WHERE id = $1`, refundID))
	if errors.Is(err, pgx.ErrNoRows) {
		return notFound("refund"), nil
	}
	if err != nil {
		return nil, err
	}
	return jsonResponse(200, r), nil
}
