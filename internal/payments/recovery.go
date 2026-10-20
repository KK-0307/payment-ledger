package payments

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"paymentledger/internal/db"
	"paymentledger/internal/idempotency"
	"paymentledger/internal/metrics"
	"paymentledger/internal/processor"
	"paymentledger/internal/stripe"
)

// RecoveryWorker drives payments and refunds left in 'processing' (unknown
// processor outcome, crashed request, client gave up) to a final state. Any
// number of workers may run across instances: items are claimed with a
// lease using FOR UPDATE SKIP LOCKED.
type RecoveryWorker struct {
	Svc         *Service
	Interval    time.Duration
	Batch       int
	Concurrency int
	KeyMaxAge   time.Duration // idempotency key retention
}

func (w *RecoveryWorker) Run(ctx context.Context) {
	if w.Svc.cfg.Unsafe {
		w.Svc.log.Warn("recovery worker disabled in unsafe mode")
		return
	}
	t := time.NewTicker(w.Interval)
	defer t.Stop()
	lastReap := time.Time{}
	for {
		n, err := w.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			w.Svc.log.Warn("recovery", "err", err)
		}
		if time.Since(lastReap) > time.Minute {
			lastReap = time.Now()
			if deleted, err := idempotency.Reap(ctx, w.Svc.pool, int(w.KeyMaxAge.Seconds())); err != nil && ctx.Err() == nil {
				w.Svc.log.Warn("reap idempotency keys", "err", err)
			} else if deleted > 0 {
				w.Svc.log.Info("reaped idempotency keys", "count", deleted)
			}
		}
		if n >= w.Batch {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// RunOnce claims and processes one batch of payments and one of refunds.
func (w *RecoveryWorker) RunOnce(ctx context.Context) (int, error) {
	s := w.Svc
	rows, err := s.pool.Query(ctx, `
		UPDATE payments SET lease_until = now() + make_interval(secs => $2)
		WHERE id IN (
			SELECT id FROM payments
			WHERE status = 'processing' AND next_attempt_at <= now()
			  AND (lease_until IS NULL OR lease_until < now())
			ORDER BY next_attempt_at LIMIT $1 FOR UPDATE SKIP LOCKED)
		RETURNING `+paymentCols, w.Batch, s.cfg.Lease.Seconds())
	if err != nil {
		return 0, err
	}
	pays, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (*Payment, error) { return scanPayment(r) })
	if err != nil {
		return 0, err
	}

	rows, err = s.pool.Query(ctx, `
		UPDATE refunds SET lease_until = now() + make_interval(secs => $2)
		WHERE id IN (
			SELECT id FROM refunds
			WHERE status = 'processing' AND next_attempt_at <= now()
			  AND (lease_until IS NULL OR lease_until < now())
			ORDER BY next_attempt_at LIMIT $1 FOR UPDATE SKIP LOCKED)
		RETURNING `+refundCols, w.Batch, s.cfg.Lease.Seconds())
	if err != nil {
		return len(pays), err
	}
	refs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (*Refund, error) { return scanRefund(r) })
	if err != nil {
		return len(pays), err
	}

	sem := make(chan struct{}, max(1, w.Concurrency))
	var wg sync.WaitGroup
	run := func(fn func()) {
		wg.Add(1)
		sem <- struct{}{}
		go func() { defer wg.Done(); defer func() { <-sem }(); fn() }()
	}
	for _, p := range pays {
		metrics.RecoveryRuns.WithLabelValues("payment").Inc()
		run(func() {
			stop := s.holdLease("payments", p.ID)
			o, perr := s.resolveCharge(ctx, p)
			stop()
			if _, _, err := s.applyCharge(ctx, p, o, perr, 0, "recovery"); err != nil && ctx.Err() == nil {
				s.log.Warn("recover payment", "payment", p.ID, "err", err)
			}
		})
	}
	for _, r := range refs {
		metrics.RecoveryRuns.WithLabelValues("refund").Inc()
		run(func() {
			p, err := scanPayment(s.pool.QueryRow(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1`, r.PaymentID))
			if err != nil {
				s.log.Warn("recover refund: load payment", "refund", r.ID, "err", err)
				return
			}
			stop := s.holdLease("refunds", r.ID)
			o, perr := s.resolveRefund(ctx, r, p)
			stop()
			if _, _, err := s.applyRefund(ctx, r, p, o, perr, 0, "recovery"); err != nil && ctx.Err() == nil {
				s.log.Warn("recover refund", "refund", r.ID, "err", err)
			}
		})
	}
	wg.Wait()
	return len(pays) + len(refs), nil
}

// HandleProcessorEvent applies a verified Stripe webhook event. Events are
// deduplicated by id; the transition itself is the same guarded update used
// by requests and recovery, so a webhook racing either is harmless.
func (s *Service) HandleProcessorEvent(ctx context.Context, ev *stripe.Event) error {
	var apply func(tx pgx.Tx) error

	switch ev.Type {
	case "payment_intent.succeeded", "payment_intent.payment_failed", "payment_intent.canceled":
		var pi stripe.PaymentIntent
		if err := json.Unmarshal(ev.Data.Object, &pi); err != nil {
			return err
		}
		o := processor.PaymentIntentOutcome(&pi)
		paymentID := pi.Metadata["payment_id"]
		if paymentID != "" && o.Result != processor.Pending {
			p, err := scanPayment(s.pool.QueryRow(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1`, paymentID))
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			if p != nil && p.Status == StatusProcessing {
				var accts *captureAccounts
				if o.Result == processor.Succeeded {
					if accts, err = s.captureAccounts(ctx, p.MerchantID, p.Currency, p.ID); err != nil {
						return err
					}
				}
				apply = func(tx pgx.Tx) error {
					_, _, err := s.applyChargeTx(ctx, tx, p.ID, o, nil, 0, accts, "webhook")
					return err
				}
			}
		}
	case "refund.created", "refund.updated", "refund.failed", "charge.refund.updated":
		var rf stripe.Refund
		if err := json.Unmarshal(ev.Data.Object, &rf); err != nil {
			return err
		}
		o := processor.RefundOutcome(&rf)
		refundID := rf.Metadata["refund_id"]
		if refundID != "" && o.Result != processor.Pending {
			r, err := scanRefund(s.pool.QueryRow(ctx, `SELECT `+refundCols+` FROM refunds WHERE id = $1`, refundID))
			if err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			if r != nil && r.Status == StatusProcessing {
				p, err := scanPayment(s.pool.QueryRow(ctx, `SELECT `+paymentCols+` FROM payments WHERE id = $1`, r.PaymentID))
				if err != nil {
					return err
				}
				var accts *captureAccounts
				if o.Result == processor.Succeeded {
					if accts, err = s.refundAccounts(ctx, p, r.ID); err != nil {
						return err
					}
				}
				apply = func(tx pgx.Tx) error {
					_, _, err := s.applyRefundTx(ctx, tx, r.ID, p.MerchantID, o, nil, 0, accts, "webhook")
					return err
				}
			}
		}
	}

	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO processor_events (id, type, payload) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING`,
			ev.ID, ev.Type, ev.Data.Object)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			metrics.WebhooksReceived.WithLabelValues("duplicate").Inc()
			return nil
		}
		if apply != nil {
			if err := apply(tx); err != nil {
				return err
			}
			metrics.WebhooksReceived.WithLabelValues("applied").Inc()
		} else {
			metrics.WebhooksReceived.WithLabelValues("ignored").Inc()
		}
		_, err = tx.Exec(ctx, `UPDATE processor_events SET processed_at = now() WHERE id = $1`, ev.ID)
		return err
	})
}
