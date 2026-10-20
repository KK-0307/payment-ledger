// Package outbox implements the transactional outbox: domain events are
// inserted in the same transaction as the state change that caused them,
// then delivered at-least-once to a webhook endpoint by Dispatcher.
// Receivers deduplicate on the event id.
package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"paymentledger/internal/id"
	"paymentledger/internal/metrics"
	"paymentledger/internal/stripe"
)

// Event is the delivered envelope.
type Event struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Created int64           `json:"created"`
	Data    json.RawMessage `json:"data"`
}

// Enqueue records an event inside tx. (type, aggregateID) is unique, so an
// object can emit each event type at most once, even if two code paths race
// to record the same transition.
func Enqueue(ctx context.Context, tx pgx.Tx, typ, aggregateID string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (event_id, type, aggregate_id, payload) VALUES ($1, $2, $3, $4)
		ON CONFLICT (type, aggregate_id) DO NOTHING`, id.New("evt"), typ, aggregateID, raw)
	return err
}

type Dispatcher struct {
	Pool        *pgxpool.Pool
	URL         string // empty disables delivery
	Secret      string // HMAC secret; signature uses the Stripe scheme
	HTTP        *http.Client
	Batch       int
	Concurrency int
	Interval    time.Duration
	ClaimFor    time.Duration // how long a claimed event is invisible to others
	Log         *slog.Logger
}

// Run polls until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	if d.URL == "" {
		d.Log.Info("outbox dispatcher disabled (no OUTBOX_WEBHOOK_URL)")
		return
	}
	t := time.NewTicker(d.Interval)
	defer t.Stop()
	for {
		n, err := d.DispatchOnce(ctx)
		if err != nil && ctx.Err() == nil {
			d.Log.Warn("outbox dispatch", "err", err)
		}
		if n == d.Batch {
			continue // backlog: go again immediately
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

type claimed struct {
	id       int64
	eventID  string
	typ      string
	payload  []byte
	created  time.Time
	attempts int
}

// DispatchOnce claims a batch and delivers it; returns the batch size.
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	rows, err := d.Pool.Query(ctx, `
		UPDATE outbox_events SET next_attempt_at = now() + make_interval(secs => $2), attempts = attempts + 1
		WHERE id IN (
			SELECT id FROM outbox_events
			WHERE delivered_at IS NULL AND next_attempt_at <= now()
			ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED)
		RETURNING id, event_id, type, payload, created_at, attempts`, d.Batch, d.ClaimFor.Seconds())
	if err != nil {
		return 0, err
	}
	batch, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (claimed, error) {
		var c claimed
		err := r.Scan(&c.id, &c.eventID, &c.typ, &c.payload, &c.created, &c.attempts)
		return c, err
	})
	if err != nil {
		return 0, err
	}
	sem := make(chan struct{}, max(1, d.Concurrency))
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		delivered []int64
	)
	for _, c := range batch {
		wg.Add(1)
		sem <- struct{}{}
		go func(c claimed) {
			defer wg.Done()
			defer func() { <-sem }()
			if d.deliver(ctx, c) {
				mu.Lock()
				delivered = append(delivered, c.id)
				mu.Unlock()
			}
		}(c)
	}
	wg.Wait()
	if len(delivered) > 0 {
		// One commit per batch instead of one per event.
		if _, err := d.Pool.Exec(ctx, `UPDATE outbox_events SET delivered_at = now(), last_error = NULL WHERE id = ANY($1)`, delivered); err != nil {
			// The receivers got them; they will be delivered again after the
			// claim expires. Receivers must deduplicate on event id.
			d.Log.Warn("outbox mark delivered", "count", len(delivered), "err", err)
		}
	}
	return len(batch), nil
}

// deliver posts one event; on failure it reschedules it with backoff.
func (d *Dispatcher) deliver(ctx context.Context, c claimed) bool {
	body, _ := json.Marshal(Event{ID: c.eventID, Type: c.typ, Created: c.created.Unix(), Data: c.payload})
	err := d.post(ctx, body)
	if err == nil {
		metrics.OutboxDeliveries.WithLabelValues("delivered").Inc()
		return true
	}
	metrics.OutboxDeliveries.WithLabelValues("failed").Inc()
	backoff := min(time.Duration(1<<min(c.attempts, 10))*250*time.Millisecond, 5*time.Minute)
	backoff = backoff/2 + time.Duration(rand.Int64N(int64(backoff/2)+1))
	if _, uerr := d.Pool.Exec(ctx, `
		UPDATE outbox_events SET next_attempt_at = now() + make_interval(secs => $2), last_error = $3 WHERE id = $1`,
		c.id, backoff.Seconds(), err.Error()); uerr != nil {
		d.Log.Warn("outbox reschedule", "event", c.eventID, "err", uerr)
	}
	return false
}

func (d *Dispatcher) post(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if d.Secret != "" {
		req.Header.Set("Ledger-Signature", stripe.SignatureHeader(body, d.Secret, time.Now()))
	}
	resp, err := d.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) //nolint:errcheck
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("receiver returned %d", resp.StatusCode)
	}
	return nil
}
