// Package audit verifies ledger and payment invariants directly against the
// database. It is used by the /v1/admin/audit endpoint and by the benchmark
// oracle after every run.
package audit

import (
	"context"
	"fmt"

	"paymentledger/internal/db"
)

type Check struct {
	Name       string `json:"name"`
	Passed     bool   `json:"passed"`
	Violations int64  `json:"violations"`
	Detail     string `json:"detail,omitempty"`
}

type Report struct {
	Passed bool             `json:"passed"`
	Checks []Check          `json:"checks"`
	Counts map[string]int64 `json:"counts"`
}

// Each check is a query returning the number of violating rows.
var checks = []struct{ name, detail, sql string }{
	{"journal_entries_balanced", "every journal entry's postings sum to zero and it has >= 2 postings", `
		SELECT count(*) FROM (
			SELECT j.id FROM journal_entries j LEFT JOIN postings p ON p.journal_entry_id = j.id
			GROUP BY j.id HAVING count(p.id) < 2 OR coalesce(sum(p.amount), 0) <> 0) x`},
	{"account_balances_match_postings", "cached account balance equals the sum of its postings (every shard row)", `
		SELECT count(*) FROM accounts a
		LEFT JOIN (SELECT account_id, sum(amount) s FROM postings GROUP BY account_id) p ON p.account_id = a.id
		WHERE a.balance <> coalesce(p.s, 0)`},
	{"logical_balances_match_postings", "a sharded account's balance (sum of its shards) equals the sum of postings to all its shards", `
		SELECT count(*) FROM (
			SELECT a.base_code FROM accounts a
			LEFT JOIN (SELECT account_id, sum(amount) s FROM postings GROUP BY account_id) p ON p.account_id = a.id
			GROUP BY a.base_code HAVING sum(a.balance) <> coalesce(sum(p.s), 0)) x`},
	{"account_shards_consistent", "all shards of one logical account share type, currency and merchant, and shard suffixes are numeric", `
		SELECT (SELECT count(*) FROM (
			SELECT base_code FROM accounts GROUP BY base_code
			HAVING count(DISTINCT type) > 1 OR count(DISTINCT currency) > 1 OR count(DISTINCT coalesce(merchant_id, '')) > 1) x)
		     + (SELECT count(*) FROM accounts WHERE code <> base_code AND code !~ '^[^#]+#[1-9][0-9]*$')`},
	{"ledger_sums_to_zero_per_currency", "sum of all balances is zero in every currency", `
		SELECT count(*) FROM (SELECT currency FROM accounts GROUP BY currency HAVING sum(balance) <> 0) x`},
	{"posting_currency_matches_account", "postings only touch accounts in the journal's currency", `
		SELECT count(*) FROM postings p JOIN accounts a ON a.id = p.account_id JOIN journal_entries j ON j.id = p.journal_entry_id
		WHERE a.currency <> j.currency`},
	{"succeeded_payments_have_one_capture", "each succeeded payment has exactly one capture entry debiting clearing by its amount", `
		SELECT count(*) FROM payments pay
		LEFT JOIN journal_entries j ON j.kind = 'payment_capture' AND j.reference = pay.id
		LEFT JOIN postings p ON p.journal_entry_id = j.id AND p.amount > 0
		WHERE pay.status = 'succeeded' AND (j.id IS NULL OR p.amount IS DISTINCT FROM pay.amount)`},
	{"no_capture_without_success", "no capture entry exists for a payment that is not succeeded", `
		SELECT count(*) FROM journal_entries j LEFT JOIN payments pay ON pay.id = j.reference
		WHERE j.kind = 'payment_capture' AND (pay.id IS NULL OR pay.status <> 'succeeded')`},
	{"succeeded_refunds_have_one_entry", "each succeeded refund has exactly one refund entry; others have none", `
		SELECT count(*) FROM refunds r LEFT JOIN journal_entries j ON j.kind = 'refund' AND j.reference = r.id
		WHERE (r.status = 'succeeded') <> (j.id IS NOT NULL)`},
	{"refund_reservations_consistent", "payments.amount_refunded equals succeeded + in-flight refunds", `
		SELECT count(*) FROM payments pay
		LEFT JOIN (SELECT payment_id, sum(amount) s FROM refunds WHERE status IN ('succeeded', 'processing', 'needs_review') GROUP BY payment_id) r
		  ON r.payment_id = pay.id
		WHERE pay.amount_refunded <> coalesce(r.s, 0)`},
	{"fees_within_amount", "fee is set exactly for succeeded payments and 0 <= fee <= amount", `
		SELECT count(*) FROM payments
		WHERE (status = 'succeeded') <> (fee IS NOT NULL) OR fee < 0 OR fee > amount`},
	{"terminal_events_recorded", "each succeeded/failed payment has its outbox event", `
		SELECT count(*) FROM payments pay LEFT JOIN outbox_events e ON e.aggregate_id = pay.id AND e.type = 'payment.' || pay.status
		WHERE pay.status IN ('succeeded', 'failed') AND e.id IS NULL`},
}

var counts = map[string]string{
	"payments_total":        `SELECT count(*) FROM payments`,
	"payments_succeeded":    `SELECT count(*) FROM payments WHERE status = 'succeeded'`,
	"payments_failed":       `SELECT count(*) FROM payments WHERE status = 'failed'`,
	"payments_processing":   `SELECT count(*) FROM payments WHERE status = 'processing'`,
	"payments_needs_review": `SELECT count(*) FROM payments WHERE status = 'needs_review'`,
	"refunds_total":         `SELECT count(*) FROM refunds`,
	"refunds_succeeded":     `SELECT count(*) FROM refunds WHERE status = 'succeeded'`,
	"refunds_processing":    `SELECT count(*) FROM refunds WHERE status = 'processing'`,
	"refunds_needs_review":  `SELECT count(*) FROM refunds WHERE status = 'needs_review'`,
	"journal_entries":       `SELECT count(*) FROM journal_entries`,
	"postings":              `SELECT count(*) FROM postings`,
	"outbox_events":         `SELECT count(*) FROM outbox_events`,
	"outbox_undelivered":    `SELECT count(*) FROM outbox_events WHERE delivered_at IS NULL`,
	"processor_events":      `SELECT count(*) FROM processor_events`,
	"captured_amount":       `SELECT coalesce(sum(amount), 0) FROM payments WHERE status = 'succeeded'`,
	"refunded_amount":       `SELECT coalesce(sum(amount), 0) FROM refunds WHERE status = 'succeeded'`,
	"fee_amount":            `SELECT coalesce(sum(fee), 0) FROM payments WHERE status = 'succeeded'`,
}

func Run(ctx context.Context, q db.Querier) (*Report, error) {
	rep := &Report{Passed: true, Counts: map[string]int64{}}
	for _, c := range checks {
		var n int64
		if err := q.QueryRow(ctx, c.sql).Scan(&n); err != nil {
			return nil, fmt.Errorf("audit %s: %w", c.name, err)
		}
		ch := Check{Name: c.name, Passed: n == 0, Violations: n, Detail: c.detail}
		rep.Passed = rep.Passed && ch.Passed
		rep.Checks = append(rep.Checks, ch)
	}
	for name, sql := range counts {
		var n int64
		if err := q.QueryRow(ctx, sql).Scan(&n); err != nil {
			return nil, fmt.Errorf("audit count %s: %w", name, err)
		}
		rep.Counts[name] = n
	}
	return rep, nil
}

// ClearingBalances returns the processor_clearing balance per currency (the
// sum over all of the account's shards), which must equal what the processor
// says it captured minus refunded.
func ClearingBalances(ctx context.Context, q db.Querier) (map[string]int64, error) {
	rows, err := q.Query(ctx, `
		SELECT currency, sum(balance)::bigint FROM accounts WHERE base_code LIKE 'processor_clearing:%' GROUP BY currency`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var cur string
		var bal int64
		if err := rows.Scan(&cur, &bal); err != nil {
			return nil, err
		}
		out[cur] = bal
	}
	return out, rows.Err()
}
