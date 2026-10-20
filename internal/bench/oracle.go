package bench

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"paymentledger/internal/audit"
)

// procPI / procRefund are the processor's records (simulator snapshot or
// Stripe API listing): the ground truth for money movement.
type procPI struct {
	ID       string
	Status   string
	Amount   int64
	Currency string
	Metadata map[string]string
}

type procRefund struct {
	ID            string
	PaymentIntent string
	Status        string
	Amount        int64
	Currency      string
	Metadata      map[string]string
}

type OracleCheck struct {
	Name       string `json:"name"`
	Passed     bool   `json:"passed"`
	Violations int64  `json:"violations"`
	Detail     string `json:"detail"`
}

type Oracle struct {
	Passed bool          `json:"passed"`
	Checks []OracleCheck `json:"checks"`
	// Informational (not pass/fail).
	DuplicateChargeAmountMinor int64          `json:"duplicate_charge_amount_minor"`
	GaveUpFinalStatus          map[string]int `json:"gave_up_final_db_status"`
}

func (o *Oracle) add(name, detail string, violations int64) {
	c := OracleCheck{Name: name, Passed: violations == 0, Violations: violations, Detail: detail}
	o.Checks = append(o.Checks, c)
	o.Passed = o.Passed && c.Passed
}

type dbPayment struct {
	id, status, currency, clientRef string
	amount                          int64
	processorID                     *string
}

func runOracle(ctx context.Context, pool *pgxpool.Pool, rep *audit.Report, pis []procPI, refunds []procRefund,
	ops []*opRecord, outboxDelivered map[string]int) (*Oracle, error) {

	o := &Oracle{Passed: true, GaveUpFinalStatus: map[string]int{}}

	// 1. Internal ledger/payment invariants.
	for _, c := range rep.Checks {
		o.add("db."+c.Name, c.Detail, c.Violations)
	}

	// Load DB state.
	rows, err := pool.Query(ctx, `SELECT id, status, amount, currency, processor_id, coalesce(metadata->>'client_ref', '') FROM payments`)
	if err != nil {
		return nil, err
	}
	payments := map[string]*dbPayment{}
	byRef := map[string][]*dbPayment{}
	for rows.Next() {
		p := &dbPayment{}
		if err := rows.Scan(&p.id, &p.status, &p.amount, &p.currency, &p.processorID, &p.clientRef); err != nil {
			rows.Close()
			return nil, err
		}
		payments[p.id] = p
		byRef[p.clientRef] = append(byRef[p.clientRef], p)
	}
	rows.Close()

	dbRefunds := map[string]struct {
		status string
		amount int64
	}{}
	rows, err = pool.Query(ctx, `SELECT id, status, amount FROM refunds`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id, st string
		var amt int64
		if err := rows.Scan(&id, &st, &amt); err != nil {
			rows.Close()
			return nil, err
		}
		dbRefunds[id] = struct {
			status string
			amount int64
		}{st, amt}
	}
	rows.Close()

	// 2. No duplicate charges: at most one succeeded PaymentIntent per
	// logical client request.
	succeededByRef := map[string][]procPI{}
	piByID := map[string]procPI{}
	for _, pi := range pis {
		piByID[pi.ID] = pi
		if pi.Status == "succeeded" {
			succeededByRef[pi.Metadata["client_ref"]] = append(succeededByRef[pi.Metadata["client_ref"]], pi)
		}
	}
	var dup int64
	for _, list := range succeededByRef {
		if len(list) > 1 {
			dup += int64(len(list) - 1)
			for _, pi := range list[1:] {
				o.DuplicateChargeAmountMinor += pi.Amount
			}
		}
	}
	o.add("processor.no_duplicate_charges", "at most one succeeded charge per client request (client_ref)", dup)

	// 3. Every processor charge is a succeeded payment in the ledger.
	var unrecorded int64
	for _, list := range succeededByRef {
		for _, pi := range list {
			p := payments[pi.Metadata["payment_id"]]
			if p == nil || p.status != "succeeded" || p.processorID == nil || *p.processorID != pi.ID || p.amount != pi.Amount {
				unrecorded++
			}
		}
	}
	o.add("processor.charges_recorded_in_ledger", "each succeeded processor charge maps to a succeeded payment with the same id and amount", unrecorded)

	// 4. Every succeeded payment is a succeeded charge at the processor.
	var phantom int64
	for _, p := range payments {
		if p.status != "succeeded" {
			continue
		}
		pi, ok := piByID[deref(p.processorID)]
		if !ok || pi.Status != "succeeded" || pi.Amount != p.amount {
			phantom++
		}
	}
	o.add("processor.ledger_captures_exist_at_processor", "each succeeded payment has a succeeded processor charge", phantom)

	// 5. Refunds match both ways.
	var refundMismatch int64
	seen := map[string]bool{}
	for _, rf := range refunds {
		if rf.Status != "succeeded" {
			continue
		}
		id := rf.Metadata["refund_id"]
		seen[id] = true
		if r, ok := dbRefunds[id]; !ok || r.status != "succeeded" || r.amount != rf.Amount {
			refundMismatch++
		}
	}
	for id, r := range dbRefunds {
		if r.status == "succeeded" && !seen[id] {
			refundMismatch++
		}
	}
	o.add("processor.refunds_match", "succeeded refunds agree between ledger and processor", refundMismatch)

	// 6. Clearing account equals processor net (captured - refunded).
	clearing, err := audit.ClearingBalances(ctx, pool)
	if err != nil {
		return nil, err
	}
	net := map[string]int64{}
	for _, pi := range pis {
		if pi.Status == "succeeded" {
			net[upper(pi.Currency)] += pi.Amount
		}
	}
	for _, rf := range refunds {
		if rf.Status == "succeeded" {
			net[upper(rf.Currency)] -= rf.Amount
		}
	}
	var curMismatch int64
	detail := "processor_clearing balance == processor captured - refunded, per currency"
	for cur := range union(net, clearing) {
		if net[cur] != clearing[cur] {
			curMismatch++
			detail += fmt.Sprintf("; %s ledger=%d processor=%d (diff %d)", cur, clearing[cur], net[cur], clearing[cur]-net[cur])
		}
	}
	o.add("processor.clearing_matches_processor_net", detail, curMismatch)

	// 7. What the client was told is true.
	var lies int64
	for _, op := range ops {
		switch op.Outcome {
		case "succeeded":
			if p := payments[op.PaymentID]; p == nil || p.status != "succeeded" {
				lies++
			}
		case "failed":
			p := payments[op.PaymentID]
			if p == nil || p.status != "failed" || len(succeededByRef[op.Key]) > 0 {
				lies++ // told "failed" but the card was charged
			}
		case "gave_up":
			list := byRef[op.Key]
			st := "none"
			if len(list) > 0 {
				st = list[len(list)-1].status
			}
			o.GaveUpFinalStatus[st]++
		}
	}
	o.add("client.responses_match_final_state", "a succeeded/failed response to the client matches the final ledger and processor state", lies)

	// 7b. Concurrent duplicates of one request get the same answer.
	var dupDisagree int64
	for _, op := range ops {
		if op.Dup == nil || op.DupOutcome == "gave_up" || op.Outcome == "gave_up" {
			continue
		}
		if op.PaymentID != "" && op.DupPaymentID != "" && op.PaymentID != op.DupPaymentID {
			dupDisagree++
		}
	}
	o.add("client.duplicate_requests_agree", "two concurrent requests with one Idempotency-Key refer to the same payment", dupDisagree)

	// 8. Nothing left unresolved after the drain.
	unresolved := rep.Counts["payments_processing"] + rep.Counts["payments_needs_review"] +
		rep.Counts["refunds_processing"] + rep.Counts["refunds_needs_review"]
	o.add("recovery.no_unresolved_after_drain", "no payment or refund remains processing or needs_review after recovery", unresolved)

	// 9. Every outbox event reached the receiver at least once.
	rows, err = pool.Query(ctx, `SELECT event_id FROM outbox_events`)
	if err != nil {
		return nil, err
	}
	var lost int64
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		if outboxDelivered[id] == 0 {
			lost++
		}
	}
	rows.Close()
	o.add("outbox.all_events_delivered", "every outbox event was received at least once by the webhook sink", lost)
	return o, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func upper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		}
	}
	return string(b)
}

func union(a, b map[string]int64) map[string]bool {
	out := map[string]bool{}
	for k := range a {
		out[k] = true
	}
	for k := range b {
		out[k] = true
	}
	return out
}

// paymentFromBody extracts id/status from a 201/202 payment response.
func paymentFromBody(b []byte) (id, status string) {
	var p struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	json.Unmarshal(b, &p) //nolint:errcheck
	return p.ID, p.Status
}
