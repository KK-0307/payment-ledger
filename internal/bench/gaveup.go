package bench

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// GaveUp describes operations where the client exhausted its attempts. The
// money is always right for these (the oracle checks that); what matters is
// how long the client was left without an answer.
type GaveUp struct {
	Count int `json:"count"`
	// Time the client spent retrying before giving up.
	ClientElapsedMs Quantiles `json:"client_elapsed_ms"`
	// Time from the payment's creation to its final state, from the database.
	TimeToFinalMs Quantiles `json:"time_to_final_state_ms"`
	// Final database state and which path got it there.
	ResolvedBy map[string]int `json:"resolved_by"`
	// Run-length encoded attempt outcomes, e.g. "T x3, 409 x8, 503 x1"
	// (T = transport error or client timeout), most common first.
	Patterns map[string]int `json:"attempt_patterns"`
}

func attemptPattern(attempts []attemptResult) string {
	var parts []string
	for i := 0; i < len(attempts); {
		label := "T"
		if attempts[i].status != 0 {
			label = fmt.Sprint(attempts[i].status)
		}
		j := i
		for j < len(attempts) && attempts[j].status == attempts[i].status {
			j++
		}
		parts = append(parts, fmt.Sprintf("%s x%d", label, j-i))
		i = j
	}
	return strings.Join(parts, ", ")
}

func (r *run) gaveUpStats(ctx context.Context, ops []*opRecord) error {
	g := &GaveUp{ResolvedBy: map[string]int{}, Patterns: map[string]int{}}
	byKey := map[string]*opRecord{}
	var elapsed []float64
	for _, op := range ops {
		if op.Outcome != "gave_up" {
			continue
		}
		g.Count++
		byKey[op.Key] = op
		elapsed = append(elapsed, op.Res.latencyMs)
		g.Patterns[attemptPattern(op.Res.attempts)]++
	}
	if g.Count == 0 {
		return nil
	}
	rows, err := r.pool.Query(ctx, `
		SELECT metadata->>'client_ref', status || ':' || coalesce(resolved_by, '-'),
		       extract(epoch FROM updated_at - created_at) * 1000
		FROM payments WHERE status IN ('succeeded', 'failed')`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var toFinal []float64
	for rows.Next() {
		var key, state string
		var ms float64
		if err := rows.Scan(&key, &state, &ms); err != nil {
			return err
		}
		if _, ok := byKey[key]; ok {
			g.ResolvedBy[state]++
			toFinal = append(toFinal, ms)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	g.ClientElapsedMs, g.TimeToFinalMs = quantiles(elapsed), quantiles(toFinal)
	g.Patterns = topN(g.Patterns, 8)
	r.res.GaveUp = g
	return nil
}

func topN(m map[string]int, n int) map[string]int {
	type kv struct {
		k string
		v int
	}
	var all []kv
	for k, v := range m {
		all = append(all, kv{k, v})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].v > all[j].v || (all[i].v == all[j].v && all[i].k < all[j].k) })
	out := map[string]int{}
	for i := 0; i < len(all) && i < n; i++ {
		out[all[i].k] = all[i].v
	}
	return out
}
