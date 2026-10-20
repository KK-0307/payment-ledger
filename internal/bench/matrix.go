package bench

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// Arm is one implementation or configuration under comparison and the
// results measured for it.
type Arm struct {
	Label   string
	Results []*Result
}

// ArmStats aggregates the runs of one scenario in one arm.
type ArmStats struct {
	Runs, Pass                                                   int
	Thr, P50, P95, P99, Retries, FinalErr, AttemptErr, LockShare []float64
	ThrByStart                                                   []float64 // in start order, for pairing interleaved rounds
	Deadlocks, Rollbacks, Commits                                int64
	LedgerViolations, DuplicateCharges                           int64
}

// Aggregate groups results by scenario name.
func Aggregate(results []*Result) map[string]*ArmStats {
	out := map[string]*ArmStats{}
	byStart := append([]*Result(nil), results...)
	sort.SliceStable(byStart, func(i, j int) bool { return byStart[i].StartedAt.Before(byStart[j].StartedAt) })
	for _, r := range byStart {
		a := out[r.Scenario.Name]
		if a == nil {
			a = &ArmStats{}
			out[r.Scenario.Name] = a
		}
		a.Runs++
		if r.Oracle != nil && r.Oracle.Passed {
			a.Pass++
		}
		a.Thr = append(a.Thr, r.Load.ThroughputOpsPerS)
		a.ThrByStart = append(a.ThrByStart, r.Load.ThroughputOpsPerS)
		a.P50 = append(a.P50, r.LatencyMs.Succeeded.P50)
		a.P95 = append(a.P95, r.LatencyMs.Succeeded.P95)
		a.P99 = append(a.P99, r.LatencyMs.Succeeded.P99)
		a.Retries = append(a.Retries, float64(r.Load.Retries)/float64(max(1, r.Load.Ops)))
		a.FinalErr = append(a.FinalErr, r.Load.FinalErrorRate)
		a.AttemptErr = append(a.AttemptErr, r.Load.AttemptErrorRate)
		if r.Database != nil {
			a.LockShare = append(a.LockShare, r.Database.RowLockWaitShare)
			a.Deadlocks += r.Database.Deadlocks
			a.Rollbacks += r.Database.TxRolledBack
			a.Commits += r.Database.TxCommitted
		}
		a.DuplicateCharges += checkViolations(r.Oracle, "processor.no_duplicate_charges")
		if r.Oracle != nil {
			for _, c := range r.Oracle.Checks {
				if strings.HasPrefix(c.Name, "db.") {
					a.LedgerViolations += c.Violations
				}
			}
		}
	}
	return out
}

// Variability is (max-min)/median of the runs, in percent.
func Variability(v []float64) float64 {
	if len(v) < 2 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	m := medianOf(s)
	if m == 0 {
		return 0
	}
	return 100 * (s[len(s)-1] - s[0]) / m
}

func rangeOf(v []float64) (lo, hi float64) {
	if len(v) == 0 {
		return 0, 0
	}
	lo, hi = v[0], v[0]
	for _, x := range v {
		lo, hi = min(lo, x), max(hi, x)
	}
	return
}

func scenarioNames(arms []Arm) []string {
	seen := map[string]bool{}
	var names []string
	for _, a := range arms {
		for _, r := range a.Results {
			if !seen[r.Scenario.Name] {
				seen[r.Scenario.Name] = true
				names = append(names, r.Scenario.Name)
			}
		}
	}
	sort.Strings(names)
	return names
}

// WriteMatrix renders two tables for scenarios measured in several arms:
// performance (throughput with spread and change against the first arm,
// latency percentiles) and errors, lock waits and correctness. Numbers are
// medians over the runs with min–max in parentheses.
func WriteMatrix(w io.Writer, arms []Arm) {
	names := scenarioNames(arms)
	agg := make([]map[string]*ArmStats, len(arms))
	for i, a := range arms {
		agg[i] = Aggregate(a.Results)
	}

	fmt.Fprintln(w, "| scenario | arm | runs | throughput ops/s | run-to-run spread | vs "+arms[0].Label+" | rounds faster than "+arms[0].Label+" | p50 ms | p95 ms | p99 ms |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|")
	for _, n := range names {
		var base float64
		for i, a := range arms {
			s := agg[i][n]
			if s == nil {
				continue
			}
			med := medianOf(s.Thr)
			if i == 0 {
				base = med
			}
			delta, rounds := "—", "—"
			if i > 0 && base > 0 {
				delta = fmt.Sprintf("%+.1f %%", 100*(med-base)/base)
				if ref := agg[0][n]; ref != nil {
					rounds = roundsFaster(s.ThrByStart, ref.ThrByStart)
				}
			}
			fmt.Fprintf(w, "| %s | %s | %d | %s | %.1f %% | %s | %s | %s | %s | %s |\n", n, a.Label, s.Runs,
				spread(s.Thr, "%.0f"), Variability(s.Thr), delta, rounds, spread(s.P50, "%.1f"), spread(s.P95, "%.1f"), spread(s.P99, "%.1f"))
		}
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "| scenario | arm | final error rate | attempt error rate | client retries/op | row-lock wait share | deadlocks | tx rollbacks / commits | oracle | ledger invariant violations | duplicate charges |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|---|")
	for _, n := range names {
		for i, a := range arms {
			s := agg[i][n]
			if s == nil {
				continue
			}
			fmt.Fprintf(w, "| %s | %s | %s | %s | %s | %s | %d | %d / %d | %d/%d pass | %d | %d |\n", n, a.Label,
				pct(s.FinalErr), pct(s.AttemptErr), spread(s.Retries, "%.3f"), pct(s.LockShare),
				s.Deadlocks, s.Rollbacks, s.Commits, s.Pass, s.Runs, s.LedgerViolations, s.DuplicateCharges)
		}
	}
}

// roundsFaster compares the k-th run of an arm with the k-th run of the
// reference. Interleaved runs were made in rounds (every arm once, back to
// back), so this pairs runs that shared the machine's momentary state.
func roundsFaster(arm, ref []float64) string {
	n := min(len(arm), len(ref))
	wins := 0
	for k := 0; k < n; k++ {
		if arm[k] > ref[k] {
			wins++
		}
	}
	return fmt.Sprintf("%d/%d", wins, n)
}

func pct(v []float64) string {
	if len(v) == 0 {
		return "-"
	}
	p := make([]float64, len(v))
	for i := range v {
		p[i] = 100 * v[i]
	}
	return spread(p, "%.1f") + " %"
}

// WriteMatrixCSV writes one row per run, for external plotting.
func WriteMatrixCSV(w io.Writer, arms []Arm) error {
	cw := csv.NewWriter(w)
	if err := cw.Write([]string{"arm", "scenario", "run_id", "ops", "throughput_ops_s", "p50_ms", "p95_ms", "p99_ms",
		"final_error_rate", "attempt_error_rate", "retries", "row_lock_wait_share", "deadlocks", "tx_rolled_back", "tx_committed", "oracle_passed"}); err != nil {
		return err
	}
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	for _, a := range arms {
		for _, r := range a.Results {
			var share float64
			var dl, rb, cm int64
			if r.Database != nil {
				share, dl, rb, cm = r.Database.RowLockWaitShare, r.Database.Deadlocks, r.Database.TxRolledBack, r.Database.TxCommitted
			}
			if err := cw.Write([]string{a.Label, r.Scenario.Name, r.RunID, strconv.Itoa(r.Load.Ops), f(r.Load.ThroughputOpsPerS),
				f(r.LatencyMs.Succeeded.P50), f(r.LatencyMs.Succeeded.P95), f(r.LatencyMs.Succeeded.P99),
				f(r.Load.FinalErrorRate), f(r.Load.AttemptErrorRate), strconv.Itoa(r.Load.Retries), f(share),
				strconv.FormatInt(dl, 10), strconv.FormatInt(rb, 10), strconv.FormatInt(cm, 10),
				strconv.FormatBool(r.Oracle != nil && r.Oracle.Passed)}); err != nil {
				return err
			}
		}
	}
	cw.Flush()
	return cw.Error()
}

// CommonScenarios returns the scenario names present in every arm, sorted.
func CommonScenarios(arms []Arm) []string {
	var out []string
	for _, n := range scenarioNames(arms) {
		all := true
		for _, a := range arms {
			found := false
			for _, r := range a.Results {
				if r.Scenario.Name == n {
					found = true
					break
				}
			}
			all = all && found
		}
		if all {
			out = append(out, n)
		}
	}
	return out
}
