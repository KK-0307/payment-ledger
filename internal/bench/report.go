package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// LoadResults reads every result JSON under dir.
func LoadResults(dir string) ([]*Result, error) {
	var out []*Result
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".json") {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var r Result
		if err := json.Unmarshal(b, &r); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if r.Scenario != nil {
			out = append(out, &r)
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Scenario.Name != out[j].Scenario.Name {
			return out[i].Scenario.Name < out[j].Scenario.Name
		}
		return out[i].RunID < out[j].RunID
	})
	return out, err
}

func checkViolations(o *Oracle, name string) int64 {
	if o == nil {
		return 0
	}
	for _, c := range o.Checks {
		if c.Name == name {
			return c.Violations
		}
	}
	return 0
}

// spread formats the median and, for several runs, the min-max range.
func spread(vals []float64, format string) string {
	if len(vals) == 0 {
		return "-"
	}
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	med := s[len(s)/2]
	if len(s)%2 == 0 {
		med = (s[len(s)/2-1] + s[len(s)/2]) / 2
	}
	if len(s) == 1 || s[0] == s[len(s)-1] {
		return fmt.Sprintf(format, med)
	}
	return fmt.Sprintf(format+" ("+format+"–"+format+")", med, s[0], s[len(s)-1])
}

// WriteMarkdown renders a summary table grouped by scenario. Numbers are
// medians over the runs found, with min–max in parentheses.
func WriteMarkdown(w io.Writer, results []*Result) {
	groups := map[string][]*Result{}
	var names []string
	for _, r := range results {
		if _, ok := groups[r.Scenario.Name]; !ok {
			names = append(names, r.Scenario.Name)
		}
		groups[r.Scenario.Name] = append(groups[r.Scenario.Name], r)
	}

	fmt.Fprintln(w, "| scenario | runs | ops | throughput (ops/s) | p50 ms | p99 ms | retries/op | drain s | oracle | duplicate charges | ledger≠processor | client told wrong |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|---|---|")
	for _, name := range names {
		rs := groups[name]
		var thr, p50, p99, retr, drain, dup, mism, lies []float64
		pass := 0
		for _, r := range rs {
			thr = append(thr, r.Load.ThroughputOpsPerS)
			p50 = append(p50, r.LatencyMs.Succeeded.P50)
			p99 = append(p99, r.LatencyMs.Succeeded.P99)
			retr = append(retr, float64(r.Load.Retries)/float64(max(1, r.Load.Ops)))
			drain = append(drain, r.Drain.DurationS)
			dup = append(dup, float64(checkViolations(r.Oracle, "processor.no_duplicate_charges")))
			mism = append(mism, float64(checkViolations(r.Oracle, "processor.charges_recorded_in_ledger")+
				checkViolations(r.Oracle, "processor.ledger_captures_exist_at_processor")))
			lies = append(lies, float64(checkViolations(r.Oracle, "client.responses_match_final_state")))
			if r.Oracle != nil && r.Oracle.Passed {
				pass++
			}
		}
		fmt.Fprintf(w, "| %s | %d | %d | %s | %s | %s | %s | %s | %d/%d pass | %s | %s | %s |\n",
			name, len(rs), rs[0].Load.Ops, spread(thr, "%.0f"), spread(p50, "%.1f"), spread(p99, "%.1f"),
			spread(retr, "%.3f"), spread(drain, "%.2f"), pass, len(rs), spread(dup, "%.0f"), spread(mism, "%.0f"), spread(lies, "%.0f"))
	}

	writeSweepTable(w, names, results)

	fmt.Fprintln(w)
	fmt.Fprintln(w, "Injected faults (median per run):")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "| scenario | processor 429 | 5xx before | reset before | lost after charge | 502 after charge | sim cached 500 | DB conn resets | DB blackouts | API SIGKILLs |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|---|---|")
	for _, name := range names {
		rs := groups[name]
		col := func(f func(*Result) float64) string {
			var v []float64
			for _, r := range rs {
				v = append(v, f(r))
			}
			return spread(v, "%.0f")
		}
		fmt.Fprintf(w, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", name,
			col(func(r *Result) float64 { return float64(r.Faults.Processor.RateLimited) }),
			col(func(r *Result) float64 { return float64(r.Faults.Processor.ErrorBefore) }),
			col(func(r *Result) float64 { return float64(r.Faults.Processor.ResetBefore) }),
			col(func(r *Result) float64 { return float64(r.Faults.Processor.DropAfter) }),
			col(func(r *Result) float64 { return float64(r.Faults.Processor.ErrorAfter) }),
			col(func(r *Result) float64 {
				if r.Faults.Simulator == nil {
					return 0
				}
				return float64(r.Faults.Simulator.InternalErrors)
			}),
			col(func(r *Result) float64 { return float64(r.Faults.Database.Resets) }),
			col(func(r *Result) float64 { return float64(r.Faults.Database.Blackouts) }),
			col(func(r *Result) float64 { return float64(r.Faults.APICrashes) }))
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, "Final outcomes and who resolved them (summed over runs):")
	fmt.Fprintln(w)
	for _, name := range names {
		outcomes := map[string]int{}
		resolved := map[string]int64{}
		for _, r := range groups[name] {
			for k, v := range r.Outcomes {
				outcomes[k] += v
			}
			for k, v := range r.ResolvedBy {
				resolved[k] += v
			}
		}
		fmt.Fprintf(w, "- **%s**: client outcomes %s; final states by resolver %s\n", name, fmtMap(outcomes), fmtMap64(resolved))
	}
}

func fmtMap(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%d", k, m[k])
	}
	return "`" + strings.Join(parts, " ") + "`"
}

func fmtMap64(m map[string]int64) string {
	c := make(map[string]int, len(m))
	for k, v := range m {
		c[k] = int(v)
	}
	return fmtMap(c)
}

// sweepGap separates sweeps: runs of one `make bench` invocation follow each
// other within minutes, so a longer pause starts a new sweep.
const sweepGap = 30 * time.Minute

// writeSweepTable prints per-sweep throughput so that a shift between
// sweeps (same binary, same scenarios, different time) is visible instead of
// being averaged away.
func writeSweepTable(w io.Writer, names []string, results []*Result) {
	sorted := append([]*Result(nil), results...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].StartedAt.Before(sorted[j].StartedAt) })
	sweepOf := map[string]int{}
	var starts []time.Time
	for i, r := range sorted {
		if i == 0 || r.StartedAt.Sub(sorted[i-1].StartedAt) > sweepGap {
			starts = append(starts, r.StartedAt)
		}
		sweepOf[r.RunID] = len(starts) - 1
	}
	if len(starts) < 2 {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Throughput (ops/s) per sweep, median (min–max). A new sweep starts after a pause of more than 30 minutes:")
	fmt.Fprintln(w)
	header, sep := "| scenario |", "|---|"
	for i, t := range starts {
		header += fmt.Sprintf(" sweep %c (%s UTC) |", 'A'+i, t.UTC().Format("2006-01-02 15:04"))
		sep += "---|"
	}
	fmt.Fprintln(w, header)
	fmt.Fprintln(w, sep)
	for _, name := range names {
		row := "| " + name + " |"
		for i := range starts {
			var thr []float64
			for _, r := range results {
				if r.Scenario.Name == name && sweepOf[r.RunID] == i {
					thr = append(thr, r.Load.ThroughputOpsPerS)
				}
			}
			row += " " + spread(thr, "%.0f") + " |"
		}
		fmt.Fprintln(w, row)
	}
}

func medianOf(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	if len(s)%2 == 0 {
		return (s[len(s)/2-1] + s[len(s)/2]) / 2
	}
	return s[len(s)/2]
}

type scenarioAgg struct {
	runs, pass, ops, gaveUp int
	thr, p50, p99, final99  []float64
	maxLatency              float64
}

func aggregate(results []*Result) map[string]*scenarioAgg {
	out := map[string]*scenarioAgg{}
	for _, r := range results {
		a := out[r.Scenario.Name]
		if a == nil {
			a = &scenarioAgg{}
			out[r.Scenario.Name] = a
		}
		a.runs++
		if r.Oracle != nil && r.Oracle.Passed {
			a.pass++
		}
		a.ops += r.Load.Ops
		a.gaveUp += r.Outcomes["gave_up"]
		a.thr = append(a.thr, r.Load.ThroughputOpsPerS)
		a.p50 = append(a.p50, r.LatencyMs.Succeeded.P50)
		a.p99 = append(a.p99, r.LatencyMs.Succeeded.P99)
		a.final99 = append(a.final99, r.LatencyMs.AllFinal.P99)
		a.maxLatency = max(a.maxLatency, r.LatencyMs.AllFinal.Max)
	}
	return out
}

// WriteComparison renders a before/after table for two sets of results.
func WriteComparison(w io.Writer, before, after []*Result) {
	b, a := aggregate(before), aggregate(after)
	var names []string
	for n := range a {
		if _, ok := b[n]; ok {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	fmt.Fprintln(w, "| scenario | runs (before/after) | oracle | throughput ops/s | clients gave up | succeeded p99 ms | all-ops p99 ms | slowest op ms |")
	fmt.Fprintln(w, "|---|---|---|---|---|---|---|---|")
	for _, n := range names {
		x, y := b[n], a[n]
		fmt.Fprintf(w, "| %s | %d / %d | %d/%d → %d/%d | %.0f → %.0f | %d (%.2f%%) → %d (%.2f%%) | %.0f → %.0f | %.0f → %.0f | %.0f → %.0f |\n",
			n, x.runs, y.runs, x.pass, x.runs, y.pass, y.runs, medianOf(x.thr), medianOf(y.thr),
			x.gaveUp, 100*float64(x.gaveUp)/float64(max(1, x.ops)), y.gaveUp, 100*float64(y.gaveUp)/float64(max(1, y.ops)),
			medianOf(x.p99), medianOf(y.p99), medianOf(x.final99), medianOf(y.final99), x.maxLatency, y.maxLatency)
	}
}
