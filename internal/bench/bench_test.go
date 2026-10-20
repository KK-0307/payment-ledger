package bench

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

func TestQuantilesIncludeP95(t *testing.T) {
	var ms []float64
	for i := 1; i <= 100; i++ {
		ms = append(ms, float64(i))
	}
	q := quantiles(ms)
	if q.P50 != 50 || q.P90 != 90 || q.P95 != 95 || q.P99 != 99 || q.Max != 100 {
		t.Fatalf("quantiles = %+v", q)
	}
}

func TestHotMerchantShare(t *testing.T) {
	sc := &Scenario{Seed: 7, Merchants: 10, HotMerchantFraction: 0.6}
	const n = 20000
	counts := make([]int, sc.Merchants)
	for i := 0; i < n; i++ {
		// Whatever the ordinary mix picked (here: always merchant 0 or 5),
		// merchant 0's share is the configured fraction and nothing else
		// is sent to it.
		base := 0
		if i%2 == 0 {
			base = 5
		}
		counts[hotMerchant(sc, fmt.Sprint(i), base, sc.Merchants)]++
	}
	if got := float64(counts[0]) / n; math.Abs(got-0.6) > 0.02 {
		t.Fatalf("merchant 0 got %.3f of the operations, want 0.6 (counts %v)", got, counts)
	}
	// Deterministic.
	if hotMerchant(sc, "42", 3, 10) != hotMerchant(sc, "42", 3, 10) {
		t.Fatal("hotMerchant is not deterministic")
	}
	// Disabled: the ordinary mix passes through.
	if hotMerchant(&Scenario{Seed: 7}, "1", 4, 10) != 4 {
		t.Fatal("hot merchant applied while disabled")
	}
}

func TestScenarioValidatesHotFraction(t *testing.T) {
	sc := &Scenario{Name: "x", Dataset: "d", Rows: 1, Merchants: 5, HotMerchantFraction: 1.5}
	sc.defaults()
	if sc.validate() == nil {
		t.Fatal("hot_merchant_fraction 1.5 accepted")
	}
	sc.HotMerchantFraction, sc.Merchants = 0.5, 1
	if sc.validate() == nil {
		t.Fatal("hot merchant with a single merchant accepted")
	}
	sc.Merchants = 5
	if err := sc.validate(); err != nil {
		t.Fatal(err)
	}
}

var fakeClock = time.Unix(1_000_000, 0)

func fakeResult(name string, thr float64, p99 float64, pass bool) *Result {
	fakeClock = fakeClock.Add(time.Second)
	r := &Result{StartedAt: fakeClock, Scenario: &Scenario{Name: name}, Oracle: &Oracle{Passed: pass}, Database: &DBStats{RowLockWaitShare: 0.25, Deadlocks: 1}}
	r.Load.ThroughputOpsPerS, r.Load.Ops, r.Load.Retries = thr, 1000, 10
	r.LatencyMs.Succeeded = Quantiles{P50: 1, P95: 2, P99: p99}
	return r
}

func TestMatrixAndPlot(t *testing.T) {
	arms := []Arm{
		{Label: "single-row", Results: []*Result{fakeResult("s1", 1000, 50, true), fakeResult("s1", 1100, 40, true), fakeResult("s1", 900, 60, true)}},
		{Label: "adaptive", Results: []*Result{fakeResult("s1", 1500, 20, true), fakeResult("s1", 1600, 25, true), fakeResult("s1", 1400, 22, false)}},
	}
	var buf bytes.Buffer
	WriteMatrix(&buf, arms)
	out := buf.String()
	for _, want := range []string{"| s1 | single-row | 3 |", "+50.0 %", "3/3", "3/3 pass", "2/3 pass", "20.0 %"} {
		if !strings.Contains(out, want) {
			t.Errorf("matrix output lacks %q:\n%s", want, out)
		}
	}
	if v := Variability([]float64{900, 1000, 1100}); math.Abs(v-20) > 1e-9 {
		t.Errorf("variability = %v, want 20", v)
	}
	if got := CommonScenarios(arms); len(got) != 1 || got[0] != "s1" {
		t.Errorf("common scenarios = %v", got)
	}

	var csvBuf bytes.Buffer
	if err := WriteMatrixCSV(&csvBuf, arms); err != nil || strings.Count(csvBuf.String(), "\n") != 7 {
		t.Errorf("csv: %v\n%s", err, csvBuf.String())
	}

	var svg bytes.Buffer
	ChartFromArms("t & <title>", "ops/s", arms, []string{"s1"}, func(r *Result) float64 { return r.Load.ThroughputOpsPerS }).WriteSVG(&svg)
	dec := xml.NewDecoder(&svg)
	for {
		if _, err := dec.Token(); err != nil {
			if err.Error() != "EOF" {
				t.Fatalf("SVG is not well-formed XML: %v", err)
			}
			break
		}
	}
}

func TestBurstTimeCompressesEachPeriod(t *testing.T) {
	const period, duty = 4.0, 0.25 // bursts occupy the first 1 s of every 4 s
	prev := -1.0
	perPeriod := map[int]int{}
	for i := 0; i < 2000; i++ {
		orig := float64(i) * 0.01 // 100 arrivals per second over 20 s
		got := burstTime(orig, period, duty)
		if got < prev {
			t.Fatalf("burstTime is not monotonic at %v: %v after %v", orig, got, prev)
		}
		prev = got
		if in := math.Mod(got, period); in >= period*duty+1e-9 {
			t.Fatalf("arrival %v mapped to %v, outside the burst window (offset %v in the period)", orig, got, in)
		}
		// An arrival stays in its own period, so the total span and the
		// mean rate do not change; only the rate within a period does.
		if int(got/period) != int(orig/period) {
			t.Fatalf("arrival %v moved to another period (%v)", orig, got)
		}
		perPeriod[int(got/period)]++
	}
	for p, n := range perPeriod {
		if n != 400 {
			t.Fatalf("period %d holds %d arrivals, want 400", p, n)
		}
	}
	// 400 arrivals in 1 s is 4x the mean of 100/s: mean / duty.
}

func TestScenarioValidatesWorkloadShapes(t *testing.T) {
	mk := func(f func(*Scenario)) error {
		sc := &Scenario{Name: "x", Dataset: "d", Rows: 1}
		sc.defaults()
		f(sc)
		return sc.validate()
	}
	if mk(func(s *Scenario) { s.Load.DuplicateFraction = 1.2 }) == nil {
		t.Error("duplicate_fraction 1.2 accepted")
	}
	if mk(func(s *Scenario) { s.Load.Burst = &Burst{} }) == nil {
		t.Error("empty burst accepted")
	}
}
