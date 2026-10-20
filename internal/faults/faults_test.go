package faults

import (
	"math"
	"strconv"
	"testing"
)

func TestUniformDeterministicAndCalibrated(t *testing.T) {
	if Uniform(1, "a", "b") != Uniform(1, "a", "b") {
		t.Fatal("not deterministic")
	}
	if Uniform(1, "a", "b") == Uniform(2, "a", "b") || Uniform(1, "ab") == Uniform(1, "a", "b") {
		t.Fatal("seed or part boundaries ignored")
	}
	const n = 200_000
	hits := 0
	var sum float64
	for i := 0; i < n; i++ {
		u := Uniform(7, "k", strconv.Itoa(i))
		if u < 0 || u >= 1 {
			t.Fatalf("out of range: %v", u)
		}
		if u < 0.05 {
			hits++
		}
		sum += u
	}
	if p := float64(hits) / n; math.Abs(p-0.05) > 0.003 {
		t.Fatalf("P(u<0.05) = %.4f", p)
	}
	if m := sum / n; math.Abs(m-0.5) > 0.005 {
		t.Fatalf("mean = %.4f", m)
	}
}

func TestLatencyMedian(t *testing.T) {
	f := HTTP{LatencyMedianMs: 100, LatencySigma: 0.5}
	below := 0
	const n = 20_000
	for i := 0; i < n; i++ {
		if f.Latency(3, strconv.Itoa(i), 0).Milliseconds() < 100 {
			below++
		}
	}
	if p := float64(below) / n; math.Abs(p-0.5) > 0.02 {
		t.Fatalf("fraction below median = %.3f", p)
	}
}
