// Command bench runs fault-injection benchmarks and renders their results.
//
//	bench run    -scenario bench/scenarios/baseline.json [-repeat 3]
//	bench run    -scenario 'bench/scenarios/*.json' -repeat 3
//	bench report -dir bench/results
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"paymentledger/internal/bench"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = runCmd(os.Args[2:])
	case "report":
		err = reportCmd(os.Args[2:])
	case "compare":
		err = compareCmd(os.Args[2:])
	case "matrix":
		err = matrixCmd(os.Args[2:])
	case "plot":
		err = plotCmd(os.Args[2:])
	case "outcomes":
		err = outcomesCmd(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench run -scenario GLOB [-repeat N] | bench report -dir DIR | bench compare -before DIR -after DIR | bench matrix -arm LABEL=DIR ... [-csv FILE] | bench plot -arm LABEL=DIR ... -metric throughput|p99|lockshare -scenarios A,B -out FILE.svg")
	os.Exit(2)
}

func runCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	pattern := fs.String("scenario", "", "scenario file or glob")
	repeat := fs.Int("repeat", 1, "runs per scenario")
	apiBin := fs.String("api-bin", "bin/api", "API server binary (go build -o bin/api ./cmd/api)")
	admin := fs.String("admin-db", os.Getenv("BENCH_ADMIN_DATABASE_URL"), "PostgreSQL URL with CREATE DATABASE rights (default $BENCH_ADMIN_DATABASE_URL)")
	out := fs.String("out", "bench/results", "results directory")
	logs := fs.String("logs", "data/bench-logs", "API log directory")
	keep := fs.Bool("keep-db", false, "keep each run's database")
	var envs multiFlag
	fs.Var(&envs, "env", "KEY=VALUE added to the API processes' environment after the scenario's server.env (repeatable); recorded in the result")
	fs.Parse(args) //nolint:errcheck
	if *pattern == "" || *admin == "" {
		return fmt.Errorf("-scenario and -admin-db (or BENCH_ADMIN_DATABASE_URL) are required")
	}
	files, err := filepath.Glob(*pattern)
	if err != nil || len(files) == 0 {
		return fmt.Errorf("no scenarios match %q", *pattern)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	overrides := map[string]string{}
	for _, kv := range envs {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			return fmt.Errorf("-env %q: want KEY=VALUE", kv)
		}
		overrides[k] = v
	}

	failed := 0
	for _, f := range files {
		sc, raw, err := bench.LoadScenario(f)
		if err != nil {
			return err
		}
		for i := 0; i < *repeat; i++ {
			res, err := bench.Run(ctx, sc, raw, bench.Options{AdminDBURL: *admin, APIBin: *apiBin, ResultsDir: *out, LogDir: *logs, KeepDB: *keep, Progress: os.Stderr, EnvOverrides: overrides})
			if err != nil {
				return fmt.Errorf("%s run %d: %w", sc.Name, i+1, err)
			}
			path, err := bench.WriteResult(*out, res)
			if err != nil {
				return err
			}
			verdict := "PASS"
			if !res.Oracle.Passed {
				verdict = "FAIL"
				failed++
			}
			fmt.Fprintf(os.Stderr, "[%s] %s: %d ops in %.1fs (%.0f ops/s), succeeded p50=%.1fms p99=%.1fms, drain %.2fs, oracle %s -> %s\n",
				sc.Name, res.RunID, res.Load.Ops, res.Load.DurationS, res.Load.ThroughputOpsPerS,
				res.LatencyMs.Succeeded.P50, res.LatencyMs.Succeeded.P99, res.Drain.DurationS, verdict, path)
			for _, c := range res.Oracle.Checks {
				if !c.Passed {
					fmt.Fprintf(os.Stderr, "    FAILED %s: %d violations (%s)\n", c.Name, c.Violations, c.Detail)
				}
			}
		}
	}
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "%d run(s) failed the oracle (expected only for the unsafe baseline)\n", failed)
	}
	return nil
}

func reportCmd(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	dir := fs.String("dir", "bench/results", "results directory")
	fs.Parse(args) //nolint:errcheck
	results, err := bench.LoadResults(*dir)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		return fmt.Errorf("no results in %s", *dir)
	}
	bench.WriteMarkdown(os.Stdout, results)
	return nil
}

func compareCmd(args []string) error {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	beforeDir := fs.String("before", "", "results directory of the baseline")
	afterDir := fs.String("after", "bench/results", "results directory to compare")
	fs.Parse(args) //nolint:errcheck
	before, err := bench.LoadResults(*beforeDir)
	if err != nil {
		return err
	}
	after, err := bench.LoadResults(*afterDir)
	if err != nil {
		return err
	}
	if len(before) == 0 || len(after) == 0 {
		return fmt.Errorf("need results in both directories")
	}
	bench.WriteComparison(os.Stdout, before, after)
	return nil
}

// multiFlag collects a repeatable string flag.
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

// armFlags parses repeated -arm LABEL=DIR values.
func loadArms(specs multiFlag) ([]bench.Arm, error) {
	if len(specs) < 1 {
		return nil, fmt.Errorf("at least one -arm LABEL=DIR is required")
	}
	var arms []bench.Arm
	for _, spec := range specs {
		label, dir, ok := strings.Cut(spec, "=")
		if !ok || label == "" || dir == "" {
			return nil, fmt.Errorf("-arm %q: want LABEL=DIR", spec)
		}
		rs, err := bench.LoadResults(dir)
		if err != nil {
			return nil, err
		}
		if len(rs) == 0 {
			return nil, fmt.Errorf("no results in %s", dir)
		}
		arms = append(arms, bench.Arm{Label: label, Results: rs})
	}
	return arms, nil
}

func matrixCmd(args []string) error {
	fs := flag.NewFlagSet("matrix", flag.ExitOnError)
	var armSpecs multiFlag
	fs.Var(&armSpecs, "arm", "LABEL=DIR of results (repeatable; the first arm is the reference)")
	csvPath := fs.String("csv", "", "also write one CSV row per run to this file")
	fs.Parse(args) //nolint:errcheck
	arms, err := loadArms(armSpecs)
	if err != nil {
		return err
	}
	bench.WriteMatrix(os.Stdout, arms)
	if *csvPath != "" {
		f, err := os.Create(*csvPath)
		if err != nil {
			return err
		}
		defer f.Close()
		return bench.WriteMatrixCSV(f, arms)
	}
	return nil
}

func plotCmd(args []string) error {
	fs := flag.NewFlagSet("plot", flag.ExitOnError)
	var armSpecs multiFlag
	fs.Var(&armSpecs, "arm", "LABEL=DIR of results (repeatable)")
	metric := fs.String("metric", "throughput", "throughput | p99 | lockshare")
	scenarios := fs.String("scenarios", "", "comma-separated scenario names, in plot order (default: all common)")
	title := fs.String("title", "", "chart title")
	subtitle := fs.String("subtitle", "", "benchmark conditions shown under the title (\\n separates lines)")
	labels := fs.String("labels", "", "comma-separated category labels, in the order of -scenarios")
	values := fs.Bool("values", false, "print each bar's median above it")
	out := fs.String("out", "", "SVG file to write")
	fs.Parse(args) //nolint:errcheck
	if *out == "" {
		return fmt.Errorf("-out is required")
	}
	arms, err := loadArms(armSpecs)
	if err != nil {
		return err
	}
	var names []string
	if *scenarios != "" {
		names = strings.Split(*scenarios, ",")
	} else {
		names = bench.CommonScenarios(arms)
	}
	var ylabel string
	var val func(*bench.Result) float64
	switch *metric {
	case "throughput":
		ylabel, val = "operations per second", func(r *bench.Result) float64 { return r.Load.ThroughputOpsPerS }
	case "p99":
		ylabel, val = "succeeded-payment p99 latency (ms)", func(r *bench.Result) float64 { return r.LatencyMs.Succeeded.P99 }
	case "lockshare":
		ylabel, val = "row-lock waits, % of active-backend samples", func(r *bench.Result) float64 {
			if r.Database == nil {
				return 0
			}
			return 100 * r.Database.RowLockWaitShare
		}
	default:
		return fmt.Errorf("unknown metric %q", *metric)
	}
	if *title == "" {
		*title = *metric
	}
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	chart := bench.ChartFromArms(*title, ylabel, arms, names, val)
	chart.Subtitle = strings.ReplaceAll(*subtitle, `\n`, "\n")
	chart.ValueLabels = *values
	if *labels != "" {
		chart.Labels = strings.Split(*labels, ",")
	}
	chart.WriteSVG(f)
	return nil
}

// outcomesCmd draws the oracle's verdict per group of runs. A group is
// LABEL=DIR[+DIR...][@skip=PREFIX | @only=PREFIX][@expect-failure][@note=TEXT]:
// skip/only filter by scenario-name prefix.
func outcomesCmd(args []string) error {
	fs := flag.NewFlagSet("outcomes", flag.ExitOnError)
	var specs multiFlag
	fs.Var(&specs, "group", "LABEL=DIR[+DIR][@skip=PREFIX|@only=PREFIX][@expect-failure][@note=TEXT] (repeatable)")
	title := fs.String("title", "Accounting correctness outcomes", "chart title")
	subtitle := fs.String("subtitle", "", "conditions shown under the title (\\n separates lines)")
	out := fs.String("out", "", "SVG file to write")
	fs.Parse(args) //nolint:errcheck
	if *out == "" || len(specs) == 0 {
		return fmt.Errorf("-out and at least one -group are required")
	}
	var groups []bench.OutcomeGroup
	for _, spec := range specs {
		parts := strings.Split(spec, "@")
		label, dirs, ok := strings.Cut(parts[0], "=")
		if !ok || label == "" || dirs == "" {
			return fmt.Errorf("-group %q: want LABEL=DIR[+DIR]", spec)
		}
		var skip, only, note string
		byDesign := false
		for _, opt := range parts[1:] {
			switch {
			case strings.HasPrefix(opt, "skip="):
				skip = strings.TrimPrefix(opt, "skip=")
			case strings.HasPrefix(opt, "only="):
				only = strings.TrimPrefix(opt, "only=")
			case opt == "expect-failure":
				byDesign = true
			case strings.HasPrefix(opt, "note="):
				note = strings.TrimPrefix(opt, "note=")
			default:
				return fmt.Errorf("-group %q: unknown option %q", spec, opt)
			}
		}
		var rs []*bench.Result
		for _, dir := range strings.Split(dirs, "+") {
			loaded, err := bench.LoadResults(dir)
			if err != nil {
				return err
			}
			for _, r := range loaded {
				name := r.Scenario.Name
				if (skip != "" && strings.HasPrefix(name, skip)) || (only != "" && !strings.HasPrefix(name, only)) {
					continue
				}
				rs = append(rs, r)
			}
		}
		if len(rs) == 0 {
			return fmt.Errorf("-group %q matches no results", spec)
		}
		groups = append(groups, bench.GroupFromResults(label, rs, byDesign, note))
	}
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	bench.WriteOutcomesSVG(f, *title, strings.ReplaceAll(*subtitle, `\n`, "\n"), groups)
	return nil
}
