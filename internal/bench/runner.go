package bench

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"paymentledger/internal/audit"
	"paymentledger/internal/dataset"
	"paymentledger/internal/db"
	"paymentledger/internal/faultproxy"
	"paymentledger/internal/faults"
	"paymentledger/internal/stripe"
	"paymentledger/internal/stripesim"
)

type Options struct {
	AdminDBURL string
	APIBin     string
	ResultsDir string
	LogDir     string
	KeepDB     bool
	Progress   io.Writer
	// EnvOverrides are added to the API processes' environment after the
	// scenario's own server.env (e.g. LEDGER_PAYABLE_SHARD_RATE=0). They are
	// recorded in the result.
	EnvOverrides map[string]string
}

type opRecord struct {
	Row        int
	Key        string
	Merchant   string
	Amount     int64
	Currency   string
	PM         string
	WantRefund bool
	Scheduled  time.Duration // open loop: offset from load start

	Res           callResult
	LatencyMs     float64
	Outcome       string      // succeeded | failed | needs_review | gave_up | rejected
	Dup           *callResult // concurrent duplicate request, same Idempotency-Key
	DupOutcome    string
	DupPaymentID  string
	PaymentID     string
	RefundRes     *callResult
	RefundOutcome string
	RefundLatency float64
}

type Result struct {
	RunID          string            `json:"run_id"`
	Scenario       *Scenario         `json:"scenario"`
	ScenarioSHA256 string            `json:"scenario_sha256"`
	EnvOverrides   map[string]string `json:"env_overrides,omitempty"`
	StartedAt      time.Time         `json:"started_at"`
	FinishedAt     time.Time         `json:"finished_at"`
	Environment    Environment       `json:"environment"`

	Dataset struct {
		File             string  `json:"file"`
		SHA256           string  `json:"sha256"`
		Offset           int     `json:"offset"`
		Rows             int     `json:"rows"`
		AmountTotalMinor int64   `json:"amount_total_minor"`
		SpanSeconds      float64 `json:"span_seconds"`
		TimeScale        float64 `json:"time_scale,omitempty"`
		FraudRows        int     `json:"label_1_rows"`
	} `json:"dataset"`

	Load struct {
		DurationS         float64        `json:"duration_s"`
		Ops               int            `json:"ops"`
		ThroughputOpsPerS float64        `json:"throughput_ops_per_s"`
		Attempts          int            `json:"attempts"`
		DuplicateRequests int            `json:"duplicate_requests,omitempty"`
		Retries           int            `json:"retries"`
		AttemptHistogram  map[string]int `json:"attempt_histogram"`
		AttemptStatuses   map[string]int `json:"attempt_statuses"`
		// FinalErrorRate: operations that ended gave_up, rejected or
		// needs_review, over all operations (a declined card is a correct
		// "failed" outcome, not an error). AttemptErrorRate: attempts that
		// got no answer or a 5xx, over all attempts.
		FinalErrorRate   float64 `json:"final_error_rate"`
		AttemptErrorRate float64 `json:"attempt_error_rate"`
	} `json:"load"`

	Database *DBStats `json:"database,omitempty"`

	Outcomes  map[string]int `json:"outcomes"`
	LatencyMs struct {
		Succeeded    Quantiles `json:"succeeded"`
		AllFinal     Quantiles `json:"all_final"`
		SingleAttmpt Quantiles `json:"per_attempt"`
	} `json:"latency_ms"`

	Refunds struct {
		Requested int            `json:"requested"`
		Outcomes  map[string]int `json:"outcomes"`
		LatencyMs Quantiles      `json:"latency_ms"`
	} `json:"refunds"`

	Drain struct {
		DurationS float64          `json:"duration_s"`
		TimedOut  bool             `json:"timed_out"`
		Remaining map[string]int64 `json:"remaining"`
	} `json:"drain"`

	Faults struct {
		Processor  faultproxy.HTTPStats `json:"processor_link"`
		Database   faultproxy.TCPStats  `json:"database_link"`
		Simulator  *stripesim.Stats     `json:"simulator,omitempty"`
		APICrashes int                  `json:"api_crashes"`
		Events     []string             `json:"events"`
		Sink       SinkStats            `json:"outbox_sink"`
	} `json:"faults"`

	ResolvedBy map[string]int64 `json:"resolved_by"`
	GaveUp     *GaveUp          `json:"gave_up,omitempty"`
	Audit      *audit.Report    `json:"audit"`
	Oracle     *Oracle          `json:"oracle"`
}

type run struct {
	sc    *Scenario
	opt   Options
	res   *Result
	pool  *pgxpool.Pool
	tcp   *faultproxy.TCPProxy
	proxy *faultproxy.HTTPProxy
	sim   *stripesim.Server
	snk   *sink
	apis  []*apiProc
	cl    *client

	loadStart time.Time
	evMu      sync.Mutex
	restarts  sync.WaitGroup
	done      atomic.Int64
}

func (r *run) logf(format string, args ...any) {
	if r.opt.Progress != nil {
		fmt.Fprintf(r.opt.Progress, "[%s] "+format+"\n", append([]any{r.sc.Name}, args...)...)
	}
}

func (r *run) event(format string, args ...any) {
	msg := fmt.Sprintf("t=%.1fs ", time.Since(r.loadStart).Seconds()) + fmt.Sprintf(format, args...)
	r.evMu.Lock()
	r.res.Faults.Events = append(r.res.Faults.Events, msg)
	r.evMu.Unlock()
	r.logf("event: %s", msg)
}

// Run executes one benchmark run end to end.
func Run(ctx context.Context, sc *Scenario, scRaw []byte, opt Options) (*Result, error) {
	scSum := sha256.Sum256(scRaw)
	res := &Result{RunID: time.Now().UTC().Format("20060102T150405Z") + "-" + randHex(3), Scenario: sc,
		ScenarioSHA256: hex.EncodeToString(scSum[:]), StartedAt: time.Now().UTC(),
		Outcomes: map[string]int{}, ResolvedBy: map[string]int64{}}
	res.Load.AttemptHistogram = map[string]int{}
	res.Load.AttemptStatuses = map[string]int{}
	res.Refunds.Outcomes = map[string]int{}
	r := &run{sc: sc, opt: opt, res: res}

	// ---- dataset
	txns, dsSHA, err := dataset.ReadNormalized(sc.Dataset)
	if err != nil {
		return nil, fmt.Errorf("dataset: %w (run `go run ./cmd/dataset fetch-ulb` first)", err)
	}
	if sc.Offset+sc.Rows > len(txns) {
		return nil, fmt.Errorf("dataset has %d rows, scenario needs offset %d + rows %d", len(txns), sc.Offset, sc.Rows)
	}
	window := txns[sc.Offset : sc.Offset+sc.Rows]
	res.Dataset.File, res.Dataset.SHA256, res.Dataset.Offset, res.Dataset.Rows = sc.Dataset, dsSHA, sc.Offset, len(window)
	for _, t := range window {
		res.Dataset.AmountTotalMinor += t.AmountMinor
		res.Dataset.FraudRows += t.Label
	}
	res.Dataset.SpanSeconds = window[len(window)-1].TimeS - window[0].TimeS

	// ---- database
	pool, dbURL, err := db.CreateDatabase(ctx, opt.AdminDBURL, "bench", 16)
	if err != nil {
		return nil, fmt.Errorf("create database: %w", err)
	}
	r.pool = pool
	dbName := pool.Config().ConnConfig.Database
	defer func() {
		pool.Close()
		if !opt.KeepDB {
			if err := db.DropDatabase(context.Background(), opt.AdminDBURL, dbName); err != nil {
				r.logf("drop database %s: %v", dbName, err)
			}
		} else {
			r.logf("kept database %s", dbName)
		}
	}()
	binSum, err := fileSHA256(opt.APIBin)
	if err != nil {
		return nil, fmt.Errorf("api binary: %w", err)
	}
	res.Environment = captureEnv(ctx, pool, binSum)

	logDir := filepath.Join(opt.LogDir, res.RunID)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, err
	}

	// ---- database link fault proxy
	u, _ := url.Parse(dbURL)
	r.tcp, err = faultproxy.NewTCP("127.0.0.1:0", u.Host, sc.Seed)
	if err != nil {
		return nil, err
	}
	defer r.tcp.Close()
	r.tcp.SetFaults(sc.DB.Faults)
	apiDB := *u
	apiDB.Host = r.tcp.Addr()
	q := apiDB.Query()
	q.Set("application_name", apiAppName) // lets the harness tell the API's backends from its own
	apiDB.RawQuery = q.Encode()

	// ---- outbox webhook sink
	const outboxSecret = "whsec_bench_outbox"
	r.snk = newSink(outboxSecret, sc.Seed, sc.OutboxSink.ErrorProb)
	sinkURL, closeSink, err := serve(r.snk)
	if err != nil {
		return nil, err
	}
	defer closeSink()

	// ---- processor (+ fault proxy) and webhook fan-in
	const stripeWebhookSecret = "whsec_bench_stripe"
	lb := &webhookLB{r: r}
	lbURL, closeLB, err := serve(lb)
	if err != nil {
		return nil, err
	}
	defer closeLB()
	var upstream, stripeKey string
	if sc.Processor.Kind == "sim" {
		simCfg := stripesim.Config{Seed: sc.Seed, InternalErrorProb: sc.Processor.InternalErrorProb}
		if sc.Processor.Webhooks {
			simCfg.WebhookURL = lbURL + "/v1/webhooks/stripe"
			simCfg.WebhookSecret = stripeWebhookSecret
			simCfg.WebhookDelay = time.Duration(sc.Processor.WebhookDelayMs) * time.Millisecond
			simCfg.WebhookDropProb = sc.Processor.WebhookDropProb
		}
		r.sim = stripesim.New(simCfg)
		defer r.sim.Close()
		simURL, closeSim, err := serve(r.sim.Handler())
		if err != nil {
			return nil, err
		}
		defer closeSim()
		upstream, stripeKey = simURL, "sk_test_bench_simulator"
	} else {
		upstream, stripeKey = stripe.DefaultBaseURL, os.Getenv("STRIPE_SECRET_KEY")
		if err := stripe.CheckKey(upstream, stripeKey); err != nil {
			return nil, fmt.Errorf("processor.kind=stripe needs STRIPE_SECRET_KEY=sk_test_...: %w", err)
		}
	}
	r.proxy, err = faultproxy.NewHTTP(upstream, sc.Seed, sc.Processor.Faults)
	if err != nil {
		return nil, err
	}
	proxyURL, closeProxy, err := serve(r.proxy)
	if err != nil {
		return nil, err
	}
	defer closeProxy()

	// ---- API instances
	apiKey := "bench_" + randHex(8)
	vars := map[string]string{
		"DATABASE_URL": apiDB.String(), "API_KEYS": apiKey, "MIGRATE_ON_START": "false",
		"STRIPE_API_BASE": proxyURL, "STRIPE_SECRET_KEY": stripeKey, "STRIPE_WEBHOOK_SECRET": stripeWebhookSecret,
		"OUTBOX_WEBHOOK_URL": sinkURL, "OUTBOX_WEBHOOK_SECRET": outboxSecret, "OUTBOX_CLAIM_FOR": "5s",
		"IDEMPOTENCY_MODE": sc.Server.IdempotencyMode, "LOG_LEVEL": "warn",
		"PROCESSOR_TIMEOUT": "2s", "PROCESSOR_INLINE_RETRIES": "1", "LEASE": "2s",
		"RECOVERY_INTERVAL": "250ms", "RECOVERY_BACKOFF_BASE": "200ms", "RECOVERY_BACKOFF_MAX": "3s",
		"REQUEST_BUDGET": "8s", "DB_MAX_CONNS": "32",
	}
	for k, v := range sc.Server.Env {
		vars[k] = v
	}
	for k, v := range opt.EnvOverrides {
		vars[k] = v
	}
	if len(opt.EnvOverrides) > 0 {
		res.EnvOverrides = opt.EnvOverrides
	}
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}
	for k, v := range vars {
		env = append(env, k+"="+v)
	}
	sort.Strings(env)
	for i := 0; i < sc.Server.Instances; i++ {
		a, err := newAPIProc(i, opt.APIBin, logDir, env)
		if err != nil {
			return nil, err
		}
		if err := a.start(); err != nil {
			return nil, err
		}
		r.apis = append(r.apis, a)
	}
	defer func() {
		for _, a := range r.apis {
			a.stop()
		}
	}()
	for _, a := range r.apis {
		if err := a.waitReady(ctx, 30*time.Second); err != nil {
			return nil, err
		}
	}

	transport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        8192,
		MaxIdleConnsPerHost: 4096,
		IdleConnTimeout:     90 * time.Second,
	}
	r.cl = &client{
		http:      &http.Client{Timeout: time.Duration(sc.Client.TimeoutMs) * time.Millisecond, Transport: transport},
		instances: r.apis, apiKey: apiKey, maxAttempts: sc.Client.MaxAttempts,
		base: time.Duration(sc.Client.BackoffBaseMs) * time.Millisecond, cap: time.Duration(sc.Client.BackoffMaxMs) * time.Millisecond,
	}

	// ---- merchants (through the API, idempotently)
	merchants := make([]string, sc.Merchants)
	for i := range merchants {
		rng := rand.New(rand.NewPCG(sc.Seed, uint64(i)))
		cr := r.cl.call(ctx, rng, "POST", "/v1/merchants", fmt.Sprintf("%s-merchant-%d", res.RunID, i),
			map[string]string{"name": fmt.Sprintf("bench merchant %d", i)}, i)
		if cr.final.status != 201 {
			return nil, fmt.Errorf("create merchant %d: status %d %s %v", i, cr.final.status, cr.final.body, cr.final.err)
		}
		merchants[i], _ = paymentFromBody(cr.final.body)
	}

	ops := r.buildOps(window, merchants)
	r.logf("run %s: %d ops, %d merchants, mode=%s concurrency=%d, idempotency=%s, instances=%d",
		res.RunID, len(ops), len(merchants), sc.Load.Mode, sc.Load.Concurrency, sc.Server.IdempotencyMode, len(r.apis))

	// ---- load phase with scheduled faults
	r.loadStart = time.Now()
	evCtx, stopEvents := context.WithCancel(ctx)
	evDone := make(chan struct{})
	go func() { r.scheduler(evCtx); close(evDone) }()
	progressDone := make(chan struct{})
	go r.progress(len(ops), progressDone)
	sampler := startDBSampler(ctx, pool)
	r.runLoad(ctx, ops)
	res.Load.DurationS = time.Since(r.loadStart).Seconds()
	sampler.stop()
	close(progressDone)
	stopEvents()
	<-evDone
	r.restarts.Wait()

	// ---- heal and drain
	r.proxy.SetFaults(faults.HTTP{})
	r.tcp.SetFaults(faults.TCP{})
	r.snk.setFaults(false)
	for _, a := range r.apis {
		if !a.running() {
			if err := a.start(); err != nil {
				return nil, err
			}
			if err := a.waitReady(ctx, 30*time.Second); err != nil {
				return nil, err
			}
		}
	}
	r.drain(ctx)
	// Backends hand their counters to pg_stat_database when they go idle
	// (after up to ~10 s) or exit, so stop the API processes, which closes
	// all their connections, and wait for the backends to leave before
	// reading the deadlock and transaction counters. Nothing below needs the
	// API.
	for _, a := range r.apis {
		a.stop()
	}
	waitForBackendsToExit(ctx, pool, 5*time.Second)
	res.Database = sampler.result(ctx)

	// ---- collect and check
	rep, err := audit.Run(ctx, pool)
	if err != nil {
		return nil, err
	}
	res.Audit = rep
	pis, refunds, err := r.processorRecords(ctx, stripeKey)
	if err != nil {
		return nil, fmt.Errorf("read processor records: %w", err)
	}
	_, delivered := r.snk.stats()
	res.Oracle, err = runOracle(ctx, pool, rep, pis, refunds, ops, delivered)
	if err != nil {
		return nil, err
	}
	if err := r.resolvedBy(ctx); err != nil {
		return nil, err
	}
	r.summarize(ops)
	if err := r.gaveUpStats(ctx, ops); err != nil {
		return nil, err
	}
	res.FinishedAt = time.Now().UTC()
	return res, nil
}

func (r *run) buildOps(window []dataset.Txn, merchants []string) []*opRecord {
	sc := r.sc
	cdf := zipfCDF(len(merchants), sc.MerchantZipfS)
	scale := 1.0
	if sc.Load.Mode == "open" {
		span := window[len(window)-1].TimeS - window[0].TimeS + 1
		scale = sc.Load.TargetRate / (float64(len(window)) / span)
		r.res.Dataset.TimeScale = scale
	}
	ops := make([]*opRecord, len(window))
	for i, t := range window {
		row := strconv.Itoa(t.Row)
		var m int
		if t.MerchantKey != "" {
			m = int(faults.Uniform(sc.Seed, "merchant_key", t.MerchantKey) * float64(len(merchants)))
		} else {
			u := faults.Uniform(sc.Seed, "merchant", row)
			m = sort.SearchFloat64s(cdf, u)
			if m >= len(merchants) {
				m = len(merchants) - 1
			}
		}
		m = hotMerchant(sc, row, m, len(merchants))
		pm := "pm_card_visa"
		if t.Label == 1 {
			pm = sc.FraudPaymentMethod
		}
		// Dataset timestamps have one-second resolution; spread arrivals
		// deterministically within each second.
		at := (t.TimeS - window[0].TimeS + faults.Uniform(sc.Seed, "arrival", row)) / scale
		if b := sc.Load.Burst; b != nil && sc.Load.Mode == "open" {
			at = burstTime(at, b.PeriodS, b.Duty)
		}
		// The key depends only on the dataset row, so fault decisions (keyed
		// on it) are identical across runs; each run has a fresh database.
		ops[i] = &opRecord{Row: t.Row, Key: "row-" + row, Merchant: merchants[m], Amount: t.AmountMinor,
			Currency: t.Currency, PM: pm, WantRefund: faults.Uniform(sc.Seed, "refund", row) < sc.RefundFraction,
			Scheduled: time.Duration(at * float64(time.Second))}
	}
	if sc.Load.Mode == "open" {
		sort.SliceStable(ops, func(i, j int) bool { return ops[i].Scheduled < ops[j].Scheduled })
	}
	return ops
}

// burstTime compresses the arrivals of each period into the period's first
// duty fraction: an arrival at offset x within its period lands at x*duty. The
// mean arrival rate is unchanged, and the rate during a burst is the mean
// divided by duty.
func burstTime(t, period, duty float64) float64 {
	k := math.Floor(t / period)
	return k*period + (t-k*period)*duty
}

// hotMerchant applies the scenario's hot-merchant share to the merchant m
// chosen by the ordinary mix: merchant 0 receives HotMerchantFraction of the
// operations and the others keep the mix, with merchant 0's own share spread
// uniformly over them.
func hotMerchant(sc *Scenario, row string, m, n int) int {
	if sc.HotMerchantFraction <= 0 || n < 2 {
		return m
	}
	if faults.Uniform(sc.Seed, "hot_merchant", row) < sc.HotMerchantFraction {
		return 0
	}
	if m == 0 {
		return 1 + int(faults.Uniform(sc.Seed, "hot_merchant_other", row)*float64(n-1))%(n-1)
	}
	return m
}

// zipfCDF returns the cumulative distribution of a Zipf(s) law over n
// merchants (s = 0: uniform). Real merchant volumes are heavily skewed.
func zipfCDF(n int, s float64) []float64 {
	w := make([]float64, n)
	var total float64
	for k := range w {
		w[k] = 1 / math.Pow(float64(k+1), s)
		total += w[k]
	}
	cdf := make([]float64, n)
	var acc float64
	for k := range w {
		acc += w[k] / total
		cdf[k] = acc
	}
	return cdf
}

func (r *run) runLoad(ctx context.Context, ops []*opRecord) {
	c := r.sc.Load.Concurrency
	var wg sync.WaitGroup
	if r.sc.Load.Mode == "closed" {
		ch := make(chan *opRecord)
		for w := 0; w < c; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for op := range ch {
					r.doOp(ctx, op, time.Now())
				}
			}()
		}
		for _, op := range ops {
			ch <- op
		}
		close(ch)
		wg.Wait()
		return
	}
	// Open loop: arrivals follow the (time-scaled) dataset regardless of how
	// fast the system responds. Latency is measured from the scheduled
	// arrival, so queueing behind a saturated client counts (no coordinated
	// omission).
	sem := make(chan struct{}, c)
	for _, op := range ops {
		at := r.loadStart.Add(op.Scheduled)
		if d := time.Until(at); d > 0 {
			time.Sleep(d)
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(op *opRecord) {
			defer wg.Done()
			defer func() { <-sem }()
			r.doOp(ctx, op, at)
		}(op)
	}
	wg.Wait()
}

func (r *run) doOp(ctx context.Context, op *opRecord, t0 time.Time) {
	defer r.done.Add(1)
	rng := rand.New(rand.NewPCG(r.sc.Seed, uint64(op.Row)))
	body := map[string]any{
		"merchant_id": op.Merchant, "amount": op.Amount, "currency": op.Currency, "payment_method": op.PM,
		"metadata": map[string]string{"client_ref": op.Key, "bench_run": r.res.RunID, "dataset_row": strconv.Itoa(op.Row)},
	}
	var dupDone chan callResult
	if f := r.sc.Load.DuplicateFraction; f > 0 && faults.Uniform(r.sc.Seed, "duplicate", strconv.Itoa(op.Row)) < f {
		dupDone = make(chan callResult, 1)
		go func() {
			dupRng := rand.New(rand.NewPCG(r.sc.Seed^0xd0b1e, uint64(op.Row)))
			dupDone <- r.cl.call(ctx, dupRng, "POST", "/v1/payments", op.Key, body, op.Row+1)
		}()
	}
	op.Res = r.cl.call(ctx, rng, "POST", "/v1/payments", op.Key, body, op.Row)
	op.LatencyMs = msSince(t0)
	if dupDone != nil {
		d := <-dupDone
		op.Dup = &d
		op.DupOutcome, op.DupPaymentID = classify(d)
	}
	op.Outcome, op.PaymentID = classify(op.Res)
	if op.Outcome != "succeeded" || !op.WantRefund {
		return
	}
	t1 := time.Now()
	rr := r.cl.call(ctx, rng, "POST", "/v1/payments/"+op.PaymentID+"/refunds", op.Key+"-refund", map[string]any{}, op.Row+1)
	op.RefundRes = &rr
	op.RefundLatency = msSince(t1)
	op.RefundOutcome, _ = classify(rr)
}

func classify(c callResult) (outcome, id string) {
	switch {
	case c.gaveUp:
		return "gave_up", ""
	case c.final.status == 201:
		id, st := paymentFromBody(c.final.body)
		return st, id
	case c.final.status == 202:
		id, _ := paymentFromBody(c.final.body)
		return "needs_review", id
	default:
		return "rejected", ""
	}
}

func (r *run) scheduler(ctx context.Context) {
	var wg sync.WaitGroup
	for _, ev := range r.sc.Events {
		wg.Add(1)
		go func(ev Event) {
			defer wg.Done()
			t := ev.AtS
			for n := 0; ; n++ {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Until(r.loadStart.Add(time.Duration(t * float64(time.Second))))):
				}
				r.fire(ev, n)
				if ev.EveryS <= 0 {
					return
				}
				t += ev.EveryS
			}
		}(ev)
	}
	wg.Wait()
}

func (r *run) fire(ev Event, n int) {
	dur := time.Duration(ev.DurationS * float64(time.Second))
	switch ev.Type {
	case "db_blackout":
		r.tcp.Blackout(dur)
		r.event("database blackout for %s", dur)
	case "processor_faults":
		r.proxy.SetFaults(*ev.Faults)
		r.event("processor faults set to %+v", *ev.Faults)
	case "db_faults":
		r.tcp.SetFaults(*ev.DBFaults)
		r.event("database faults set to %+v", *ev.DBFaults)
	case "api_crash":
		idx := ev.Instance
		if idx < 0 {
			idx = n % len(r.apis)
		}
		a := r.apis[idx%len(r.apis)]
		pid := a.kill()
		r.res.Faults.APICrashes++
		r.event("SIGKILL api instance %d (pid %d); restart in %s", a.idx, pid, dur)
		r.restarts.Add(1)
		go func() {
			defer r.restarts.Done()
			time.Sleep(dur)
			for try := 1; try <= 5; try++ {
				err := a.start()
				if err == nil {
					if err = a.waitReady(context.Background(), 30*time.Second); err == nil {
						return
					}
				}
				r.logf("restart api %d (try %d): %v", a.idx, try, err)
				time.Sleep(500 * time.Millisecond)
			}
		}()
	}
}

func (r *run) progress(total int, done chan struct{}) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			n := r.done.Load()
			r.logf("progress %d/%d (%.0f ops/s)", n, total, float64(n)/time.Since(r.loadStart).Seconds())
		}
	}
}

// drain waits until recovery has resolved everything and the outbox is
// empty, with all faults healed. Its duration is the time to consistency.
func (r *run) drain(ctx context.Context) {
	start := time.Now()
	deadline := start.Add(time.Duration(r.sc.DrainTimeoutS) * time.Second)
	q := `SELECT
		(SELECT count(*) FROM payments WHERE status = 'processing'),
		(SELECT count(*) FROM refunds WHERE status = 'processing'),
		(SELECT count(*) FROM outbox_events WHERE delivered_at IS NULL)`
	var pay, ref, out int64
	for {
		if err := r.pool.QueryRow(ctx, q).Scan(&pay, &ref, &out); err == nil && pay+ref+out == 0 {
			break
		}
		if time.Now().After(deadline) {
			r.res.Drain.TimedOut = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	r.res.Drain.DurationS = time.Since(start).Seconds()
	r.res.Drain.Remaining = map[string]int64{"payments_processing": pay, "refunds_processing": ref, "outbox_undelivered": out}
	r.logf("drain: %.2fs (remaining %v, timed out %v)", r.res.Drain.DurationS, r.res.Drain.Remaining, r.res.Drain.TimedOut)
}

func (r *run) processorRecords(ctx context.Context, key string) ([]procPI, []procRefund, error) {
	var pis []procPI
	var refunds []procRefund
	if r.sim != nil {
		st := r.sim.Snapshot()
		r.res.Faults.Simulator = &st.Stats
		for _, pi := range st.PaymentIntents {
			pis = append(pis, procPI{ID: pi.ID, Status: pi.Status, Amount: pi.Amount, Currency: pi.Currency, Metadata: pi.Metadata})
		}
		for _, rf := range st.Refunds {
			refunds = append(refunds, procRefund{ID: rf.ID, PaymentIntent: rf.PaymentIntent, Status: rf.Status, Amount: rf.Amount, Currency: rf.Currency, Metadata: rf.Metadata})
		}
		return pis, refunds, nil
	}
	// Real Stripe: list everything created since the run started, keep this
	// run's objects (metadata bench_run), and list their refunds.
	sc, err := stripe.New(stripe.DefaultBaseURL, key, &http.Client{Timeout: 30 * time.Second})
	if err != nil {
		return nil, nil, err
	}
	err = sc.ListPaymentIntents(ctx, r.res.StartedAt.Add(-time.Minute), func(pi stripe.PaymentIntent) error {
		if pi.Metadata["bench_run"] == r.res.RunID {
			pis = append(pis, procPI{ID: pi.ID, Status: pi.Status, Amount: pi.Amount, Currency: pi.Currency, Metadata: pi.Metadata})
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	rows, err := r.pool.Query(ctx, `SELECT DISTINCT p.processor_id FROM refunds r JOIN payments p ON p.id = r.payment_id WHERE p.processor_id IS NOT NULL`)
	if err != nil {
		return nil, nil, err
	}
	var piIDs []string
	for rows.Next() {
		var id string
		rows.Scan(&id) //nolint:errcheck
		piIDs = append(piIDs, id)
	}
	rows.Close()
	for _, id := range piIDs {
		list, err := sc.ListRefunds(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		for _, rf := range list {
			refunds = append(refunds, procRefund{ID: rf.ID, PaymentIntent: rf.PaymentIntent, Status: rf.Status, Amount: rf.Amount, Currency: rf.Currency, Metadata: rf.Metadata})
		}
	}
	return pis, refunds, nil
}

func (r *run) resolvedBy(ctx context.Context) error {
	rows, err := r.pool.Query(ctx, `
		SELECT 'payment:' || status || ':' || coalesce(resolved_by, '-'), count(*) FROM payments GROUP BY 1
		UNION ALL
		SELECT 'refund:' || status || ':' || coalesce(resolved_by, '-'), count(*) FROM refunds GROUP BY 1`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var n int64
		if err := rows.Scan(&k, &n); err != nil {
			return err
		}
		r.res.ResolvedBy[k] = n
	}
	return rows.Err()
}

func (r *run) summarize(ops []*opRecord) {
	res := r.res
	res.Load.Ops = len(ops)
	res.Load.ThroughputOpsPerS = float64(len(ops)) / res.Load.DurationS
	var succ, all, per, refLat []float64
	for _, op := range ops {
		res.Outcomes[op.Outcome]++
		all = append(all, op.LatencyMs)
		if op.Outcome == "succeeded" {
			succ = append(succ, op.LatencyMs)
		}
		if op.Dup != nil {
			res.Load.DuplicateRequests++
		}
		res.Load.Attempts += len(op.Res.attempts)
		res.Load.Retries += max(0, len(op.Res.attempts)-1)
		res.Load.AttemptHistogram[strconv.Itoa(len(op.Res.attempts))]++
		for _, a := range op.Res.attempts {
			per = append(per, a.latencyMs)
			k := strconv.Itoa(a.status)
			if a.status == 0 {
				k = "transport_error"
				if a.err != nil && strings.Contains(a.err.Error(), "Client.Timeout") {
					k = "client_timeout"
				}
			}
			res.Load.AttemptStatuses[k]++
		}
		if op.RefundRes != nil {
			res.Refunds.Requested++
			res.Refunds.Outcomes[op.RefundOutcome]++
			refLat = append(refLat, op.RefundLatency)
		}
	}
	if n := len(ops); n > 0 {
		bad := res.Outcomes["gave_up"] + res.Outcomes["rejected"] + res.Outcomes["needs_review"]
		res.Load.FinalErrorRate = float64(bad) / float64(n)
	}
	if res.Load.Attempts > 0 {
		bad := res.Load.AttemptStatuses["transport_error"] + res.Load.AttemptStatuses["client_timeout"]
		for k, v := range res.Load.AttemptStatuses {
			if len(k) == 3 && k[0] == '5' {
				bad += v
			}
		}
		res.Load.AttemptErrorRate = float64(bad) / float64(res.Load.Attempts)
	}
	res.LatencyMs.Succeeded = quantiles(succ)
	res.LatencyMs.AllFinal = quantiles(all)
	res.LatencyMs.SingleAttmpt = quantiles(per)
	res.Refunds.LatencyMs = quantiles(refLat)
	res.Faults.Processor = r.proxy.Stats()
	res.Faults.Database = r.tcp.Stats()
	res.Faults.Sink, _ = r.snk.stats()
}

// webhookLB forwards simulator webhooks to whichever API instance is up.
type webhookLB struct{ r *run }

func (l *webhookLB) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(req.Body)
	c := &http.Client{Timeout: 5 * time.Second}
	for _, a := range l.r.apis {
		out, _ := http.NewRequest(http.MethodPost, a.URL()+req.URL.Path, strings.NewReader(string(body)))
		out.Header = req.Header.Clone()
		resp, err := c.Do(out)
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		w.Write(b) //nolint:errcheck
		return
	}
	w.WriteHeader(http.StatusBadGateway)
}

func serve(h http.Handler) (string, func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
	go srv.Serve(ln) //nolint:errcheck
	return "http://" + ln.Addr().String(), func() { srv.Close() }, nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func randHex(n int) string {
	b := make([]byte, n)
	crand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}

// WriteResult stores the result as JSON under dir/<scenario>/<run_id>.json.
func WriteResult(dir string, res *Result) (string, error) {
	d := filepath.Join(dir, res.Scenario.Name)
	if err := os.MkdirAll(d, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(d, res.RunID+".json")
	b, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return "", err
	}
	return path, os.WriteFile(path, append(b, '\n'), 0o644)
}
