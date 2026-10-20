// Package stripesim is an in-memory server that speaks the subset of the
// Stripe API used by this project, including Stripe's idempotency-key
// semantics and signed webhooks. It lets the benchmark run tens of
// thousands of payments offline and lets the oracle compare the ledger with
// the processor's own records.
//
// It is an emulator, not Stripe: response shapes follow Stripe's documented
// objects, but decline codes for some test PaymentMethods are
// approximations, and there is no rate limiting unless injected.
package stripesim

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"paymentledger/internal/faults"
	"paymentledger/internal/stripe"
)

type Config struct {
	Seed uint64
	// InternalErrorProb: probability that executing a create request ends
	// in a 500 *after* the side effect happened. As on Stripe, the 500 is
	// stored under the idempotency key, so retries keep getting it and the
	// client must look the object up instead.
	InternalErrorProb float64

	WebhookURL      string
	WebhookSecret   string
	WebhookDelay    time.Duration
	WebhookDropProb float64 // events never delivered
}

type Server struct {
	cfg Config

	mu      sync.Mutex
	pis     map[string]*stripe.PaymentIntent
	piOrder []string
	piKeys  map[string]string // pi id -> idempotency key used to create it
	refunds map[string]*stripe.Refund
	rfOrder []string
	idem    map[string]*idemEntry
	execs   map[string]int // executions per fault key

	webhooks chan []byte
	stats    Stats
	closed   chan struct{}
}

type idemEntry struct {
	fingerprint string
	done        bool
	status      int
	body        []byte
}

type Stats struct {
	Requests          int64 `json:"requests"`
	IdempotentReplays int64 `json:"idempotent_replays"`
	IdempotencyErrors int64 `json:"idempotency_errors"`
	InternalErrors    int64 `json:"internal_errors"`
	WebhooksSent      int64 `json:"webhooks_sent"`
	WebhooksDropped   int64 `json:"webhooks_dropped"`
	WebhooksFailed    int64 `json:"webhooks_failed"`
}

func New(cfg Config) *Server {
	s := &Server{
		cfg: cfg, pis: map[string]*stripe.PaymentIntent{}, piKeys: map[string]string{},
		refunds: map[string]*stripe.Refund{}, idem: map[string]*idemEntry{}, execs: map[string]int{},
		webhooks: make(chan []byte, 100_000), closed: make(chan struct{}),
	}
	if cfg.WebhookURL != "" {
		for i := 0; i < 16; i++ {
			go s.webhookWorker()
		}
	}
	return s
}

func (s *Server) Close() { close(s.closed) }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payment_intents", s.auth(s.createPaymentIntent))
	mux.HandleFunc("GET /v1/payment_intents", s.auth(s.listPaymentIntents))
	mux.HandleFunc("GET /v1/payment_intents/search", s.auth(s.searchPaymentIntents))
	mux.HandleFunc("GET /v1/payment_intents/{id}", s.auth(s.getPaymentIntent))
	mux.HandleFunc("POST /v1/refunds", s.auth(s.createRefund))
	mux.HandleFunc("GET /v1/refunds", s.auth(s.listRefunds))
	mux.HandleFunc("GET /v1/refunds/{id}", s.auth(s.getRefund))
	mux.HandleFunc("GET /_sim/state", s.state)
	return mux
}

func (s *Server) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Request-Id", "req_"+randHex(12))
		tok, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !strings.HasPrefix(tok, "sk_test_") {
			writeErr(w, 401, stripe.ErrorBody{Type: "invalid_request_error", Message: "Invalid API Key provided"})
			return
		}
		s.mu.Lock()
		s.stats.Requests++
		s.mu.Unlock()
		h(w, r)
	}
}

// --------------------------------------------------------------- idempotency

// idempotent runs exec at most once per Idempotency-Key, replaying the
// stored response for later requests with the same key and parameters.
// exec returns (status, body, cache); validation errors are not cached.
func (s *Server) idempotent(w http.ResponseWriter, r *http.Request, form url.Values, exec func(faultKey string, execIndex int) (int, []byte, bool)) {
	key := r.Header.Get("Idempotency-Key")
	fp := r.Method + " " + r.URL.Path + " " + form.Encode() // Encode sorts keys
	faultKey := key
	if ref := form.Get("metadata[client_ref]"); ref != "" {
		faultKey = r.URL.Path + "|" + ref // stable across runs, unlike ids
	}
	if key == "" {
		s.mu.Lock()
		n := s.execs[faultKey]
		s.execs[faultKey] = n + 1
		s.mu.Unlock()
		status, body, _ := exec(faultKey, n)
		writeRaw(w, status, body)
		return
	}

	s.mu.Lock()
	e, ok := s.idem[key]
	switch {
	case ok && e.fingerprint != fp:
		s.stats.IdempotencyErrors++
		s.mu.Unlock()
		writeErr(w, 400, stripe.ErrorBody{Type: "idempotency_error",
			Message: "Keys for idempotent requests can only be used with the same parameters they were first used with."})
		return
	case ok && !e.done:
		s.stats.IdempotencyErrors++
		s.mu.Unlock()
		writeErr(w, 409, stripe.ErrorBody{Type: "idempotency_error", Code: "idempotency_key_in_use",
			Message: "There is currently another in-progress request using this Idempotent Key."})
		return
	case ok:
		s.stats.IdempotentReplays++
		status, body := e.status, e.body
		s.mu.Unlock()
		w.Header().Set("Idempotent-Replayed", "true")
		writeRaw(w, status, body)
		return
	}
	e = &idemEntry{fingerprint: fp}
	s.idem[key] = e
	n := s.execs[faultKey]
	s.execs[faultKey] = n + 1
	s.mu.Unlock()

	status, body, cache := exec(faultKey, n)

	s.mu.Lock()
	if cache {
		e.done, e.status, e.body = true, status, body
	} else {
		delete(s.idem, key)
	}
	s.mu.Unlock()
	writeRaw(w, status, body)
}

// maybeInternalError decides whether this execution "fails after acting".
func (s *Server) maybeInternalError(faultKey string, n int) bool {
	if s.cfg.InternalErrorProb <= 0 {
		return false
	}
	if faults.Uniform(s.cfg.Seed, "sim500", faultKey, strconv.Itoa(n)) < s.cfg.InternalErrorProb {
		s.mu.Lock()
		s.stats.InternalErrors++
		s.mu.Unlock()
		return true
	}
	return false
}

var internalErrorBody = mustJSON(map[string]stripe.ErrorBody{"error": {Type: "api_error",
	Message: "An unknown error occurred (simulated)."}})

// ------------------------------------------------------------ PaymentIntents

type pmBehavior struct {
	ok          bool
	code        string
	declineCode string
	message     string
	requires3DS bool
}

var paymentMethods = map[string]pmBehavior{
	"pm_card_visa":           {ok: true},
	"pm_card_visa_debit":     {ok: true},
	"pm_card_mastercard":     {ok: true},
	"pm_card_amex":           {ok: true},
	"pm_card_discover":       {ok: true},
	"pm_card_us":             {ok: true},
	"pm_card_chargeDeclined": {code: "card_declined", declineCode: "generic_decline", message: "Your card was declined."},

	"pm_card_chargeDeclinedInsufficientFunds": {code: "card_declined", declineCode: "insufficient_funds", message: "Your card has insufficient funds."},
	"pm_card_chargeDeclinedFraudulent":        {code: "card_declined", declineCode: "fraudulent", message: "Your card was declined."},
	"pm_card_chargeDeclinedExpiredCard":       {code: "expired_card", message: "Your card has expired."},
	"pm_card_radarBlock":                      {code: "card_declined", declineCode: "fraudulent", message: "Your card was declined."},
	"pm_card_authenticationRequired":          {requires3DS: true},
}

var metadataRe = regexp.MustCompile(`^metadata\[(.+)\]$`)

func metadataOf(form url.Values) map[string]string {
	md := map[string]string{}
	for k, v := range form {
		if m := metadataRe.FindStringSubmatch(k); m != nil && len(v) > 0 {
			md[m[1]] = v[0]
		}
	}
	return md
}

func (s *Server) createPaymentIntent(w http.ResponseWriter, r *http.Request) {
	form, ok := parseForm(w, r)
	if !ok {
		return
	}
	s.idempotent(w, r, form, func(faultKey string, n int) (int, []byte, bool) {
		amount, err := strconv.ParseInt(form.Get("amount"), 10, 64)
		if err != nil || amount <= 0 {
			return 400, errJSON(stripe.ErrorBody{Type: "invalid_request_error", Code: "parameter_invalid_integer", Param: "amount", Message: "Invalid integer"}), false
		}
		if amount < 50 {
			return 400, errJSON(stripe.ErrorBody{Type: "invalid_request_error", Code: "amount_too_small", Param: "amount",
				Message: "Amount must be at least 50 cents"}), false
		}
		cur := strings.ToLower(form.Get("currency"))
		if len(cur) != 3 {
			return 400, errJSON(stripe.ErrorBody{Type: "invalid_request_error", Code: "parameter_invalid_string", Param: "currency", Message: "Invalid currency"}), false
		}
		pmID := form.Get("payment_method")
		pm, known := paymentMethods[pmID]
		if !known {
			return 400, errJSON(stripe.ErrorBody{Type: "invalid_request_error", Code: "resource_missing", Param: "payment_method",
				Message: "No such PaymentMethod: '" + pmID + "'"}), false
		}

		pi := &stripe.PaymentIntent{
			ID: "pi_" + randHex(12), Object: "payment_intent", Amount: amount, Currency: cur,
			Created: time.Now().Unix(), Description: form.Get("description"), PaymentMethod: pmID,
			Metadata: metadataOf(form),
		}
		status, body := 200, []byte(nil)
		switch {
		case pm.ok:
			pi.Status = "succeeded"
			pi.LatestCharge = "ch_" + randHex(12)
		case pm.requires3DS:
			pi.Status = "requires_action"
		default:
			pi.Status = "requires_payment_method"
			pi.LastPaymentError = &stripe.ErrorBody{Type: "card_error", Code: pm.code, DeclineCode: pm.declineCode, Message: pm.message}
			status = 402
		}

		s.mu.Lock()
		s.pis[pi.ID] = pi
		s.piOrder = append(s.piOrder, pi.ID)
		s.piKeys[pi.ID] = r.Header.Get("Idempotency-Key")
		snapshot := *pi
		s.mu.Unlock()

		if status == 402 {
			body = errJSON(stripe.ErrorBody{Type: "card_error", Code: pm.code, DeclineCode: pm.declineCode, Message: pm.message, PaymentIntent: &snapshot})
		} else {
			body = mustJSON(snapshot)
		}
		switch pi.Status {
		case "succeeded":
			s.emit("payment_intent.succeeded", snapshot)
		case "requires_payment_method":
			s.emit("payment_intent.payment_failed", snapshot)
		}
		if s.maybeInternalError(faultKey, n) {
			return 500, internalErrorBody, true
		}
		return status, body, true
	})
}

func (s *Server) getPaymentIntent(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	pi, ok := s.pis[r.PathValue("id")]
	var snap stripe.PaymentIntent
	if ok {
		snap = *pi
	}
	s.mu.Unlock()
	if !ok {
		writeErr(w, 404, stripe.ErrorBody{Type: "invalid_request_error", Code: "resource_missing", Message: "No such payment_intent"})
		return
	}
	writeRaw(w, 200, mustJSON(snap))
}

var searchRe = regexp.MustCompile(`^metadata\['([^']+)'\]:'([^']*)'$`)

func (s *Server) searchPaymentIntents(w http.ResponseWriter, r *http.Request) {
	m := searchRe.FindStringSubmatch(r.URL.Query().Get("query"))
	if m == nil {
		writeErr(w, 400, stripe.ErrorBody{Type: "invalid_request_error", Message: "simulator supports only metadata['k']:'v' queries"})
		return
	}
	var out []stripe.PaymentIntent
	s.mu.Lock()
	for _, id := range s.piOrder {
		if pi := s.pis[id]; pi.Metadata[m[1]] == m[2] {
			out = append(out, *pi)
		}
	}
	s.mu.Unlock()
	writeRaw(w, 200, mustJSON(map[string]any{"object": "search_result", "data": nonNil(out), "has_more": false}))
}

func (s *Server) listPaymentIntents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 10
	}
	gte, _ := strconv.ParseInt(q.Get("created[gte]"), 10, 64)
	after := q.Get("starting_after")
	var out []stripe.PaymentIntent
	hasMore := false
	s.mu.Lock()
	// Stripe lists newest first.
	started := after == ""
	for i := len(s.piOrder) - 1; i >= 0; i-- {
		pi := s.pis[s.piOrder[i]]
		if !started {
			started = pi.ID == after
			continue
		}
		if pi.Created < gte {
			continue
		}
		if len(out) == limit {
			hasMore = true
			break
		}
		out = append(out, *pi)
	}
	s.mu.Unlock()
	writeRaw(w, 200, mustJSON(map[string]any{"object": "list", "data": nonNil(out), "has_more": hasMore}))
}

// ------------------------------------------------------------------- Refunds

func (s *Server) createRefund(w http.ResponseWriter, r *http.Request) {
	form, ok := parseForm(w, r)
	if !ok {
		return
	}
	s.idempotent(w, r, form, func(faultKey string, n int) (int, []byte, bool) {
		s.mu.Lock()
		pi, ok := s.pis[form.Get("payment_intent")]
		if !ok {
			s.mu.Unlock()
			return 400, errJSON(stripe.ErrorBody{Type: "invalid_request_error", Code: "resource_missing", Param: "payment_intent", Message: "No such payment_intent"}), false
		}
		if pi.Status != "succeeded" {
			s.mu.Unlock()
			return 400, errJSON(stripe.ErrorBody{Type: "invalid_request_error", Code: "charge_not_refundable", Message: "PaymentIntent has not succeeded"}), false
		}
		var refunded int64
		for _, id := range s.rfOrder {
			if rf := s.refunds[id]; rf.PaymentIntent == pi.ID && rf.Status == "succeeded" {
				refunded += rf.Amount
			}
		}
		amount := pi.Amount - refunded
		if v := form.Get("amount"); v != "" {
			amount, _ = strconv.ParseInt(v, 10, 64)
		}
		if amount <= 0 || amount > pi.Amount-refunded {
			s.mu.Unlock()
			return 400, errJSON(stripe.ErrorBody{Type: "invalid_request_error", Code: "amount_too_large", Param: "amount",
				Message: fmt.Sprintf("Refund amount (%d) is greater than unrefunded amount on charge (%d)", amount, pi.Amount-refunded)}), false
		}
		rf := &stripe.Refund{ID: "re_" + randHex(12), Object: "refund", Amount: amount, Currency: pi.Currency,
			Status: "succeeded", PaymentIntent: pi.ID, Created: time.Now().Unix(), Metadata: metadataOf(form)}
		s.refunds[rf.ID] = rf
		s.rfOrder = append(s.rfOrder, rf.ID)
		snap := *rf
		s.mu.Unlock()
		s.emit("refund.created", snap)
		if s.maybeInternalError(faultKey, n) {
			return 500, internalErrorBody, true
		}
		return 200, mustJSON(snap), true
	})
}

func (s *Server) getRefund(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	rf, ok := s.refunds[r.PathValue("id")]
	var snap stripe.Refund
	if ok {
		snap = *rf
	}
	s.mu.Unlock()
	if !ok {
		writeErr(w, 404, stripe.ErrorBody{Type: "invalid_request_error", Code: "resource_missing", Message: "No such refund"})
		return
	}
	writeRaw(w, 200, mustJSON(snap))
}

func (s *Server) listRefunds(w http.ResponseWriter, r *http.Request) {
	piID := r.URL.Query().Get("payment_intent")
	var out []stripe.Refund
	s.mu.Lock()
	for i := len(s.rfOrder) - 1; i >= 0; i-- {
		if rf := s.refunds[s.rfOrder[i]]; piID == "" || rf.PaymentIntent == piID {
			out = append(out, *rf)
		}
	}
	s.mu.Unlock()
	writeRaw(w, 200, mustJSON(map[string]any{"object": "list", "data": nonNil(out), "has_more": false}))
}

// ------------------------------------------------------------------ webhooks

func (s *Server) emit(typ string, obj any) {
	if s.cfg.WebhookURL == "" {
		return
	}
	raw, _ := json.Marshal(obj)
	ev := map[string]any{"id": "evt_" + randHex(12), "object": "event", "type": typ, "created": time.Now().Unix(),
		"livemode": false, "api_version": stripe.APIVersion, "data": map[string]json.RawMessage{"object": raw}}
	b, _ := json.Marshal(ev)
	select {
	case s.webhooks <- b:
	default:
		s.mu.Lock()
		s.stats.WebhooksDropped++
		s.mu.Unlock()
	}
}

var webhookTransport = func() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = 32
	return t
}()

func (s *Server) webhookWorker() {
	client := &http.Client{Timeout: 5 * time.Second, Transport: webhookTransport}
	for {
		var body []byte
		select {
		case <-s.closed:
			return
		case body = <-s.webhooks:
		}
		var ev struct{ ID string }
		json.Unmarshal(body, &ev) //nolint:errcheck
		if faults.Uniform(s.cfg.Seed, "webhook_drop", ev.ID) < s.cfg.WebhookDropProb {
			s.mu.Lock()
			s.stats.WebhooksDropped++
			s.mu.Unlock()
			continue
		}
		if s.cfg.WebhookDelay > 0 {
			time.Sleep(s.cfg.WebhookDelay)
		}
		delivered := false
		for attempt := 0; attempt < 6 && !delivered; attempt++ {
			if attempt > 0 {
				select {
				case <-s.closed:
					return
				case <-time.After(time.Duration(math.Pow(2, float64(attempt-1))) * 500 * time.Millisecond):
				}
			}
			req, _ := http.NewRequest(http.MethodPost, s.cfg.WebhookURL, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Stripe-Signature", stripe.SignatureHeader(body, s.cfg.WebhookSecret, time.Now()))
			resp, err := client.Do(req)
			if err == nil {
				io.Copy(io.Discard, resp.Body) //nolint:errcheck
				resp.Body.Close()
				delivered = resp.StatusCode >= 200 && resp.StatusCode < 300
			}
		}
		s.mu.Lock()
		if delivered {
			s.stats.WebhooksSent++
		} else {
			s.stats.WebhooksFailed++
		}
		s.mu.Unlock()
	}
}

// --------------------------------------------------------------------- state

// PaymentIntentRecord is a PaymentIntent plus the key that created it.
type PaymentIntentRecord struct {
	stripe.PaymentIntent
	IdempotencyKey string `json:"idempotency_key"`
}

type State struct {
	PaymentIntents []PaymentIntentRecord `json:"payment_intents"`
	Refunds        []stripe.Refund       `json:"refunds"`
	Stats          Stats                 `json:"stats"`
}

func (s *Server) Snapshot() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := State{Stats: s.stats}
	for _, id := range s.piOrder {
		st.PaymentIntents = append(st.PaymentIntents, PaymentIntentRecord{PaymentIntent: *s.pis[id], IdempotencyKey: s.piKeys[id]})
	}
	for _, id := range s.rfOrder {
		st.Refunds = append(st.Refunds, *s.refunds[id])
	}
	sort.SliceStable(st.Refunds, func(i, j int) bool { return st.Refunds[i].Created < st.Refunds[j].Created })
	return st
}

func (s *Server) state(w http.ResponseWriter, _ *http.Request) {
	writeRaw(w, 200, mustJSON(s.Snapshot()))
}

// ------------------------------------------------------------------- helpers

func parseForm(w http.ResponseWriter, r *http.Request) (url.Values, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, 400, stripe.ErrorBody{Type: "invalid_request_error", Message: "unreadable body"})
		return nil, false
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		writeErr(w, 400, stripe.ErrorBody{Type: "invalid_request_error", Message: "invalid form body"})
		return nil, false
	}
	return form, true
}

func writeRaw(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body) //nolint:errcheck
}

func writeErr(w http.ResponseWriter, status int, e stripe.ErrorBody) { writeRaw(w, status, errJSON(e)) }

func errJSON(e stripe.ErrorBody) []byte { return mustJSON(map[string]stripe.ErrorBody{"error": e}) }

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}
