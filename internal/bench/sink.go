package bench

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"paymentledger/internal/faults"
	"paymentledger/internal/outbox"
	"paymentledger/internal/stripe"
)

// sink receives the API's outbox webhooks, verifying signatures and
// counting deliveries per event id (at-least-once ⇒ duplicates allowed).
type sink struct {
	secret    string
	seed      uint64
	errorProb float64

	mu         sync.Mutex
	deliveries map[string]int // event id -> successful deliveries
	attempts   map[string]int
	badSig     int
	injected   int
	enabled    bool
}

func newSink(secret string, seed uint64, errorProb float64) *sink {
	return &sink{secret: secret, seed: seed, errorProb: errorProb, deliveries: map[string]int{}, attempts: map[string]int{}, enabled: true}
}

func (s *sink) setFaults(on bool) {
	s.mu.Lock()
	s.enabled = on
	s.mu.Unlock()
}

func (s *sink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err := stripe.VerifySignature(body, r.Header.Get("Ledger-Signature"), []string{s.secret}, stripe.DefaultTolerance, time.Now()); err != nil {
		s.mu.Lock()
		s.badSig++
		s.mu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var ev outbox.Event
	if err := json.Unmarshal(body, &ev); err != nil || ev.ID == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	n := s.attempts[ev.ID]
	s.attempts[ev.ID] = n + 1
	fail := s.enabled && faults.Uniform(s.seed, "sink", ev.ID, strconv.Itoa(n)) < s.errorProb
	if fail {
		s.injected++
	} else {
		s.deliveries[ev.ID]++
	}
	s.mu.Unlock()
	if fail {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type SinkStats struct {
	UniqueEvents   int `json:"unique_events"`
	Deliveries     int `json:"deliveries"`
	DuplicateDeliv int `json:"duplicate_deliveries"`
	InjectedErrors int `json:"injected_errors"`
	BadSignatures  int `json:"bad_signatures"`
}

func (s *sink) stats() (SinkStats, map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := SinkStats{UniqueEvents: len(s.deliveries), InjectedErrors: s.injected, BadSignatures: s.badSig}
	cp := make(map[string]int, len(s.deliveries))
	for id, n := range s.deliveries {
		st.Deliveries += n
		if n > 1 {
			st.DuplicateDeliv += n - 1
		}
		cp[id] = n
	}
	return st, cp
}
