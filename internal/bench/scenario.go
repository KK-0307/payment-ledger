// Package bench replays a public transaction dataset against the payment
// API while injecting faults, then checks a correctness oracle and writes a
// machine-readable result. Everything that influences a run is in the
// Scenario plus the dataset file, both of which are hashed into the result.
package bench

import (
	"encoding/json"
	"fmt"
	"os"

	"paymentledger/internal/faults"
)

// Burst shapes open-loop arrivals into periodic bursts.
type Burst struct {
	PeriodS float64 `json:"period_s"`
	Duty    float64 `json:"duty"`
}

type Scenario struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Seed        uint64 `json:"seed"`

	Dataset string `json:"dataset"` // normalized CSV (see internal/dataset)
	Offset  int    `json:"offset"`  // first record (file is time-sorted)
	Rows    int    `json:"rows"`

	Merchants     int     `json:"merchants"`
	MerchantZipfS float64 `json:"merchant_zipf_s"` // 0 = uniform; datasets without merchants are assigned synthetically
	// HotMerchantFraction > 0 sends exactly that share of operations (in
	// expectation, deterministically per row) to merchant 0; the rest follow
	// the Zipf/uniform mix over the other merchants. It builds the dedicated
	// hot-merchant contention workloads.
	HotMerchantFraction float64 `json:"hot_merchant_fraction,omitempty"`
	// Payment method for rows labelled fraudulent (label=1); others use
	// pm_card_visa. Default pm_card_radarBlock (a decline).
	FraudPaymentMethod string  `json:"fraud_payment_method"`
	RefundFraction     float64 `json:"refund_fraction"` // of succeeded payments, refunded in full right after

	Load struct {
		Mode        string  `json:"mode"`        // "closed": fixed workers; "open": time-scaled dataset arrivals
		Concurrency int     `json:"concurrency"` // closed: workers; open: max in-flight operations
		TargetRate  float64 `json:"target_rate"` // open: mean arrivals per second after time scaling
		// Burst (open loop) compresses the arrivals of each Period into its
		// first Duty fraction, so the mean rate stays TargetRate while the rate
		// during a burst is TargetRate/Duty and the rest of the period is idle.
		Burst *Burst `json:"burst,omitempty"`
		// DuplicateFraction of the operations are sent twice at the same time
		// with the same Idempotency-Key (a client double-submit). Both answers
		// must agree; the oracle checks it. The second request is not counted
		// in throughput or latency.
		DuplicateFraction float64 `json:"duplicate_fraction,omitempty"`
	} `json:"load"`

	Client struct {
		TimeoutMs     int `json:"timeout_ms"`
		MaxAttempts   int `json:"max_attempts"`
		BackoffBaseMs int `json:"backoff_base_ms"`
		BackoffMaxMs  int `json:"backoff_max_ms"`
	} `json:"client"`

	Server struct {
		Instances       int               `json:"instances"`
		IdempotencyMode string            `json:"idempotency_mode"` // "on" | "unsafe-off"
		Env             map[string]string `json:"env"`
	} `json:"server"`

	Processor struct {
		Kind              string      `json:"kind"` // "sim" | "stripe" (real Stripe test mode)
		Webhooks          bool        `json:"webhooks"`
		WebhookDelayMs    int         `json:"webhook_delay_ms"`
		WebhookDropProb   float64     `json:"webhook_drop_prob"`
		InternalErrorProb float64     `json:"internal_error_prob"`
		Faults            faults.HTTP `json:"faults"`
	} `json:"processor"`

	DB struct {
		Faults faults.TCP `json:"faults"`
	} `json:"db"`

	OutboxSink struct {
		ErrorProb float64 `json:"error_prob"` // receiver returns 500
	} `json:"outbox_sink"`

	Events []Event `json:"events"`

	DrainTimeoutS int `json:"drain_timeout_s"`
}

// Event is a timed fault action relative to the start of the load phase.
type Event struct {
	AtS       float64      `json:"at_s"`
	EveryS    float64      `json:"every_s"` // >0: repeat until load ends
	Type      string       `json:"type"`    // db_blackout | api_crash | processor_faults | db_faults
	DurationS float64      `json:"duration_s"`
	Instance  int          `json:"instance"` // api_crash: instance index; -1 rotates
	Faults    *faults.HTTP `json:"faults,omitempty"`
	DBFaults  *faults.TCP  `json:"db_faults,omitempty"`
}

func LoadScenario(path string) (*Scenario, []byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	var s Scenario
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", path, err)
	}
	s.defaults()
	return &s, raw, s.validate()
}

func (s *Scenario) defaults() {
	if s.Seed == 0 {
		s.Seed = 1
	}
	if s.Merchants == 0 {
		s.Merchants = 100
	}
	if s.FraudPaymentMethod == "" {
		s.FraudPaymentMethod = "pm_card_radarBlock"
	}
	if s.Load.Mode == "" {
		s.Load.Mode = "closed"
	}
	if s.Load.Concurrency == 0 {
		s.Load.Concurrency = 32
	}
	if s.Client.TimeoutMs == 0 {
		s.Client.TimeoutMs = 5000
	}
	if s.Client.MaxAttempts == 0 {
		s.Client.MaxAttempts = 10
	}
	if s.Client.BackoffBaseMs == 0 {
		s.Client.BackoffBaseMs = 50
	}
	if s.Client.BackoffMaxMs == 0 {
		s.Client.BackoffMaxMs = 2000
	}
	if s.Server.Instances == 0 {
		s.Server.Instances = 1
	}
	if s.Server.IdempotencyMode == "" {
		s.Server.IdempotencyMode = "on"
	}
	if s.Processor.Kind == "" {
		s.Processor.Kind = "sim"
	}
	if s.DrainTimeoutS == 0 {
		s.DrainTimeoutS = 120
	}
}

func (s *Scenario) validate() error {
	switch {
	case s.Name == "":
		return fmt.Errorf("scenario name required")
	case s.Dataset == "" || s.Rows <= 0:
		return fmt.Errorf("dataset and rows are required")
	case s.HotMerchantFraction < 0 || s.HotMerchantFraction > 1 || (s.HotMerchantFraction > 0 && s.Merchants < 2):
		return fmt.Errorf("hot_merchant_fraction must be in [0,1] and needs at least 2 merchants")
	case s.Load.DuplicateFraction < 0 || s.Load.DuplicateFraction > 1:
		return fmt.Errorf("load.duplicate_fraction must be in [0,1]")
	case s.Load.Burst != nil && (s.Load.Mode != "open" || s.Load.Burst.PeriodS <= 0 || s.Load.Burst.Duty <= 0 || s.Load.Burst.Duty > 1):
		return fmt.Errorf("load.burst needs open mode, period_s > 0 and 0 < duty <= 1")
	case s.Load.Mode != "closed" && s.Load.Mode != "open":
		return fmt.Errorf("load.mode must be closed or open")
	case s.Load.Mode == "open" && s.Load.TargetRate <= 0:
		return fmt.Errorf("open load needs target_rate")
	case s.Processor.Kind != "sim" && s.Processor.Kind != "stripe":
		return fmt.Errorf("processor.kind must be sim or stripe")
	case s.Processor.Kind == "stripe" && (s.Processor.Webhooks || s.Processor.InternalErrorProb > 0):
		return fmt.Errorf("webhooks/internal_error_prob are simulator-only")
	}
	for _, e := range s.Events {
		switch e.Type {
		case "db_blackout", "api_crash":
		case "processor_faults":
			if e.Faults == nil {
				return fmt.Errorf("processor_faults event needs faults")
			}
		case "db_faults":
			if e.DBFaults == nil {
				return fmt.Errorf("db_faults event needs db_faults")
			}
		default:
			return fmt.Errorf("unknown event type %q", e.Type)
		}
	}
	return nil
}
