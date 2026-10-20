package payments

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const (
	StatusProcessing  = "processing"
	StatusSucceeded   = "succeeded"
	StatusFailed      = "failed"
	StatusNeedsReview = "needs_review"
)

// Recovery points stored on idempotency keys.
const (
	PointPaymentCreated = "payment_created"
	PointRefundCreated  = "refund_created"
)

type Merchant struct {
	ID        string    `json:"id"`
	Object    string    `json:"object"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type Payment struct {
	ID             string            `json:"id"`
	Object         string            `json:"object"`
	MerchantID     string            `json:"merchant_id"`
	Amount         int64             `json:"amount"`
	Currency       string            `json:"currency"`
	PaymentMethod  string            `json:"payment_method"`
	Description    string            `json:"description,omitempty"`
	Metadata       map[string]string `json:"metadata"`
	Status         string            `json:"status"`
	ProcessorID    *string           `json:"processor_id,omitempty"`
	FailureCode    *string           `json:"failure_code,omitempty"`
	FailureMessage *string           `json:"failure_message,omitempty"`
	Fee            *int64            `json:"fee,omitempty"`
	AmountRefunded int64             `json:"amount_refunded"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`

	ProcessorKey *string `json:"-"`
	Attempts     int     `json:"-"`
	// LeaseLeft is the time until lease_until, as of read time (0 = no live lease).
	LeaseLeft time.Duration `json:"-"`
}

func (p *Payment) Terminal() bool { return p.Status == StatusSucceeded || p.Status == StatusFailed }

type Refund struct {
	ID             string    `json:"id"`
	Object         string    `json:"object"`
	PaymentID      string    `json:"payment_id"`
	Amount         int64     `json:"amount"`
	Currency       string    `json:"currency"`
	Status         string    `json:"status"`
	ProcessorID    *string   `json:"processor_id,omitempty"`
	FailureCode    *string   `json:"failure_code,omitempty"`
	FailureMessage *string   `json:"failure_message,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`

	ProcessorKey *string       `json:"-"`
	Attempts     int           `json:"-"`
	LeaseLeft    time.Duration `json:"-"`
}

func (r *Refund) Terminal() bool { return r.Status == StatusSucceeded || r.Status == StatusFailed }

// Response is an HTTP response produced by the service. Responses with
// Retryable=false for idempotent endpoints are what get stored on the key.
type Response struct {
	Status     int
	Body       []byte
	Retryable  bool          // client should retry with the same key
	RetryAfter time.Duration // hint for Retry-After
	Replayed   bool          // served from a stored idempotency response
}

type ErrorBody struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func errorResponse(status int, typ, code, msg string) *Response {
	b, _ := json.Marshal(map[string]ErrorBody{"error": {Type: typ, Code: code, Message: msg}})
	return &Response{Status: status, Body: b}
}

func jsonResponse(status int, v any) *Response {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return &Response{Status: status, Body: b}
}

func retryableResponse(status int, code, msg string, after time.Duration) *Response {
	r := errorResponse(status, "api_error", code, msg)
	r.Retryable = true
	r.RetryAfter = after
	return r
}

type CreatePaymentRequest struct {
	MerchantID    string            `json:"merchant_id"`
	Amount        int64             `json:"amount"`
	Currency      string            `json:"currency"`
	PaymentMethod string            `json:"payment_method"`
	Description   string            `json:"description,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

var (
	currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)
	pmRe       = regexp.MustCompile(`^pm_[A-Za-z0-9_]{1,100}$`)
)

// MinAmount mirrors Stripe's minimum charge for most currencies (0.50).
const (
	MinAmount = 50
	MaxAmount = 99_999_999
)

func (r *CreatePaymentRequest) normalize() *Response {
	r.Currency = strings.ToUpper(strings.TrimSpace(r.Currency))
	bad := func(code, msg string) *Response {
		return errorResponse(http.StatusBadRequest, "invalid_request_error", code, msg)
	}
	switch {
	case r.MerchantID == "":
		return bad("parameter_missing", "merchant_id is required")
	case r.Amount < MinAmount || r.Amount > MaxAmount:
		return bad("amount_invalid", fmt.Sprintf("amount must be between %d and %d minor units", MinAmount, MaxAmount))
	case !currencyRe.MatchString(r.Currency):
		return bad("currency_invalid", "currency must be an ISO 4217 code")
	case !pmRe.MatchString(r.PaymentMethod):
		return bad("payment_method_invalid", "payment_method must be a PaymentMethod id such as pm_card_visa")
	case len(r.Description) > 1000:
		return bad("description_too_long", "description is limited to 1000 characters")
	case len(r.Metadata) > 20:
		return bad("metadata_invalid", "at most 20 metadata keys")
	}
	for k, v := range r.Metadata {
		if k == "" || len(k) > 40 || len(v) > 500 || k == "payment_id" || k == "merchant_id" {
			return bad("metadata_invalid", "metadata keys must be 1-40 chars (payment_id and merchant_id are reserved), values up to 500 chars")
		}
	}
	if r.Metadata == nil {
		r.Metadata = map[string]string{}
	}
	return nil
}

type CreateRefundRequest struct {
	Amount int64 `json:"amount,omitempty"` // 0 = remaining refundable amount
}
