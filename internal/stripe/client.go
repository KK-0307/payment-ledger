// Package stripe is a small client for the subset of the Stripe API this
// service uses (PaymentIntents, Refunds, search/list) plus webhook signature
// verification. It talks to https://api.stripe.com in test mode, or to any
// server that speaks the same protocol (the bundled simulator, or the
// fault-injection proxy in front of either).
package stripe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultBaseURL = "https://api.stripe.com"
	// APIVersion pins response shapes regardless of the account default.
	APIVersion = "2024-06-20"
)

// Client is safe for concurrent use.
type Client struct {
	BaseURL   string
	SecretKey string
	HTTP      *http.Client
}

// CheckKey refuses live-mode keys. This project only ever runs in test mode.
func CheckKey(baseURL, key string) error {
	if strings.HasPrefix(key, "sk_live_") || strings.HasPrefix(key, "rk_live_") {
		return errors.New("stripe: live-mode keys are refused; use a test-mode key (sk_test_...)")
	}
	if !strings.HasPrefix(key, "sk_test_") && !strings.HasPrefix(key, "rk_test_") {
		return errors.New("stripe: secret key must start with sk_test_ or rk_test_")
	}
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("stripe: invalid base URL %q", baseURL)
	}
	return nil
}

func New(baseURL, key string, httpClient *http.Client) (*Client, error) {
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	if err := CheckKey(baseURL, key); err != nil {
		return nil, err
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), SecretKey: key, HTTP: httpClient}, nil
}

type PaymentIntent struct {
	ID               string            `json:"id"`
	Object           string            `json:"object"`
	Amount           int64             `json:"amount"`
	Currency         string            `json:"currency"`
	Status           string            `json:"status"`
	Created          int64             `json:"created"`
	Description      string            `json:"description,omitempty"`
	PaymentMethod    string            `json:"payment_method,omitempty"`
	LatestCharge     string            `json:"latest_charge,omitempty"`
	Metadata         map[string]string `json:"metadata"`
	LastPaymentError *ErrorBody        `json:"last_payment_error,omitempty"`
}

type Refund struct {
	ID            string            `json:"id"`
	Object        string            `json:"object"`
	Amount        int64             `json:"amount"`
	Currency      string            `json:"currency"`
	Status        string            `json:"status"`
	PaymentIntent string            `json:"payment_intent"`
	Created       int64             `json:"created"`
	FailureReason string            `json:"failure_reason,omitempty"`
	Metadata      map[string]string `json:"metadata"`
}

type ErrorBody struct {
	Type          string         `json:"type"`
	Code          string         `json:"code,omitempty"`
	DeclineCode   string         `json:"decline_code,omitempty"`
	Message       string         `json:"message,omitempty"`
	Param         string         `json:"param,omitempty"`
	PaymentIntent *PaymentIntent `json:"payment_intent,omitempty"`
}

// Error is a non-2xx response from the API.
type Error struct {
	HTTPStatus int
	RequestID  string
	ErrorBody
}

func (e *Error) Error() string {
	return fmt.Sprintf("stripe: %d %s/%s: %s (request %s)", e.HTTPStatus, e.Type, e.Code, e.Message, e.RequestID)
}

// TransportError wraps failures where no HTTP response was received
// (timeouts, connection resets). The outcome of the request is unknown.
type TransportError struct{ Err error }

func (e *TransportError) Error() string { return "stripe: transport: " + e.Err.Error() }
func (e *TransportError) Unwrap() error { return e.Err }

type CreatePaymentIntentParams struct {
	Amount        int64
	Currency      string
	PaymentMethod string
	Description   string
	Metadata      map[string]string
}

// CreateAndConfirmPaymentIntent creates a card PaymentIntent and confirms it
// immediately (server-side confirmation with a test PaymentMethod such as
// pm_card_visa). idempotencyKey may be empty only in the unsafe benchmark
// baseline.
func (c *Client) CreateAndConfirmPaymentIntent(ctx context.Context, p CreatePaymentIntentParams, idempotencyKey string) (*PaymentIntent, error) {
	form := url.Values{}
	form.Set("amount", strconv.FormatInt(p.Amount, 10))
	form.Set("currency", strings.ToLower(p.Currency))
	form.Set("payment_method", p.PaymentMethod)
	form.Set("confirm", "true")
	form.Set("payment_method_types[]", "card")
	if p.Description != "" {
		form.Set("description", p.Description)
	}
	setMetadata(form, p.Metadata)
	var pi PaymentIntent
	if err := c.do(ctx, http.MethodPost, "/v1/payment_intents", form, idempotencyKey, &pi); err != nil {
		return nil, err
	}
	return &pi, nil
}

func (c *Client) GetPaymentIntent(ctx context.Context, id string) (*PaymentIntent, error) {
	var pi PaymentIntent
	if err := c.do(ctx, http.MethodGet, "/v1/payment_intents/"+url.PathEscape(id), nil, "", &pi); err != nil {
		return nil, err
	}
	return &pi, nil
}

// SearchPaymentIntentsByMetadata uses the Search API. Stripe documents that
// search results can lag writes by up to about a minute.
func (c *Client) SearchPaymentIntentsByMetadata(ctx context.Context, key, value string) ([]PaymentIntent, error) {
	q := url.Values{}
	q.Set("query", fmt.Sprintf("metadata['%s']:'%s'", key, value))
	var res struct {
		Data []PaymentIntent `json:"data"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/payment_intents/search?"+q.Encode(), nil, "", &res); err != nil {
		return nil, err
	}
	return res.Data, nil
}

// ListPaymentIntents pages through PaymentIntents created at or after since.
func (c *Client) ListPaymentIntents(ctx context.Context, since time.Time, fn func(PaymentIntent) error) error {
	after := ""
	for {
		q := url.Values{}
		q.Set("limit", "100")
		q.Set("created[gte]", strconv.FormatInt(since.Unix(), 10))
		if after != "" {
			q.Set("starting_after", after)
		}
		var res struct {
			Data    []PaymentIntent `json:"data"`
			HasMore bool            `json:"has_more"`
		}
		if err := c.do(ctx, http.MethodGet, "/v1/payment_intents?"+q.Encode(), nil, "", &res); err != nil {
			return err
		}
		for _, pi := range res.Data {
			if err := fn(pi); err != nil {
				return err
			}
		}
		if !res.HasMore || len(res.Data) == 0 {
			return nil
		}
		after = res.Data[len(res.Data)-1].ID
	}
}

type CreateRefundParams struct {
	PaymentIntent string
	Amount        int64
	Metadata      map[string]string
}

func (c *Client) CreateRefund(ctx context.Context, p CreateRefundParams, idempotencyKey string) (*Refund, error) {
	form := url.Values{}
	form.Set("payment_intent", p.PaymentIntent)
	form.Set("amount", strconv.FormatInt(p.Amount, 10))
	setMetadata(form, p.Metadata)
	var r Refund
	if err := c.do(ctx, http.MethodPost, "/v1/refunds", form, idempotencyKey, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (c *Client) GetRefund(ctx context.Context, id string) (*Refund, error) {
	var r Refund
	if err := c.do(ctx, http.MethodGet, "/v1/refunds/"+url.PathEscape(id), nil, "", &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// ListRefunds returns refunds for a PaymentIntent (first 100).
func (c *Client) ListRefunds(ctx context.Context, paymentIntent string) ([]Refund, error) {
	q := url.Values{}
	q.Set("payment_intent", paymentIntent)
	q.Set("limit", "100")
	var res struct {
		Data []Refund `json:"data"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/refunds?"+q.Encode(), nil, "", &res); err != nil {
		return nil, err
	}
	return res.Data, nil
}

func setMetadata(form url.Values, md map[string]string) {
	keys := make([]string, 0, len(md))
	for k := range md {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		form.Set("metadata["+k+"]", md[k])
	}
}

func (c *Client) do(ctx context.Context, method, path string, form url.Values, idempotencyKey string, out any) error {
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.SecretKey)
	req.Header.Set("Stripe-Version", APIVersion)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return &TransportError{Err: err}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		// Headers arrived but the body did not: still unknown outcome.
		return &TransportError{Err: err}
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := json.Unmarshal(raw, out); err != nil {
			return &TransportError{Err: fmt.Errorf("decode %s %s: %w", method, path, err)}
		}
		return nil
	}
	apiErr := &Error{HTTPStatus: resp.StatusCode, RequestID: resp.Header.Get("Request-Id")}
	var envelope struct {
		Error ErrorBody `json:"error"`
	}
	if json.Unmarshal(raw, &envelope) == nil {
		apiErr.ErrorBody = envelope.Error
	} else {
		apiErr.Type = "api_error"
		apiErr.Message = strings.TrimSpace(string(raw))
	}
	return apiErr
}
