// Package httpapi exposes the payment service over JSON/HTTP.
package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"paymentledger/internal/audit"
	"paymentledger/internal/id"
	"paymentledger/internal/ledger"
	"paymentledger/internal/metrics"
	"paymentledger/internal/payments"
	"paymentledger/internal/stripe"
)

type Server struct {
	Svc            *payments.Service
	Pool           *pgxpool.Pool
	APIKeys        []string // bearer tokens; idempotency keys are scoped per token
	WebhookSecrets []string // Stripe endpoint secrets (whsec_...)
	Log            *slog.Logger
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	route := func(pattern string, auth bool, h func(http.ResponseWriter, *http.Request) (*payments.Response, error)) {
		name := pattern
		mux.Handle(pattern, s.instrument(name, auth, h))
	}

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) }) //nolint:errcheck
	mux.HandleFunc("GET /readyz", s.ready)
	mux.Handle("GET /metrics", promhttp.Handler())

	route("POST /v1/merchants", true, s.createMerchant)
	route("GET /v1/merchants/{id}", true, func(w http.ResponseWriter, r *http.Request) (*payments.Response, error) {
		return s.Svc.GetMerchant(r.Context(), r.PathValue("id"))
	})
	route("POST /v1/payments", true, s.createPayment)
	route("GET /v1/payments/{id}", true, func(w http.ResponseWriter, r *http.Request) (*payments.Response, error) {
		return s.Svc.GetPayment(r.Context(), r.PathValue("id"))
	})
	route("GET /v1/payments/{id}/ledger", true, func(w http.ResponseWriter, r *http.Request) (*payments.Response, error) {
		return s.Svc.PaymentLedger(r.Context(), r.PathValue("id"))
	})
	route("POST /v1/payments/{id}/refunds", true, s.createRefund)
	route("GET /v1/refunds/{id}", true, func(w http.ResponseWriter, r *http.Request) (*payments.Response, error) {
		return s.Svc.GetRefund(r.Context(), r.PathValue("id"))
	})
	route("GET /v1/accounts/{id}", true, s.getAccount)
	route("GET /v1/accounts/{id}/postings", true, s.accountPostings)
	route("GET /v1/admin/audit", true, func(w http.ResponseWriter, r *http.Request) (*payments.Response, error) {
		rep, err := audit.Run(r.Context(), s.Pool)
		if err != nil {
			return nil, err
		}
		return jsonResp(200, rep), nil
	})
	route("POST /v1/webhooks/stripe", false, s.stripeWebhook)

	return recoverer(s.Log, mux)
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Pool.Ping(ctx); err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Write([]byte("ready\n")) //nolint:errcheck
}

type ctxKey int

const scopeKey ctxKey = 1

func (s *Server) instrument(route string, auth bool, h func(http.ResponseWriter, *http.Request) (*payments.Response, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		reqID := r.Header.Get("X-Request-Id")
		if reqID == "" || len(reqID) > 100 {
			reqID = id.New("req")
		}
		w.Header().Set("Request-Id", reqID)

		var resp *payments.Response
		var err error
		if auth {
			scope, ok := s.authenticate(r)
			if !ok {
				resp = errResp(401, "authentication_error", "invalid_api_key", "provide a valid API key as a Bearer token")
			} else {
				r = r.WithContext(context.WithValue(r.Context(), scopeKey, scope))
			}
		}
		if resp == nil {
			r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
			resp, err = h(w, r)
		}
		if err != nil {
			// Infrastructure failure (database unreachable, timeout). Nothing
			// was finalised, so the client may retry with the same key.
			s.Log.Warn("request failed", "route", route, "request_id", reqID, "err", err)
			resp = errResp(503, "api_error", "service_unavailable", "temporary failure; retry with the same Idempotency-Key")
			resp.Retryable = true
			resp.RetryAfter = time.Second
		}
		writeResponse(w, resp)
		metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(resp.Status)).Inc()
		metrics.HTTPDuration.WithLabelValues(route).Observe(time.Since(start).Seconds())
	})
}

func writeResponse(w http.ResponseWriter, resp *payments.Response) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	if resp.Replayed {
		h.Set("Idempotent-Replayed", "true")
	}
	if resp.Retryable {
		h.Set("Retryable", "true")
		if resp.RetryAfter > 0 {
			h.Set("Retry-After", strconv.Itoa(max(1, int(resp.RetryAfter.Seconds()))))
		}
	}
	w.WriteHeader(resp.Status)
	w.Write(resp.Body)    //nolint:errcheck
	w.Write([]byte("\n")) //nolint:errcheck
}

func (s *Server) authenticate(r *http.Request) (string, bool) {
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || tok == "" {
		return "", false
	}
	for _, k := range s.APIKeys {
		if subtle.ConstantTimeCompare([]byte(tok), []byte(k)) == 1 {
			sum := sha256.Sum256([]byte(k))
			return hex.EncodeToString(sum[:8]), true
		}
	}
	return "", false
}

func scope(r *http.Request) string { s, _ := r.Context().Value(scopeKey).(string); return s }

func decode(r *http.Request, v any) *payments.Response {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		return errResp(400, "invalid_request_error", "invalid_json", "request body: "+err.Error())
	}
	return nil
}

func (s *Server) createMerchant(w http.ResponseWriter, r *http.Request) (*payments.Response, error) {
	var req payments.CreateMerchantRequest
	if bad := decode(r, &req); bad != nil {
		return bad, nil
	}
	return s.Svc.CreateMerchant(r.Context(), scope(r), r.Header.Get("Idempotency-Key"), req)
}

func (s *Server) createPayment(w http.ResponseWriter, r *http.Request) (*payments.Response, error) {
	var req payments.CreatePaymentRequest
	if bad := decode(r, &req); bad != nil {
		return bad, nil
	}
	return s.Svc.CreatePayment(r.Context(), scope(r), r.Header.Get("Idempotency-Key"), req)
}

func (s *Server) createRefund(w http.ResponseWriter, r *http.Request) (*payments.Response, error) {
	var req payments.CreateRefundRequest
	if bad := decode(r, &req); bad != nil {
		return bad, nil
	}
	return s.Svc.CreateRefund(r.Context(), scope(r), r.Header.Get("Idempotency-Key"), r.PathValue("id"), req)
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) (*payments.Response, error) {
	a, err := ledger.GetAccount(r.Context(), s.Pool, r.PathValue("id"))
	if errors.Is(err, ledger.ErrAccountNotFound) {
		return errResp(404, "invalid_request_error", "resource_missing", "no such account"), nil
	}
	if err != nil {
		return nil, err
	}
	return jsonResp(200, a), nil
}

func (s *Server) accountPostings(w http.ResponseWriter, r *http.Request) (*payments.Response, error) {
	a, err := ledger.GetAccount(r.Context(), s.Pool, r.PathValue("id"))
	if errors.Is(err, ledger.ErrAccountNotFound) {
		return errResp(404, "invalid_request_error", "resource_missing", "no such account"), nil
	}
	if err != nil {
		return nil, err
	}
	before, _ := strconv.ParseInt(r.URL.Query().Get("before"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	ps, err := ledger.AccountPostings(r.Context(), s.Pool, a, before, limit)
	if err != nil {
		return nil, err
	}
	return jsonResp(200, map[string]any{"object": "list", "account": a, "data": ps}), nil
}

func (s *Server) stripeWebhook(w http.ResponseWriter, r *http.Request) (*payments.Response, error) {
	if len(s.WebhookSecrets) == 0 {
		return errResp(404, "invalid_request_error", "webhooks_disabled", "no webhook secret configured"), nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return errResp(400, "invalid_request_error", "body_unreadable", err.Error()), nil
	}
	ev, err := stripe.ParseEvent(body, r.Header.Get("Stripe-Signature"), s.WebhookSecrets, time.Now())
	if err != nil {
		metrics.WebhooksReceived.WithLabelValues("rejected").Inc()
		return errResp(400, "invalid_request_error", "signature_invalid", err.Error()), nil
	}
	if ev.Livemode {
		metrics.WebhooksReceived.WithLabelValues("rejected").Inc()
		return errResp(400, "invalid_request_error", "livemode_refused", "live-mode events are not accepted"), nil
	}
	if err := s.Svc.HandleProcessorEvent(r.Context(), ev); err != nil {
		return nil, err // 503: Stripe retries the delivery
	}
	return jsonResp(200, map[string]bool{"received": true}), nil
}

func recoverer(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				log.Error("panic", "path", r.URL.Path, "panic", v)
				writeResponse(w, errResp(500, "api_error", "internal_error", "internal error"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func jsonResp(status int, v any) *payments.Response {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return &payments.Response{Status: status, Body: b}
}

func errResp(status int, typ, code, msg string) *payments.Response {
	return jsonResp(status, map[string]payments.ErrorBody{"error": {Type: typ, Code: code, Message: msg}})
}
