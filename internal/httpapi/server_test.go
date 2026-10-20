package httpapi_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"paymentledger/internal/httpapi"
	"paymentledger/internal/ledger"
	"paymentledger/internal/payments"
	"paymentledger/internal/processor"
	"paymentledger/internal/stripe"
	"paymentledger/internal/stripesim"
	"paymentledger/internal/testutil"
)

func TestHTTPAPI(t *testing.T) {
	pool := testutil.NewDB(t)
	sim := stripesim.New(stripesim.Config{})
	simSrv := httptest.NewServer(sim.Handler())
	defer simSrv.Close()
	sc, err := stripe.New(simSrv.URL, "sk_test_x", &http.Client{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := payments.New(pool, ledger.New(), &processor.Stripe{Client: sc}, payments.DefaultConfig(), log)
	api := httptest.NewServer((&httpapi.Server{Svc: svc, Pool: pool, APIKeys: []string{"key_a", "key_b"}, Log: log}).Handler())
	defer api.Close()

	do := func(method, path, apiKey, idemKey, body string) (*http.Response, map[string]any) {
		req, _ := http.NewRequest(method, api.URL+path, strings.NewReader(body))
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		if idemKey != "" {
			req.Header.Set("Idempotency-Key", idemKey)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		json.NewDecoder(resp.Body).Decode(&m) //nolint:errcheck
		return resp, m
	}

	if resp, _ := do("POST", "/v1/merchants", "", "", `{"name":"x"}`); resp.StatusCode != 401 {
		t.Fatalf("no key: %d", resp.StatusCode)
	}
	if resp, _ := do("POST", "/v1/merchants", "wrong", "", `{"name":"x"}`); resp.StatusCode != 401 {
		t.Fatalf("bad key: %d", resp.StatusCode)
	}
	if resp, _ := do("POST", "/v1/merchants", "key_a", "", `{"name":"x","bogus":1}`); resp.StatusCode != 400 {
		t.Fatalf("unknown field accepted: %d", resp.StatusCode)
	}
	resp, m := do("POST", "/v1/merchants", "key_a", "m1", `{"name":"Shop"}`)
	if resp.StatusCode != 201 || resp.Header.Get("Request-Id") == "" {
		t.Fatalf("create merchant: %d %v", resp.StatusCode, m)
	}
	mer := m["id"].(string)

	body := `{"merchant_id":"` + mer + `","amount":1250,"currency":"eur","payment_method":"pm_card_visa"}`
	if resp, _ := do("POST", "/v1/payments", "key_a", "", body); resp.StatusCode != 400 {
		t.Fatalf("missing Idempotency-Key: %d", resp.StatusCode)
	}
	resp, p1 := do("POST", "/v1/payments", "key_a", "order-1", body)
	if resp.StatusCode != 201 || p1["status"] != "succeeded" {
		t.Fatalf("pay: %d %v", resp.StatusCode, p1)
	}
	resp, p2 := do("POST", "/v1/payments", "key_a", "order-1", body)
	if resp.Header.Get("Idempotent-Replayed") != "true" || p2["id"] != p1["id"] {
		t.Fatalf("replay: %v %v", resp.Header, p2)
	}
	// Keys are scoped per API key: the same key under key_b is a new request.
	resp, p3 := do("POST", "/v1/payments", "key_b", "order-1", body)
	if resp.StatusCode != 201 || p3["id"] == p1["id"] {
		t.Fatalf("scoping: %d %v", resp.StatusCode, p3)
	}
	pid := p1["id"].(string)
	if resp, l := do("GET", "/v1/payments/"+pid+"/ledger", "key_a", "", ""); resp.StatusCode != 200 || len(l["data"].([]any)) != 3 {
		t.Fatalf("ledger: %d %v", resp.StatusCode, l)
	}
	if resp, a := do("GET", "/v1/accounts/processor_clearing:EUR", "key_a", "", ""); resp.StatusCode != 200 || a["balance"].(float64) != 2500 {
		t.Fatalf("clearing account: %d %v", resp.StatusCode, a)
	}
	if resp, r := do("POST", "/v1/payments/"+pid+"/refunds", "key_a", "rf-1", `{"amount":250}`); resp.StatusCode != 201 || r["status"] != "succeeded" {
		t.Fatalf("refund: %d %v", resp.StatusCode, r)
	}
	if resp, a := do("GET", "/v1/admin/audit", "key_a", "", ""); resp.StatusCode != 200 || a["passed"] != true {
		t.Fatalf("audit: %d %v", resp.StatusCode, a)
	}
	if resp, _ := do("POST", "/v1/webhooks/stripe", "", "", `{}`); resp.StatusCode != 404 {
		t.Fatalf("webhook without configured secret: %d", resp.StatusCode)
	}
}
