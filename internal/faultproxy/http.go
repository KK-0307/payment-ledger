// Package faultproxy provides fault-injecting proxies: an HTTP reverse proxy
// for the processor link (works in front of real Stripe test mode or the
// simulator) and a TCP proxy for the database link.
package faultproxy

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"paymentledger/internal/faults"
)

// HTTPStats counts injected faults.
type HTTPStats struct {
	Requests    int64 `json:"requests"`
	RateLimited int64 `json:"rate_limited"`
	ErrorBefore int64 `json:"error_before"`
	ResetBefore int64 `json:"reset_before"`
	DropAfter   int64 `json:"drop_after"`
	ErrorAfter  int64 `json:"error_after"`
	Forwarded   int64 `json:"forwarded"`
}

type HTTPProxy struct {
	seed   uint64
	target *url.URL
	rp     *httputil.ReverseProxy
	faults atomic.Pointer[faults.HTTP]

	mu       sync.Mutex
	attempts map[string]int

	requests, rateLimited, errorBefore, resetBefore, dropAfter, errorAfter, forwarded atomic.Int64
}

func NewHTTP(target string, seed uint64, f faults.HTTP) (*HTTPProxy, error) {
	u, err := url.Parse(target)
	if err != nil {
		return nil, err
	}
	p := &HTTPProxy{seed: seed, target: u, attempts: map[string]int{}}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = 512
	p.rp = &httputil.ReverseProxy{
		Rewrite:   func(r *httputil.ProxyRequest) { r.SetURL(u); r.Out.Host = u.Host },
		Transport: transport,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	p.SetFaults(f)
	return p, nil
}

func (p *HTTPProxy) SetFaults(f faults.HTTP) { p.faults.Store(&f) }

func (p *HTTPProxy) Stats() HTTPStats {
	return HTTPStats{
		Requests: p.requests.Load(), RateLimited: p.rateLimited.Load(), ErrorBefore: p.errorBefore.Load(),
		ResetBefore: p.resetBefore.Load(), DropAfter: p.dropAfter.Load(), ErrorAfter: p.errorAfter.Load(),
		Forwarded: p.forwarded.Load(),
	}
}

// faultKey identifies the logical request so that fault decisions do not
// depend on random ids: metadata[client_ref] (qualified by path, so a
// payment and its refund are distinct) when the caller supplied one, else
// the Idempotency-Key, else method+URL.
func faultKey(r *http.Request, body []byte) string {
	if len(body) > 0 {
		if v, err := url.ParseQuery(string(body)); err == nil {
			if ref := v.Get("metadata[client_ref]"); ref != "" {
				return r.URL.Path + "|" + ref
			}
		}
	}
	if k := r.Header.Get("Idempotency-Key"); k != "" {
		return k
	}
	return r.Method + " " + r.URL.RequestURI()
}

func (p *HTTPProxy) nextAttempt(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := p.attempts[key]
	p.attempts[key] = n + 1
	return n
}

func (p *HTTPProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.requests.Add(1)
	f := p.faults.Load()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	key := faultKey(r, body)
	attempt := p.nextAttempt(key)
	a := itoa(attempt)

	pre := faults.Uniform(p.seed, "pre", key, a)
	switch {
	case pre < f.RateLimitProb:
		p.rateLimited.Add(1)
		stripeError(w, http.StatusTooManyRequests, "rate_limit_error", "rate_limit", "injected: too many requests")
		return
	case pre < f.RateLimitProb+f.ErrorBeforeProb:
		p.errorBefore.Add(1)
		stripeError(w, http.StatusInternalServerError, "api_error", "", "injected: internal error before execution")
		return
	case pre < f.RateLimitProb+f.ErrorBeforeProb+f.ResetBeforeProb:
		p.resetBefore.Add(1)
		reset(w)
		return
	}

	if d := f.Latency(p.seed, key, attempt); d > 0 {
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			return
		}
	}

	post := faults.Uniform(p.seed, "post", key, a)
	switch {
	case post < f.DropAfterProb:
		p.dropAfter.Add(1)
		p.rp.ServeHTTP(httptest.NewRecorder(), r) // processor acts; response is lost
		if f.HangMs > 0 {
			select {
			case <-time.After(time.Duration(f.HangMs) * time.Millisecond):
			case <-r.Context().Done():
			}
		}
		reset(w)
	case post < f.DropAfterProb+f.ErrorAfterProb:
		p.errorAfter.Add(1)
		p.rp.ServeHTTP(httptest.NewRecorder(), r)
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte("<html><body>502 Bad Gateway (injected)</body></html>")) //nolint:errcheck
	default:
		p.forwarded.Add(1)
		p.rp.ServeHTTP(w, r)
	}
}

func stripeError(w http.ResponseWriter, status int, typ, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write([]byte(`{"error":{"type":"` + typ + `","code":"` + code + `","message":"` + msg + `"}}`)) //nolint:errcheck
}

// reset aborts the connection with a TCP RST and no HTTP response.
func reset(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic(http.ErrAbortHandler)
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		return
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetLinger(0) //nolint:errcheck
	}
	conn.Close()
}

func itoa(n int) string {
	return string(appendInt(nil, n))
}

func appendInt(b []byte, n int) []byte {
	if n >= 10 {
		b = appendInt(b, n/10)
	}
	return append(b, byte('0'+n%10))
}
