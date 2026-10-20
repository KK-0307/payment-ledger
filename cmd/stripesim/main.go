// Command stripesim runs the Stripe-compatible simulator, optionally behind
// the HTTP fault proxy, for local development without a Stripe account.
//
//	go run ./cmd/stripesim -addr :12111 -webhook-url http://localhost:8080/v1/webhooks/stripe -webhook-secret whsec_dev
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"os"

	"paymentledger/internal/faultproxy"
	"paymentledger/internal/faults"
	"paymentledger/internal/stripesim"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:12111", "listen address for the simulator")
	proxyAddr := flag.String("fault-proxy-addr", "", "if set, also serve a fault-injecting proxy in front of the simulator here")
	faultsJSON := flag.String("faults", "{}", `fault settings for the proxy, e.g. {"drop_after_prob":0.05,"hang_ms":3000}`)
	seed := flag.Uint64("seed", 1, "fault seed")
	internalErr := flag.Float64("internal-error-prob", 0, "probability of a cached 500 after execution")
	whURL := flag.String("webhook-url", "", "deliver signed webhooks here")
	whSecret := flag.String("webhook-secret", "whsec_dev", "webhook signing secret")
	flag.Parse()

	sim := stripesim.New(stripesim.Config{Seed: *seed, InternalErrorProb: *internalErr, WebhookURL: *whURL, WebhookSecret: *whSecret})
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("stripesim listening on http://%s (state: /_sim/state)", ln.Addr())

	if *proxyAddr != "" {
		var f faults.HTTP
		if err := json.Unmarshal([]byte(*faultsJSON), &f); err != nil {
			log.Fatalf("parse -faults: %v", err)
		}
		p, err := faultproxy.NewHTTP("http://"+ln.Addr().String(), *seed, f)
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			log.Printf("fault proxy listening on http://%s", *proxyAddr)
			if err := http.ListenAndServe(*proxyAddr, p); err != nil {
				log.Fatal(err)
			}
		}()
	}
	if err := http.Serve(ln, sim.Handler()); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
