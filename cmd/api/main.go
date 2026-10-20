// Command api runs the payment ledger HTTP API together with its background
// workers (recovery, outbox dispatcher). Run several instances against the
// same database for high availability; workers coordinate through leases.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"paymentledger/internal/config"
	"paymentledger/internal/db"
	"paymentledger/internal/httpapi"
	"paymentledger/internal/ledger"
	"paymentledger/internal/outbox"
	"paymentledger/internal/payments"
	"paymentledger/internal/processor"
	"paymentledger/internal/stripe"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		level = slog.LevelInfo
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer pool.Close()
	if cfg.MigrateOnStart {
		if err := db.Migrate(ctx, pool); err != nil {
			return err
		}
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 512
	transport.MaxIdleConnsPerHost = 512
	sc, err := stripe.New(cfg.StripeBaseURL, cfg.StripeSecretKey, &http.Client{Timeout: cfg.ProcessorTimeout, Transport: transport})
	if err != nil {
		return err
	}
	if cfg.Payments.Unsafe {
		log.Warn("IDEMPOTENCY_MODE=unsafe-off: idempotency protections are DISABLED (benchmark baseline only)")
	}
	svc := payments.New(pool, ledger.New(), &processor.Stripe{Client: sc}, cfg.Payments, log)

	workers, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()
	done := make(chan struct{}, 2)
	go func() {
		(&payments.RecoveryWorker{Svc: svc, Interval: cfg.RecoveryInterval, Batch: cfg.RecoveryBatch,
			Concurrency: 32, KeyMaxAge: cfg.IdempotencyKeyTTL}).Run(workers)
		done <- struct{}{}
	}()
	go func() {
		// Keep as many idle connections as concurrent deliveries; the default
		// (2 per host) opens a new TCP connection for almost every event.
		outboxTransport := http.DefaultTransport.(*http.Transport).Clone()
		outboxTransport.MaxIdleConnsPerHost = 64
		(&outbox.Dispatcher{Pool: pool, URL: cfg.OutboxURL, Secret: cfg.OutboxSecret,
			HTTP: &http.Client{Timeout: 5 * time.Second, Transport: outboxTransport}, Batch: 200, Concurrency: 32,
			Interval: cfg.OutboxInterval, ClaimFor: cfg.OutboxClaimFor, Log: log}).Run(workers)
		done <- struct{}{}
	}()

	api := &httpapi.Server{Svc: svc, Pool: pool, APIKeys: cfg.APIKeys, WebhookSecrets: cfg.StripeWebhookSecrets, Log: log}
	srv := &http.Server{
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      cfg.Payments.RequestBudget + 15*time.Second,
		IdleTimeout:       120 * time.Second,
	}
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}
	log.Info("listening", "addr", ln.Addr().String(), "processor", cfg.StripeBaseURL)

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}

	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Payments.RequestBudget+5*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx) // waits for in-flight requests
	cancelWorkers()
	<-done
	<-done
	return err
}
