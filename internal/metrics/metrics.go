// Package metrics declares the Prometheus collectors exported on /metrics.
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_http_requests_total", Help: "HTTP requests by route and status code.",
	}, []string{"route", "code"})

	HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ledger_http_request_duration_seconds",
		Help:    "HTTP request latency by route.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	}, []string{"route"})

	ProcessorCalls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_processor_calls_total", Help: "Processor calls by operation and outcome.",
	}, []string{"op", "outcome"})

	ProcessorDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ledger_processor_call_duration_seconds",
		Help:    "Processor call latency.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	}, []string{"op"})

	Transitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_state_transitions_total", Help: "Payment/refund state transitions by source (request, recovery, webhook).",
	}, []string{"object", "status", "source"})

	JournalEntries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_journal_entries_total", Help: "Journal entries written by kind.",
	}, []string{"kind"})

	RecoveryRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_recovery_items_total", Help: "Items picked up by the recovery worker.",
	}, []string{"object"})

	OutboxDeliveries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_outbox_deliveries_total", Help: "Outbox webhook delivery attempts by result.",
	}, []string{"result"})

	TxRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_tx_retries_total", Help: "Transactions rolled back and retried, by reason (deadlock, serialization_failure).",
	}, []string{"reason"})

	WebhooksReceived = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledger_processor_webhooks_total", Help: "Inbound processor webhooks by result.",
	}, []string{"result"})
)

func init() {
	prometheus.MustRegister(HTTPRequests, HTTPDuration, ProcessorCalls, ProcessorDuration,
		Transitions, JournalEntries, RecoveryRuns, OutboxDeliveries, WebhooksReceived, TxRetries)
}
