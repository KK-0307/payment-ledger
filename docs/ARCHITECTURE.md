# Architecture

A shorter, illustrated walkthrough is in the [engineering case study](ENGINEERING_CASE_STUDY.md).

This document describes how the payment ledger keeps money correct when the
network, the processor, the database or the API process itself fails. Every
claim here is backed by code and by a test or benchmark check; the relevant
file is linked.

## Components

```
            ┌────────────── API instance (cmd/api) ──────────────┐
 client ──► │ httpapi ─► payments.Service ─► processor.Stripe ───┼──► Stripe test mode
 (Idem-Key) │                 │                (Idem-Key)       │    or stripesim
            │                 ▼                                 │
            │   PostgreSQL: payments · refunds · idempotency_keys
            │               journal_entries · postings · accounts │
            │               outbox_events · processor_events     │
            │                 ▲                                 │
            │ RecoveryWorker ─┘   outbox.Dispatcher ─────────────┼──► merchant webhook
            │ POST /v1/webhooks/stripe ◄─────────────────────────┼─── Stripe webhooks
            └────────────────────────────────────────────────────┘
```

Any number of API instances can share one database. Coordination happens
only through row locks, leases and `FOR UPDATE SKIP LOCKED`; no instance
holds state another instance needs.

## The ledger

[`internal/ledger`](../internal/ledger/ledger.go), schema in
[`0001_init.sql`](../internal/db/migrations/0001_init.sql).

- **Accounts** have a type (asset, liability, revenue, …), a currency and a
  cached balance. The hot system accounts are **sharded**, see
  [Sharded system accounts](#sharded-system-accounts).
- **Journal entries** group **postings**. Postings are signed integers in
  minor units: debit positive, credit negative.

Invariants, and where they are enforced:

| Invariant | Enforced by |
|---|---|
| Every journal entry sums to 0 and has ≥ 2 postings | `Journal.Validate` in Go **and** a `DEFERRABLE INITIALLY DEFERRED` constraint trigger checked at `COMMIT` |
| A posting's account has the journal's currency | `postings_apply` trigger |
| Cached `accounts.balance` = sum of its postings (per account row, so per shard) | the same trigger updates it in the posting's transaction; `audit` re-derives it, per row and per logical account |
| Postings and journal entries are never changed or deleted | `BEFORE UPDATE OR DELETE` triggers raise |
| A business event is posted at most once | `UNIQUE (kind, reference)` on journal entries; `ledger.Post` is idempotent |

`TestDatabaseEnforcesInvariants` bypasses the Go code with raw SQL. It shows
that unbalanced, empty and cross-currency entries cannot commit, and that
postings cannot be updated or deleted.

Account rows are locked in one global order (descending id), so concurrent
postings cannot deadlock (`TestConcurrentPosting`). Shard accounts are
ordinary rows with their own ids, so the same order covers them.

Across tables the order is **idempotency key, then payment or refund, then
accounts**. A concurrent request with the same `Idempotency-Key` locks the key
row (`idempotency.Acquire`) and then re-reads the payment, so any transaction
that updates a payment or refund *and* finishes the key must take the key row
first (`idempotency.Lock`). Before this was enforced, a request finishing a
payment (payment, then key) deadlocked with its concurrent duplicate (key, then
payment); PostgreSQL broke the cycle after `deadlock_timeout` (1 s) and
`db.WithTx` retried, so nothing was lost, but each such pair stalled for a second
([BENCHMARKS.md](BENCHMARKS.md#duplicate-requests-and-a-lock-order-bug-the-benchmark-found)).
`TestConcurrentDuplicatesDoNotDeadlock` and
`TestConcurrentDuplicateRefundsDoNotDeadlock` fail without it. Retried
transactions are counted by `db.TxRetries` and exported as
`ledger_tx_retries_total{reason}`; it should stay at zero in normal operation.

### Sharded system accounts

Every capture credits `fee_revenue:CUR` and debits `processor_clearing:CUR`,
and every refund credits `processor_clearing:CUR`. As single rows, all of
those postings serialised on two row locks per currency. Each of the two
accounts is therefore split into `LEDGER_SHARDS` (default 16) shard accounts
(migration [`0002`](../internal/db/migrations/0002_account_shards.sql)):

- Shard 0 keeps the plain code (`processor_clearing:EUR`), so the account that
  existed before sharding is shard 0 and no data is rewritten. Shard *n* is
  `processor_clearing:EUR#n`. Shards are created on first use.
- `ledger.ShardFor` picks the shard from the payment id (capture) or refund
  id (refund), so the choice is deterministic and spreads evenly. A refund
  usually lands on a different shard than its capture; individual shards can
  be negative, only the sum is meaningful.
- `accounts.base_code` (a generated column, the code up to `#`) groups the
  shards of a logical account. `GET /v1/accounts/processor_clearing:EUR`
  returns the logical account: balance = sum of the shards, `shard_ids`
  lists them, and `/postings` merges them. `GET /v1/accounts/<id>` returns one
  shard.
- `LEDGER_SHARDS` can change at any time. Existing shards keep their
  balances and the logical balance stays the sum. `1` gives single rows
  again.
- Nothing about the DB-enforced invariants changed: the `postings_apply`
  trigger still maintains each shard's cached balance, entries still balance
  at `COMMIT`, postings are still append-only.

Audit and oracle: `account_balances_match_postings` still holds for every
shard row, `ledger_sums_to_zero_per_currency` is unchanged, and
`processor.clearing_matches_processor_net` compares the **sum over shards**
(`audit.ClearingBalances`). Two checks were added:
`logical_balances_match_postings` (sum of a logical account's shard balances
= sum of postings to all its shards) and `account_shards_consistent` (shards
share type, currency and merchant, and have numeric suffixes).

#### Sharded merchant payable accounts

`merchant_payable:<merchant>:CUR` is sharded per merchant, and only for
merchants that are busy. The Zipf-skewed benchmark sends about 21 % of its
traffic to one merchant, and that merchant's single payable row was the
largest row-lock queue once the system accounts were sharded
([BENCHMARKS.md](BENCHMARKS.md#where-the-baseline-time-goes)). Sharding every
merchant would create up to 16 rows for each of thousands of merchants that
never contend, so the shard count follows the merchant's rate:

- `ledger.RateTracker` counts a merchant's capture and refund postings per
  merchant and currency in a sliding window of two one-second buckets (in
  memory, per API process).
- `ledger.ShardsForRate` turns the rate into a shard count: one shard per
  `LEDGER_PAYABLE_SHARD_RATE` events/s (default 25), at most
  `LEDGER_PAYABLE_MAX_SHARDS` (default 16). At or below one shard's worth of
  traffic the merchant keeps the single legacy row (`merchant_payable:M:EUR`,
  shard 0), so quiet merchants cost nothing and no data is rewritten. A rate
  of 0 disables it.
- The shard is `ledger.ShardFor(payment or refund id, shards)`, the same
  function and the same shard numbering as the system accounts. With 16 shards
  a payment therefore uses shard *k* of clearing, fees and (for a hot
  merchant) payable, so two payments conflict only if they share a *k*.
- The shard count may change from one posting to the next, and two API
  instances may disagree. That cannot corrupt anything: a business event is
  posted at most once (`journal_entries` is unique on kind and reference, and
  `ledger.Post` returns before locking any account when the entry exists), so
  the count only decides which row that one posting locks. The logical
  balance is the sum over all shard rows, whatever their number.
- Reads are unchanged: `GET /v1/merchants/{id}` and `GET /v1/accounts/{code}`
  aggregate by `base_code` and show one logical payable account.
  Individual shards can be positive or negative (a refund usually lands on a
  different shard than its capture); only the sum is meaningful.

No schema change was needed: shard rows are ordinary rows of `accounts`
with `merchant_id` set, and the `postings_apply` trigger, the deferred
balance trigger and the append-only triggers are untouched. The audit checks
`account_shards_consistent` (all shards of a payable account name the same
merchant, type and currency) and `logical_balances_match_postings` already
cover the new rows; `TestMerchantPayableShardsAudit` checks that a shard
attributed to another merchant is caught.

### Postings per event

Capture of a payment of amount *A* with fee *F*
(`fee = min(A, round(A·bps/10⁴) + fixed)`, default 2.9 % + 30):

| account | amount |
|---|---|
| `processor_clearing:CUR` (asset: the processor owes us) | +A |
| `merchant_payable:<merchant>:CUR` (liability: we owe the merchant) | −(A−F) |
| `fee_revenue:CUR` (revenue) | −F |

Refund of *R* (fees are not returned, as with Stripe):

| account | amount |
|---|---|
| `merchant_payable:<merchant>:CUR` | +R |
| `processor_clearing:CUR` | −R |

So the clearing balance must always equal "captured − refunded" according
to the processor. The benchmark oracle checks exactly that against the
processor's own records.

## Payment flow: three atomic phases

[`internal/payments/service.go`](../internal/payments/service.go)

1. **Phase 1, one transaction.** Insert or lock the client's
   `Idempotency-Key` row. A key that already holds a response replays it
   byte-for-byte. A key reused with different parameters gets `422`.
   Otherwise the transaction inserts the payment as `processing`, holding a
   **lease** (`lease_until`), and records recovery point `payment_created`.
2. **Phase 2, no transaction.** Call the processor, sending
   `Idempotency-Key: pl-pay-<payment id>`. The key is derived from the
   payment, so it is the same for every retry by anyone.
3. **Phase 3, one transaction.** Apply the outcome:
   - Guarded status update: `UPDATE … WHERE status = 'processing'`.
   - Outbox event, unique per `(type, aggregate)`.
   - The idempotency response.
   - For a capture, the journal entry. It is written last because it locks
     the hot accounts.

The processor adapter ([`internal/processor`](../internal/processor/processor.go))
turns every response into a **definite** outcome or an `UnknownError`:

| Processor response | Meaning | What happens |
|---|---|---|
| 200 `succeeded` | charged | `succeeded`, capture posted |
| 402 `card_error`, 200 `requires_payment_method`, 400 validation | not charged | `failed`, no posting |
| timeout, connection reset, 5xx, 429, 409, unreadable body | **unknown**: maybe charged | stays `processing`; client gets `503 Retryable`; retry later with the same key |

An unknown outcome is **never** treated as a failure. Phase 2 can also be
retried inline once (`PROCESSOR_INLINE_RETRIES`), still with the same key.

### Who resolves a `processing` payment

Three independent paths converge on the same guarded transition. Whichever
path comes first wins; the others become no-ops:

1. **Client retry** with the same `Idempotency-Key`. It resumes at
   `payment_created`, takes the lease, and re-asks the processor.
2. **Recovery worker** ([`recovery.go`](../internal/payments/recovery.go)).
   It claims expired-lease payments whose `next_attempt_at` has passed, using
   `FOR UPDATE SKIP LOCKED`, and backs off exponentially with jitter. After
   `RECOVERY_MAX_ATTEMPTS` it marks the payment `needs_review`; it never
   guesses.
3. **Stripe webhook** (`payment_intent.succeeded`, `…payment_failed`). The
   event is verified (HMAC-SHA256, 5-minute tolerance, several secrets
   allowed for rotation), deduplicated by event id, and applied.

`resolved_by` on each row records which path won. The benchmark reports the
distribution.

### Edge cases handled explicitly

- **Stripe caches 500s under the idempotency key.** Stripe documents that if
  a request fails after execution began, the 500 is stored and replayed for
  the same key. Re-sending can then never succeed. After `LOOKUP_AFTER_ATTEMPTS`,
  recovery looks the PaymentIntent up through the Search API
  (`metadata['payment_id']`). If several PaymentIntents exist for one
  payment, that is reported as an error instead of picking one. Covered by
  `TestCachedProcessorErrorUsesLookup` and benchmark scenario 06.
- **Processor idempotency keys expire** (Stripe: 24 h). Payments older than
  `KeyMaxAge` (23 h) are only looked up, never re-sent.
- **Client disconnects mid-request.** Phases 2–3 run on a context detached
  from the client's (`context.WithoutCancel` plus `REQUEST_BUDGET`), so work
  is finished rather than abandoned.
- **Concurrent requests with one key** serialise on the key's row lock. A
  request that finds its payment in flight does not bounce immediately: it
  waits (polling every ≤100 ms, up to `INFLIGHT_WAIT`, default 3 s) until the
  payment is finished, in which case it replays the stored response, or the
  holder's lease lapses, in which case it takes over and finishes the work
  itself. Only after `INFLIGHT_WAIT` does the client get a retryable
  `409 request_in_progress`. Covered by `TestConcurrentSameKey` (20
  concurrent requests, exactly one charge) and `TestInflightWaitIsBounded`.
- **Leases are heartbeated.** A holder (a request or the recovery worker)
  renews its lease every `LEASE/3` while it works and stops before phase 3.
  So `LEASE` (default 10 s) only bounds how long a *dead* holder blocks
  others; a live holder with a slow processor call is never preempted
  (`TestHeartbeatKeepsSlowHolder` fails if the heartbeat is removed). A
  crashed holder's lease lapses within `LEASE`, and the next retry takes
  over inside a single request (`TestDeadHolderTakeoverInsideOneRequest`).
- **The lease is an optimisation, not a safety mechanism.** If two parties
  ever overlap (a heartbeat missed during a database stall), the processor
  idempotency key and the guarded update keep the result single. The cost
  of an overlap is one redundant processor call, never a duplicate charge.

## Refunds

[`refunds.go`](../internal/payments/refunds.go). Refunds use the same three
phases. In phase 1 the refund amount is **reserved** on the payment
(`payments.amount_refunded`) under the payment's row lock.
`CHECK (amount_refunded <= amount)` makes over-refunding impossible, even with
concurrent requests (`TestRefunds` runs 10 concurrent full refunds and
exactly one wins). A failed refund releases its reservation.

## Outbox

[`internal/outbox`](../internal/outbox/outbox.go). Events
(`payment.succeeded`, `payment.failed`, `refund.succeeded`, …) are inserted
in the phase-3 transaction, so an event exists if and only if the state change
committed. The dispatcher:

- claims batches by pushing `next_attempt_at` forward (a claim lease)
- POSTs with an HMAC signature (`Ledger-Signature`, same scheme as Stripe's)
- marks the event delivered, or backs off

Delivery is **at least once**. Receivers deduplicate on the event id; the
benchmark sink counts duplicate deliveries.

## Failure matrix

| Failure | Detected as | Recovered by | Benchmark scenario |
|---|---|---|---|
| Processor 5xx/429/reset **before** acting | unknown outcome | inline retry, client retry | 03 |
| Processor acted, response **lost** (timeout, reset, 502) | unknown outcome | idempotent replay at processor | 04 (05 = same faults without protection) |
| Processor acted, then cached 500 | unknown, repeats forever | lookup by metadata | 06 |
| DB connection reset / failover mid-transaction | tx error → 503 | client retry; work in phase 1/3 is idempotent | 07 |
| DB reset **during COMMIT** (commit outcome unknown) | tx error → 503 | retry finds the key at `payment_created` or `finished` | 07 |
| API process SIGKILL mid-request | lease stops being renewed | lease lapses within `LEASE`; a retry waits and takes over in one request, or the recovery worker does | 08 |
| Webhook never delivered | — | recovery worker (webhooks are an optimisation) | 09 |
| Merchant webhook receiver failing | non-2xx | outbox retry with backoff | 09 |

## The unsafe baseline

`IDEMPOTENCY_MODE=unsafe-off` exists **only** so the benchmark can show what
the mechanisms above prevent. It turns off client idempotency, the processor
idempotency key and the recovery worker, and treats an unknown outcome as a
failure. That is how a naive integration typically behaves. With the same
seed and faults as scenario 04, it double-charges cards and its ledger
disagrees with the processor.

## Known limitations

- Single currency per journal entry; no FX.
- No 3-D Secure / redirect flows: `requires_action` is recorded as declined
  (`authentication_required`).
- No payouts or settlement files. Reconciliation is against the processor's
  API objects (PaymentIntents/Refunds), not against balance transactions.
- `needs_review` items need an operator; there is no admin UI.
- **A client can still run out of patience.** Takeover after a crash takes
  up to `LEASE` (plus the poll interval). A client with a short retry budget,
  or one that gets `503`s for the whole of a database outage, can still give
  up; the payment is then finished by recovery and the client learns the
  result from the outbox event or a later `GET`. See
  [BENCHMARKS.md](BENCHMARKS.md) for measured give-up rates before and after
  heartbeat leases.
- Hot **merchant** accounts are sharded only above a rate, per API process
  (see [Sharded merchant payable accounts](#sharded-merchant-payable-accounts)).
  The rate is a sliding one-second estimate, so a merchant whose traffic
  jumps from nothing to a burst posts its first second or so to a single row
  before the estimate catches up; the default rate and cap were chosen by
  reasoning, not tuned.
- Sharding trades read cost for write concurrency: reading a system
  account's balance sums `LEDGER_SHARDS` rows per currency, and a hot
  merchant's up to `LEDGER_PAYABLE_MAX_SHARDS` rows.
- Search-based lookup relies on Stripe Search, which Stripe documents as
  eventually consistent, so a lookup may briefly miss a just-created object.
  The worker keeps retrying until `needs_review`.
