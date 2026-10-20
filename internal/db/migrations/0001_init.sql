-- Core schema: merchants, double-entry ledger, payments, refunds,
-- idempotency keys, transactional outbox and processor (Stripe) events.
--
-- Amounts are integers in the currency's minor unit (cents). Postings are
-- signed: debit = positive, credit = negative. Every journal entry must sum
-- to zero; this is enforced by a deferred constraint trigger, so a
-- transaction that writes an unbalanced entry cannot commit.

CREATE TABLE merchants (
    id          text PRIMARY KEY,
    name        text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE accounts (
    id          bigserial PRIMARY KEY,
    code        text NOT NULL UNIQUE,
    type        text NOT NULL CHECK (type IN ('asset', 'liability', 'equity', 'revenue', 'expense')),
    currency    char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    merchant_id text REFERENCES merchants (id),
    -- Cached sum of postings. Maintained only by the postings_apply trigger.
    balance     bigint NOT NULL DEFAULT 0,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX accounts_merchant_idx ON accounts (merchant_id) WHERE merchant_id IS NOT NULL;

CREATE TABLE journal_entries (
    id          bigserial PRIMARY KEY,
    kind        text NOT NULL,
    reference   text NOT NULL,
    currency    char(3) NOT NULL,
    description text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    -- One journal per business event: a payment can be captured once, a
    -- refund can be posted once, no matter how many workers race.
    UNIQUE (kind, reference)
);

CREATE TABLE postings (
    id               bigserial PRIMARY KEY,
    journal_entry_id bigint NOT NULL REFERENCES journal_entries (id),
    account_id       bigint NOT NULL REFERENCES accounts (id),
    amount           bigint NOT NULL CHECK (amount <> 0),
    created_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX postings_account_idx ON postings (account_id, id);
CREATE INDEX postings_journal_idx ON postings (journal_entry_id);

CREATE FUNCTION postings_apply() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    acct_currency char(3);
    je_currency   char(3);
BEGIN
    SELECT currency INTO je_currency FROM journal_entries WHERE id = NEW.journal_entry_id;
    UPDATE accounts SET balance = balance + NEW.amount
     WHERE id = NEW.account_id
     RETURNING currency INTO acct_currency;
    IF acct_currency IS DISTINCT FROM je_currency THEN
        RAISE EXCEPTION 'posting currency mismatch: account % is %, journal entry % is %',
            NEW.account_id, acct_currency, NEW.journal_entry_id, je_currency;
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER postings_apply AFTER INSERT ON postings
    FOR EACH ROW EXECUTE FUNCTION postings_apply();

CREATE FUNCTION journal_check_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    je_id bigint;
    total numeric;
    n     int;
BEGIN
    IF TG_TABLE_NAME = 'journal_entries' THEN je_id := NEW.id; ELSE je_id := NEW.journal_entry_id; END IF;
    SELECT coalesce(sum(amount), 0), count(*) INTO total, n FROM postings WHERE journal_entry_id = je_id;
    IF n < 2 OR total <> 0 THEN
        RAISE EXCEPTION 'journal entry % is unbalanced (sum=%, postings=%)', je_id, total, n
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END $$;

-- Checked at COMMIT, after all postings of the entry have been inserted.
CREATE CONSTRAINT TRIGGER postings_balanced AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION journal_check_balanced();
CREATE CONSTRAINT TRIGGER journal_entries_balanced AFTER INSERT ON journal_entries
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION journal_check_balanced();

CREATE FUNCTION forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END $$;

CREATE TRIGGER postings_append_only BEFORE UPDATE OR DELETE ON postings
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER journal_entries_append_only BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE FUNCTION forbid_mutation();

-- Payments. 'processing' means the processor may or may not have charged the
-- card; only the processor (via a retried idempotent call, a lookup, or a
-- webhook) can move it to a terminal state.
CREATE TABLE payments (
    id                        text PRIMARY KEY,
    merchant_id               text NOT NULL REFERENCES merchants (id),
    amount                    bigint NOT NULL CHECK (amount > 0),
    currency                  char(3) NOT NULL,
    payment_method            text NOT NULL,
    description               text NOT NULL DEFAULT '',
    metadata                  jsonb NOT NULL DEFAULT '{}',
    status                    text NOT NULL CHECK (status IN ('processing', 'succeeded', 'failed', 'needs_review')),
    processor_id              text UNIQUE,
    -- NULL only in the deliberately unsafe "naive" benchmark mode.
    processor_idempotency_key text UNIQUE,
    failure_code              text,
    failure_message           text,
    fee                       bigint,
    -- Succeeded + in-flight refunds (in-flight refunds reserve their amount).
    amount_refunded           bigint NOT NULL DEFAULT 0,
    attempts                  int NOT NULL DEFAULT 0,
    last_error                text,
    lease_until               timestamptz,
    next_attempt_at           timestamptz,
    -- Which path made the final transition: request, recovery or webhook.
    resolved_by               text,
    created_at                timestamptz NOT NULL DEFAULT now(),
    updated_at                timestamptz NOT NULL DEFAULT now(),
    CHECK (amount_refunded >= 0 AND amount_refunded <= amount)
);
CREATE INDEX payments_recovery_idx ON payments (next_attempt_at) WHERE status = 'processing';
CREATE INDEX payments_merchant_idx ON payments (merchant_id, created_at);

CREATE TABLE refunds (
    id                        text PRIMARY KEY,
    payment_id                text NOT NULL REFERENCES payments (id),
    amount                    bigint NOT NULL CHECK (amount > 0),
    currency                  char(3) NOT NULL,
    status                    text NOT NULL CHECK (status IN ('processing', 'succeeded', 'failed', 'needs_review')),
    processor_id              text UNIQUE,
    processor_idempotency_key text UNIQUE,
    failure_code              text,
    failure_message           text,
    attempts                  int NOT NULL DEFAULT 0,
    last_error                text,
    lease_until               timestamptz,
    next_attempt_at           timestamptz,
    -- Which path made the final transition: request, recovery or webhook.
    resolved_by               text,
    created_at                timestamptz NOT NULL DEFAULT now(),
    updated_at                timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX refunds_recovery_idx ON refunds (next_attempt_at) WHERE status = 'processing';
CREATE INDEX refunds_payment_idx ON refunds (payment_id);

-- Client-supplied idempotency keys (Idempotency-Key header), scoped per API
-- key. recovery_point records how far a request got so that a retry resumes
-- instead of repeating side effects.
CREATE TABLE idempotency_keys (
    id             bigserial PRIMARY KEY,
    scope          text NOT NULL,
    key            text NOT NULL,
    request_method text NOT NULL,
    request_path   text NOT NULL,
    request_hash   bytea NOT NULL,
    recovery_point text NOT NULL,
    resource_id    text,
    response_code  int,
    response_body  bytea, -- exact bytes, so replays are byte-identical
    created_at     timestamptz NOT NULL DEFAULT now(),
    last_run_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (scope, key)
);
CREATE INDEX idempotency_keys_created_idx ON idempotency_keys (created_at);

-- Transactional outbox: events are written in the same transaction as the
-- state change and delivered at-least-once by a dispatcher.
CREATE TABLE outbox_events (
    id              bigserial PRIMARY KEY,
    event_id        text NOT NULL UNIQUE,
    type            text NOT NULL,
    aggregate_id    text NOT NULL,
    payload         jsonb NOT NULL,
    attempts        int NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    delivered_at    timestamptz,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (type, aggregate_id)
);
CREATE INDEX outbox_pending_idx ON outbox_events (next_attempt_at) WHERE delivered_at IS NULL;

-- Inbound processor webhooks, deduplicated by the processor's event id.
CREATE TABLE processor_events (
    id           text PRIMARY KEY,
    type         text NOT NULL,
    payload      jsonb NOT NULL,
    received_at  timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz
);
