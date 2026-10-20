// Package ledger implements an append-only double-entry ledger on top of
// PostgreSQL. Postings are signed integers in minor units (debit positive,
// credit negative); every journal entry sums to zero. The database enforces
// the same invariants with triggers, so a bug here cannot commit an
// unbalanced entry.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"paymentledger/internal/db"
)

type AccountType string

const (
	Asset     AccountType = "asset"
	Liability AccountType = "liability"
	Equity    AccountType = "equity"
	Revenue   AccountType = "revenue"
	Expense   AccountType = "expense"
)

// Journal kinds.
const (
	KindPaymentCapture = "payment_capture"
	KindRefund         = "refund"
)

// Well-known account codes.
func ProcessorClearing(currency string) string { return "processor_clearing:" + currency }
func FeeRevenue(currency string) string        { return "fee_revenue:" + currency }
func MerchantPayable(merchantID, currency string) string {
	return "merchant_payable:" + merchantID + ":" + currency
}

// DefaultShards is how many shard accounts each hot system account has.
const DefaultShards = 16

// ShardCode returns the code of shard n of a logical account. Shard 0 keeps
// the plain code, so an account that existed before sharding is shard 0.
func ShardCode(base string, n int) string {
	if n <= 0 {
		return base
	}
	return fmt.Sprintf("%s#%d", base, n)
}

// ShardFor picks the shard for a business event. It is a pure function of
// the event's reference (payment or refund id), so a retried event always
// lands on the same shard, and events spread evenly over the shards.
func ShardFor(reference string, shards int) int {
	if shards <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(reference)) //nolint:errcheck
	return int(h.Sum32() % uint32(shards))
}

var (
	ErrUnbalanced      = errors.New("ledger: journal entry does not balance")
	ErrInvalidJournal  = errors.New("ledger: invalid journal entry")
	ErrAccountMismatch = errors.New("ledger: account exists with different type or currency")
	ErrAccountNotFound = errors.New("ledger: account not found")
)

// Account is a logical account. For a sharded account, Balance is the sum
// of its shards, ID is the lowest shard id and ShardIDs lists every shard.
type Account struct {
	ID         int64       `json:"id"`
	Code       string      `json:"code"`
	Type       AccountType `json:"type"`
	Currency   string      `json:"currency"`
	MerchantID *string     `json:"merchant_id,omitempty"`
	Balance    int64       `json:"balance"`
	CreatedAt  time.Time   `json:"created_at"`
	ShardIDs   []int64     `json:"shard_ids,omitempty"`
}

// IDs returns every account row behind this logical account.
func (a *Account) IDs() []int64 {
	if len(a.ShardIDs) > 0 {
		return a.ShardIDs
	}
	return []int64{a.ID}
}

// Entry is one leg of a journal entry.
type Entry struct {
	AccountID int64
	Amount    int64 // debit > 0, credit < 0
}

type Journal struct {
	Kind        string
	Reference   string
	Currency    string
	Description string
	Entries     []Entry
}

type Posting struct {
	ID             int64     `json:"id"`
	JournalEntryID int64     `json:"journal_entry_id"`
	AccountID      int64     `json:"account_id"`
	AccountCode    string    `json:"account_code"`
	Amount         int64     `json:"amount"`
	Kind           string    `json:"kind"`
	Reference      string    `json:"reference"`
	CreatedAt      time.Time `json:"created_at"`
}

// Ledger caches account ids by code; accounts are never deleted, so the
// cache cannot go stale.
type Ledger struct {
	ids sync.Map // code -> int64
}

func New() *Ledger { return &Ledger{} }

// EnsureAccount returns the id of the account with the given code, creating
// it if needed.
func (l *Ledger) EnsureAccount(ctx context.Context, q db.Querier, code string, typ AccountType, currency string, merchantID *string) (int64, error) {
	if v, ok := l.ids.Load(code); ok {
		return v.(int64), nil
	}
	var id int64
	err := q.QueryRow(ctx, `
		INSERT INTO accounts (code, type, currency, merchant_id) VALUES ($1, $2, $3, $4)
		ON CONFLICT (code) DO NOTHING RETURNING id`, code, typ, currency, merchantID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		var gotType AccountType
		var gotCur string
		if err := q.QueryRow(ctx, `SELECT id, type, currency FROM accounts WHERE code = $1`, code).Scan(&id, &gotType, &gotCur); err != nil {
			return 0, fmt.Errorf("load account %s: %w", code, err)
		}
		if gotType != typ || gotCur != currency {
			return 0, fmt.Errorf("%w: %s", ErrAccountMismatch, code)
		}
	} else if err != nil {
		return 0, fmt.Errorf("create account %s: %w", code, err)
	}
	// Only cache ids that are visible outside this transaction. An account
	// created inside a transaction that later rolls back would otherwise be
	// cached with a dangling id, so callers creating accounts should do so
	// in a transaction of its own (see EnsureAccountCommitted).
	return id, nil
}

// EnsureAccountCommitted creates (if needed) the account in its own
// autocommit statement and caches its id.
func (l *Ledger) EnsureAccountCommitted(ctx context.Context, q db.Querier, code string, typ AccountType, currency string, merchantID *string) (int64, error) {
	if v, ok := l.ids.Load(code); ok {
		return v.(int64), nil
	}
	id, err := l.EnsureAccount(ctx, q, code, typ, currency, merchantID)
	if err != nil {
		return 0, err
	}
	l.ids.Store(code, id)
	return id, nil
}

// Validate checks the journal's shape without touching the database.
func (j *Journal) Validate() error {
	if j.Kind == "" || j.Reference == "" || len(j.Currency) != 3 {
		return fmt.Errorf("%w: kind, reference and currency are required", ErrInvalidJournal)
	}
	if len(j.Entries) < 2 {
		return fmt.Errorf("%w: need at least two entries", ErrInvalidJournal)
	}
	var sum int64
	seen := make(map[int64]bool, len(j.Entries))
	for _, e := range j.Entries {
		if e.Amount == 0 {
			return fmt.Errorf("%w: zero amount", ErrInvalidJournal)
		}
		if seen[e.AccountID] {
			return fmt.Errorf("%w: account %d appears twice", ErrInvalidJournal, e.AccountID)
		}
		seen[e.AccountID] = true
		sum += e.Amount
	}
	if sum != 0 {
		return fmt.Errorf("%w: sum is %d", ErrUnbalanced, sum)
	}
	return nil
}

// Post writes a balanced journal entry inside tx. It is idempotent on
// (Kind, Reference): if the entry already exists it returns its id with
// created=false and writes nothing.
//
// Account rows are locked in descending id order. Any fixed order prevents
// deadlocks, and that includes shard accounts: each shard is an ordinary
// account row with its own id. Posting to different shards of a hot account
// does not conflict, which is the point of sharding it.
func Post(ctx context.Context, tx pgx.Tx, j Journal) (id int64, created bool, err error) {
	if err := j.Validate(); err != nil {
		return 0, false, err
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO journal_entries (kind, reference, currency, description) VALUES ($1, $2, $3, $4)
		ON CONFLICT (kind, reference) DO NOTHING RETURNING id`,
		j.Kind, j.Reference, j.Currency, j.Description).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.QueryRow(ctx, `SELECT id FROM journal_entries WHERE kind = $1 AND reference = $2`, j.Kind, j.Reference).Scan(&id); err != nil {
			return 0, false, err
		}
		return id, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("insert journal entry: %w", err)
	}

	entries := append([]Entry(nil), j.Entries...)
	sort.Slice(entries, func(a, b int) bool { return entries[a].AccountID > entries[b].AccountID })
	accountIDs := make([]int64, len(entries))
	amounts := make([]int64, len(entries))
	for i, e := range entries {
		accountIDs[i], amounts[i] = e.AccountID, e.Amount
	}

	rows, err := tx.Query(ctx, `SELECT id FROM accounts WHERE id = ANY($1) ORDER BY id DESC FOR UPDATE`, accountIDs)
	if err != nil {
		return 0, false, fmt.Errorf("lock accounts: %w", err)
	}
	locked := 0
	for rows.Next() {
		locked++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("lock accounts: %w", err)
	}
	if locked != len(accountIDs) {
		return 0, false, ErrAccountNotFound
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO postings (journal_entry_id, account_id, amount)
		SELECT $1, a, m FROM unnest($2::bigint[], $3::bigint[]) WITH ORDINALITY AS t(a, m, ord) ORDER BY ord`,
		id, accountIDs, amounts); err != nil {
		return 0, false, fmt.Errorf("insert postings: %w", err)
	}
	return id, true, nil
}

// logicalAccountSelect aggregates account rows by base_code, so a sharded
// account is one row whose balance is the sum of its shards.
const logicalAccountSelect = `
	SELECT min(id), base_code, min(type), min(currency), min(merchant_id), coalesce(sum(balance), 0)::bigint,
	       min(created_at), array_agg(id ORDER BY id)
	FROM accounts`

func scanLogical(r pgx.Row) (Account, error) {
	var a Account
	var ids []int64
	if err := r.Scan(&a.ID, &a.Code, &a.Type, &a.Currency, &a.MerchantID, &a.Balance, &a.CreatedAt, &ids); err != nil {
		return a, err
	}
	if len(ids) > 1 {
		a.ShardIDs = ids
	}
	return a, nil
}

// GetAccount looks an account up by code or by numeric id. A code names the
// logical account (all shards summed); an id names one account row.
func GetAccount(ctx context.Context, q db.Querier, idOrCode string) (*Account, error) {
	a, err := scanLogical(q.QueryRow(ctx, logicalAccountSelect+` WHERE base_code = $1 GROUP BY base_code`, idOrCode))
	if errors.Is(err, pgx.ErrNoRows) {
		a = Account{}
		err = q.QueryRow(ctx, `
			SELECT id, code, type, currency, merchant_id, balance, created_at FROM accounts WHERE id::text = $1`, idOrCode).
			Scan(&a.ID, &a.Code, &a.Type, &a.Currency, &a.MerchantID, &a.Balance, &a.CreatedAt)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrAccountNotFound
	}
	return &a, err
}

func MerchantAccounts(ctx context.Context, q db.Querier, merchantID string) ([]Account, error) {
	rows, err := q.Query(ctx, logicalAccountSelect+` WHERE merchant_id = $1 GROUP BY base_code ORDER BY min(currency), base_code`, merchantID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Account, error) { return scanLogical(r) })
}

// AccountPostings lists postings for an account (every shard of it), newest
// first, before the given posting id (0 = from the newest).
func AccountPostings(ctx context.Context, q db.Querier, a *Account, beforeID int64, limit int) ([]Posting, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := q.Query(ctx, `
		SELECT p.id, p.journal_entry_id, p.account_id, a.code, p.amount, j.kind, j.reference, p.created_at
		FROM postings p JOIN journal_entries j ON j.id = p.journal_entry_id JOIN accounts a ON a.id = p.account_id
		WHERE p.account_id = ANY($1) AND ($2 = 0 OR p.id < $2)
		ORDER BY p.id DESC LIMIT $3`, a.IDs(), beforeID, limit)
	if err != nil {
		return nil, err
	}
	return collectPostings(rows)
}

// ReferencePostings lists every posting of journal entries with the given
// reference (e.g. a payment id or refund id).
func ReferencePostings(ctx context.Context, q db.Querier, references []string) ([]Posting, error) {
	rows, err := q.Query(ctx, `
		SELECT p.id, p.journal_entry_id, p.account_id, a.code, p.amount, j.kind, j.reference, p.created_at
		FROM journal_entries j JOIN postings p ON p.journal_entry_id = j.id JOIN accounts a ON a.id = p.account_id
		WHERE j.reference = ANY($1) ORDER BY p.id`, references)
	if err != nil {
		return nil, err
	}
	return collectPostings(rows)
}

func collectPostings(rows pgx.Rows) ([]Posting, error) {
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Posting, error) {
		var p Posting
		err := r.Scan(&p.ID, &p.JournalEntryID, &p.AccountID, &p.AccountCode, &p.Amount, &p.Kind, &p.Reference, &p.CreatedAt)
		return p, err
	})
}
