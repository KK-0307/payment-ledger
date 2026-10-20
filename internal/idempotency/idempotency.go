// Package idempotency stores client Idempotency-Key records, following the
// "atomic phases + recovery points" design popularised by Stripe: each key
// remembers how far its request got, and a retry resumes from there. A
// finished key replays the stored response byte-for-byte.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"paymentledger/internal/db"
)

const (
	PointStarted  = "started"
	PointFinished = "finished"
)

var (
	// ErrMismatch: the key was already used for a different request.
	ErrMismatch = errors.New("idempotency key reused with different request parameters")
	// ErrInvalidKey: missing or malformed key.
	ErrInvalidKey = errors.New("Idempotency-Key header must be 1-255 printable ASCII characters")
)

type Key struct {
	ID            int64
	RecoveryPoint string
	ResourceID    *string
	ResponseCode  *int
	ResponseBody  []byte
}

func (k *Key) Finished() bool { return k.RecoveryPoint == PointFinished }

// ValidateKey checks the header value.
func ValidateKey(key string) error {
	if len(key) == 0 || len(key) > 255 {
		return ErrInvalidKey
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x21 || key[i] > 0x7e {
			return ErrInvalidKey
		}
	}
	return nil
}

// HashRequest returns a stable fingerprint of the request parameters.
// encoding/json sorts map keys, so equal values hash equally.
func HashRequest(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("idempotency: hash request: %v", err))
	}
	sum := sha256.Sum256(b)
	return sum[:]
}

// Acquire inserts the key or locks the existing row (SELECT ... FOR UPDATE)
// for the rest of tx. Concurrent requests with the same key serialise on
// that row lock. The caller inspects RecoveryPoint to decide what to do.
func Acquire(ctx context.Context, tx pgx.Tx, scope, key, method, path string, hash []byte) (*Key, error) {
	var k Key
	err := tx.QueryRow(ctx, `
		INSERT INTO idempotency_keys (scope, key, request_method, request_path, request_hash, recovery_point)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (scope, key) DO NOTHING
		RETURNING id, recovery_point, resource_id, response_code, response_body`,
		scope, key, method, path, hash, PointStarted).
		Scan(&k.ID, &k.RecoveryPoint, &k.ResourceID, &k.ResponseCode, &k.ResponseBody)
	if err == nil {
		return &k, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("insert idempotency key: %w", err)
	}

	var gotMethod, gotPath string
	var gotHash []byte
	err = tx.QueryRow(ctx, `
		SELECT id, recovery_point, resource_id, response_code, response_body, request_method, request_path, request_hash
		FROM idempotency_keys WHERE scope = $1 AND key = $2 FOR UPDATE`, scope, key).
		Scan(&k.ID, &k.RecoveryPoint, &k.ResourceID, &k.ResponseCode, &k.ResponseBody, &gotMethod, &gotPath, &gotHash)
	if err != nil {
		return nil, fmt.Errorf("lock idempotency key: %w", err)
	}
	if gotMethod != method || gotPath != path || string(gotHash) != string(hash) {
		return nil, ErrMismatch
	}
	if _, err := tx.Exec(ctx, `UPDATE idempotency_keys SET last_run_at = now() WHERE id = $1`, k.ID); err != nil {
		return nil, err
	}
	return &k, nil
}

// Lock takes the key row's lock for the rest of tx. Code that updates a
// payment or refund AND finishes the request's key in one transaction must
// call it first: Acquire (a concurrent request with the same key) locks the key
// row and then the payment row, so every path uses the order key, then
// payment, then accounts. Taking the payment first and the key second would
// deadlock against Acquire.
func Lock(ctx context.Context, tx pgx.Tx, keyID int64) error {
	var one int
	if err := tx.QueryRow(ctx, `SELECT 1 FROM idempotency_keys WHERE id = $1 FOR UPDATE`, keyID).Scan(&one); err != nil {
		return fmt.Errorf("lock idempotency key %d: %w", keyID, err)
	}
	return nil
}

// SetRecoveryPoint advances the key to an intermediate phase.
func SetRecoveryPoint(ctx context.Context, tx pgx.Tx, keyID int64, point, resourceID string) error {
	_, err := tx.Exec(ctx, `UPDATE idempotency_keys SET recovery_point = $2, resource_id = $3 WHERE id = $1`, keyID, point, resourceID)
	return err
}

// Finish stores the final response; later requests with the key replay it.
func Finish(ctx context.Context, tx pgx.Tx, keyID int64, code int, body []byte) error {
	_, err := tx.Exec(ctx, `
		UPDATE idempotency_keys SET recovery_point = $2, response_code = $3, response_body = $4
		WHERE id = $1`, keyID, PointFinished, code, body)
	return err
}

// Reap deletes keys older than maxAgeSeconds; returns the number deleted.
func Reap(ctx context.Context, q db.Querier, maxAgeSeconds int) (int64, error) {
	tag, err := q.Exec(ctx, `DELETE FROM idempotency_keys WHERE created_at < now() - make_interval(secs => $1)`, maxAgeSeconds)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
