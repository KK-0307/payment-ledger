package stripe

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DefaultTolerance matches Stripe's libraries: reject signatures older than
// five minutes to limit replay.
const DefaultTolerance = 5 * time.Minute

var (
	ErrNoSignature      = errors.New("stripe webhook: missing or malformed Stripe-Signature header")
	ErrBadSignature     = errors.New("stripe webhook: no matching v1 signature")
	ErrTimestampTooOld  = errors.New("stripe webhook: timestamp outside tolerance")
	ErrInvalidEventJSON = errors.New("stripe webhook: invalid event JSON")
)

type Event struct {
	ID       string `json:"id"`
	Object   string `json:"object"`
	Type     string `json:"type"`
	Created  int64  `json:"created"`
	Livemode bool   `json:"livemode"`
	Data     struct {
		Object json.RawMessage `json:"object"`
	} `json:"data"`
}

// ComputeSignature returns hex(HMAC-SHA256(secret, "<t>.<payload>")).
func ComputeSignature(payload []byte, secret string, t time.Time) string {
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%d.", t.Unix())
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// SignatureHeader builds a Stripe-Signature header value.
func SignatureHeader(payload []byte, secret string, t time.Time) string {
	return fmt.Sprintf("t=%d,v1=%s", t.Unix(), ComputeSignature(payload, secret, t))
}

// VerifySignature checks a Stripe-Signature header against any of the given
// secrets (several are allowed during secret rotation).
func VerifySignature(payload []byte, header string, secrets []string, tolerance time.Duration, now time.Time) error {
	var ts int64 = -1
	var sigs []string
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch k {
		case "t":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return ErrNoSignature
			}
			ts = n
		case "v1":
			sigs = append(sigs, v)
		}
	}
	if ts < 0 || len(sigs) == 0 {
		return ErrNoSignature
	}
	t := time.Unix(ts, 0)
	if tolerance > 0 && (now.Sub(t) > tolerance || t.Sub(now) > tolerance) {
		return ErrTimestampTooOld
	}
	for _, secret := range secrets {
		want := ComputeSignature(payload, secret, t)
		for _, s := range sigs {
			if hmac.Equal([]byte(want), []byte(s)) {
				return nil
			}
		}
	}
	return ErrBadSignature
}

// ParseEvent verifies the signature and decodes the event.
func ParseEvent(payload []byte, header string, secrets []string, now time.Time) (*Event, error) {
	if err := VerifySignature(payload, header, secrets, DefaultTolerance, now); err != nil {
		return nil, err
	}
	var ev Event
	if err := json.Unmarshal(payload, &ev); err != nil || ev.ID == "" || ev.Type == "" {
		return nil, ErrInvalidEventJSON
	}
	return &ev, nil
}
