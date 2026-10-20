package stripe

import (
	"errors"
	"testing"
	"time"
)

func TestVerifySignature(t *testing.T) {
	payload := []byte(`{"id":"evt_1","type":"payment_intent.succeeded","data":{"object":{}}}`)
	now := time.Unix(1_700_000_000, 0)
	hdr := SignatureHeader(payload, "whsec_a", now)

	if err := VerifySignature(payload, hdr, []string{"whsec_a"}, DefaultTolerance, now); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	// Rotation: any configured secret may match.
	if err := VerifySignature(payload, hdr, []string{"whsec_old", "whsec_a"}, DefaultTolerance, now); err != nil {
		t.Fatalf("rotated secret rejected: %v", err)
	}
	if err := VerifySignature(payload, hdr, []string{"whsec_b"}, DefaultTolerance, now); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("wrong secret: %v", err)
	}
	tampered := append([]byte(nil), payload...)
	tampered[10] ^= 1
	if err := VerifySignature(tampered, hdr, []string{"whsec_a"}, DefaultTolerance, now); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("tampered payload: %v", err)
	}
	if err := VerifySignature(payload, hdr, []string{"whsec_a"}, DefaultTolerance, now.Add(6*time.Minute)); !errors.Is(err, ErrTimestampTooOld) {
		t.Fatalf("replayed old event: %v", err)
	}
	for _, bad := range []string{"", "t=abc,v1=00", "v1=00", "t=1"} {
		if err := VerifySignature(payload, bad, []string{"whsec_a"}, DefaultTolerance, now); !errors.Is(err, ErrNoSignature) {
			t.Fatalf("header %q: %v", bad, err)
		}
	}
	// Multiple v1 entries (Stripe sends one per active secret).
	multi := "t=1700000000,v1=00ff," + hdr[len("t=1700000000,"):]
	if err := VerifySignature(payload, multi, []string{"whsec_a"}, DefaultTolerance, now); err != nil {
		t.Fatalf("multi-signature header: %v", err)
	}
}

func TestCheckKey(t *testing.T) {
	for key, ok := range map[string]bool{
		"sk_test_123": true, "rk_test_123": true, "sk_live_123": false, "rk_live_1": false, "pk_test_1": false, "": false,
	} {
		if err := CheckKey(DefaultBaseURL, key); (err == nil) != ok {
			t.Errorf("CheckKey(%q) = %v, want ok=%v", key, err, ok)
		}
	}
}
