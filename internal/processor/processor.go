// Package processor defines the card-processor interface the payment
// service depends on and its Stripe implementation.
//
// The central idea: every call returns either a definite Outcome or an
// *UnknownError. Unknown means "the processor may or may not have acted"
// (timeouts, connection resets, 5xx, 429, idempotency conflicts). The caller
// must resolve it later by retrying with the SAME idempotency key or by
// looking the object up; it must never assume failure.
package processor

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"paymentledger/internal/stripe"
)

type Result string

const (
	Succeeded Result = "succeeded"
	Declined  Result = "declined" // definitive: no money moved
	Pending   Result = "pending"  // accepted, final state arrives later
)

type Outcome struct {
	Result      Result
	ProcessorID string
	Code        string
	Message     string
}

// UnknownError signals an ambiguous outcome.
type UnknownError struct {
	Reason string
	Err    error
}

func (e *UnknownError) Error() string {
	return "processor outcome unknown (" + e.Reason + "): " + e.Err.Error()
}
func (e *UnknownError) Unwrap() error { return e.Err }

func IsUnknown(err error) bool {
	var u *UnknownError
	return errors.As(err, &u)
}

type ChargeRequest struct {
	PaymentID      string
	MerchantID     string
	Amount         int64
	Currency       string
	PaymentMethod  string
	Description    string
	Metadata       map[string]string
	IdempotencyKey string // empty only in the unsafe baseline mode
}

type RefundRequest struct {
	RefundID       string
	PaymentID      string
	ProcessorPayID string
	Amount         int64
	Metadata       map[string]string // the payment's metadata, propagated
	IdempotencyKey string
}

type Processor interface {
	Charge(ctx context.Context, req ChargeRequest) (Outcome, error)
	// LookupCharge finds the processor's view of a payment, by processor id
	// when known, otherwise by our payment id. found=false means the
	// processor has no record (yet).
	LookupCharge(ctx context.Context, paymentID, processorID string) (o Outcome, found bool, err error)
	Refund(ctx context.Context, req RefundRequest) (Outcome, error)
	LookupRefund(ctx context.Context, refundID, processorPayID, processorRefundID string) (o Outcome, found bool, err error)
}

// Stripe adapts *stripe.Client to Processor.
type Stripe struct {
	Client *stripe.Client
}

func (s *Stripe) Charge(ctx context.Context, req ChargeRequest) (Outcome, error) {
	md := map[string]string{}
	for k, v := range req.Metadata {
		md[k] = v
	}
	md["payment_id"] = req.PaymentID
	md["merchant_id"] = req.MerchantID
	pi, err := s.Client.CreateAndConfirmPaymentIntent(ctx, stripe.CreatePaymentIntentParams{
		Amount:        req.Amount,
		Currency:      req.Currency,
		PaymentMethod: req.PaymentMethod,
		Description:   req.Description,
		Metadata:      md,
	}, req.IdempotencyKey)
	if err != nil {
		return classifyError(err)
	}
	return paymentIntentOutcome(pi), nil
}

func (s *Stripe) LookupCharge(ctx context.Context, paymentID, processorID string) (Outcome, bool, error) {
	if processorID != "" {
		pi, err := s.Client.GetPaymentIntent(ctx, processorID)
		if err != nil {
			var se *stripe.Error
			if errors.As(err, &se) && se.HTTPStatus == 404 {
				return Outcome{}, false, nil
			}
			_, cerr := classifyError(err)
			return Outcome{}, false, cerr
		}
		return paymentIntentOutcome(pi), true, nil
	}
	pis, err := s.Client.SearchPaymentIntentsByMetadata(ctx, "payment_id", paymentID)
	if err != nil {
		_, cerr := classifyError(err)
		return Outcome{}, false, cerr
	}
	if len(pis) == 0 {
		return Outcome{}, false, nil
	}
	if len(pis) > 1 {
		// More than one PaymentIntent for one payment would be a duplicate
		// charge; surface it instead of picking one.
		ids := make([]string, len(pis))
		for i := range pis {
			ids[i] = pis[i].ID
		}
		return Outcome{}, false, fmt.Errorf("processor: %d PaymentIntents for payment %s: %s", len(pis), paymentID, strings.Join(ids, ","))
	}
	return paymentIntentOutcome(&pis[0]), true, nil
}

func (s *Stripe) Refund(ctx context.Context, req RefundRequest) (Outcome, error) {
	r, err := s.Client.CreateRefund(ctx, stripe.CreateRefundParams{
		PaymentIntent: req.ProcessorPayID,
		Amount:        req.Amount,
		Metadata:      refundMetadata(req),
	}, req.IdempotencyKey)
	if err != nil {
		return classifyError(err)
	}
	return RefundOutcome(r), nil
}

func (s *Stripe) LookupRefund(ctx context.Context, refundID, processorPayID, processorRefundID string) (Outcome, bool, error) {
	if processorRefundID != "" {
		r, err := s.Client.GetRefund(ctx, processorRefundID)
		if err != nil {
			_, cerr := classifyError(err)
			return Outcome{}, false, cerr
		}
		return RefundOutcome(r), true, nil
	}
	refunds, err := s.Client.ListRefunds(ctx, processorPayID)
	if err != nil {
		_, cerr := classifyError(err)
		return Outcome{}, false, cerr
	}
	for i := range refunds {
		if refunds[i].Metadata["refund_id"] == refundID {
			return RefundOutcome(&refunds[i]), true, nil
		}
	}
	return Outcome{}, false, nil
}

func refundMetadata(req RefundRequest) map[string]string {
	md := map[string]string{}
	for k, v := range req.Metadata {
		md[k] = v
	}
	md["refund_id"] = req.RefundID
	md["payment_id"] = req.PaymentID
	return md
}

// PaymentIntentOutcome maps a PaymentIntent status to an Outcome. Exported
// for webhook handling.
func PaymentIntentOutcome(pi *stripe.PaymentIntent) Outcome { return paymentIntentOutcome(pi) }

func paymentIntentOutcome(pi *stripe.PaymentIntent) Outcome {
	o := Outcome{ProcessorID: pi.ID}
	switch pi.Status {
	case "succeeded":
		o.Result = Succeeded
	case "requires_payment_method", "canceled":
		o.Result = Declined
		o.Code = "card_declined"
		if pi.LastPaymentError != nil {
			o.Code = firstNonEmpty(pi.LastPaymentError.DeclineCode, pi.LastPaymentError.Code, o.Code)
			o.Message = pi.LastPaymentError.Message
		}
	case "requires_action":
		// 3-D Secure / SCA flows need a customer redirect, which this
		// server-to-server API does not support.
		o.Result = Declined
		o.Code = "authentication_required"
		o.Message = "payment requires customer authentication, which this API does not support"
	default: // processing, requires_confirmation, requires_capture
		o.Result = Pending
	}
	return o
}

func RefundOutcome(r *stripe.Refund) Outcome {
	o := Outcome{ProcessorID: r.ID}
	switch r.Status {
	case "succeeded":
		o.Result = Succeeded
	case "failed", "canceled":
		o.Result = Declined
		o.Code = firstNonEmpty(r.FailureReason, "refund_failed")
	default: // pending, requires_action
		o.Result = Pending
	}
	return o
}

// classifyError turns a client error into either a definite decline or an
// UnknownError.
func classifyError(err error) (Outcome, error) {
	var te *stripe.TransportError
	if errors.As(err, &te) {
		return Outcome{}, &UnknownError{Reason: "transport", Err: err}
	}
	var se *stripe.Error
	if !errors.As(err, &se) {
		return Outcome{}, &UnknownError{Reason: "unexpected", Err: err}
	}
	switch {
	case se.HTTPStatus == 402 || se.Type == "card_error":
		o := Outcome{Result: Declined, Code: firstNonEmpty(se.DeclineCode, se.Code, "card_declined"), Message: se.Message}
		if se.PaymentIntent != nil {
			o.ProcessorID = se.PaymentIntent.ID
		}
		return o, nil
	case se.HTTPStatus == 400 && se.Type == "idempotency_error":
		// Same key, different parameters: a bug on our side. Do not guess.
		return Outcome{}, &UnknownError{Reason: "idempotency_error", Err: err}
	case se.HTTPStatus == 400 && se.Type == "invalid_request_error":
		// Validation failures are not executed (and not cached) by Stripe.
		return Outcome{Result: Declined, Code: firstNonEmpty(se.Code, "invalid_request"), Message: se.Message}, nil
	default:
		// 401/403 (config), 404, 409 (concurrent idempotent request), 429,
		// 5xx: retry later with the same key.
		return Outcome{}, &UnknownError{Reason: fmt.Sprintf("http_%d", se.HTTPStatus), Err: err}
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
