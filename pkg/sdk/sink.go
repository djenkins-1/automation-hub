package sdk

import "context"

// Sink is a destination for notifications: a phone push service, a chat room,
// an email relay, a webhook.
//
// Deliver is called at least once per notification, so implementations should
// pass Notification.IdempotencyKey to the destination when it supports one.
// Failures are classified by wrapping the returned error:
//
//	sdk.Permanent(err)       never retry (bad credentials, 404, malformed request)
//	sdk.RetryAfter(err, d)   retry no sooner than d (HTTP 429 with Retry-After)
//	any other error          retry with the default backoff (network errors, 5xx)
//
// Deliver must honour ctx cancellation and must be safe for concurrent use.
type Sink interface {
	Deliver(ctx context.Context, n Notification) (Result, error)
}

// Notification is one rendered message to send to one sink.
type Notification struct {
	// ID identifies the delivery this notification belongs to.
	ID string
	// IdempotencyKey is identical across every retry of the same delivery.
	IdempotencyKey string
	// Title and Body are already rendered from the rule's templates.
	Title string
	Body  string
	// Event is the event that triggered the notification.
	Event Event
	// Attempt is 1 for the first try, 2 for the first retry, and so on.
	Attempt int
}

// Result describes a successful delivery.
type Result struct {
	// Detail is a short human-readable note recorded in the attempt log,
	// such as "HTTP 200" or "queued as msg-123".
	Detail string
}
