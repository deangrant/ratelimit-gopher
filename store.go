package ratelimit

import (
	"context"
	"errors"
	"time"
)

// ErrStopped is returned by Store methods after Close has been called.
var ErrStopped = errors.New("ratelimit: store is stopped")

// Result is the outcome of a single Take call.
type Result struct {
	// Limit is the configured token capacity for the key.
	Limit uint64
	// Remaining is the number of tokens left after this Take.
	Remaining uint64
	// Reset is the UTC time when capacity replenishes or the window ends.
	Reset time.Time
	// OK is true when the take succeeded and the request should proceed.
	OK bool
}

// Store is a rate limit backend. Implementations must be safe for concurrent
// use. Take must not return an error for a missing key; keys are created
// lazily. Backend failures (for example network errors) are returned as
// errors. Whether to fail open or closed on those errors is left to the
// caller.
//
// Close is idempotent. After Close returns, later Take calls return
// ErrStopped. Close marks the store stopped before waiting on owned
// resources (for example background workers). The context bounds that
// wait; if the wait is aborted, the store remains stopped and Close may
// return ctx.Err().
type Store interface {
	Take(ctx context.Context, key string) (Result, error)
	Close(ctx context.Context) error
}
