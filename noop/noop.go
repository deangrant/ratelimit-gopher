// Package noop provides a Store that never rate limits.
package noop

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/deangrant/ratelimit-gopher"
)

// Compile-time check.
var _ ratelimit.Store = (*Store)(nil)

// Store always allows Take calls until Close. Results use
// math.MaxUint64 for Limit and Remaining to signal unlimited
// capacity (suitable for tests and disabling limiting).
type Store struct {
	stopped atomic.Bool
}

// New returns a no-op Store.
func New() *Store {
	return &Store{}
}

// Take always reports success with an unlimited remaining count
// until Close has been called.
func (s *Store) Take(
	ctx context.Context,
	_ string,
) (ratelimit.Result, error) {
	if err := ctx.Err(); err != nil {
		return ratelimit.Result{}, err
	}
	if s.stopped.Load() {
		return ratelimit.Result{}, ratelimit.ErrStopped
	}
	return ratelimit.Result{
		Limit:     math.MaxUint64,
		Remaining: math.MaxUint64,
		Reset:     time.Now().UTC(),
		OK:        true,
	}, nil
}

// Close marks the store stopped. Later Take calls return
// ErrStopped. Close is idempotent. This backend does not wait
// on owned resources.
func (s *Store) Close(_ context.Context) error {
	s.stopped.Store(true)
	return nil
}
