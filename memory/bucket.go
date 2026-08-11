package memory

import (
	"time"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/algo"
)

// clock provides the current time. Tests inject a fake implementation.
type clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// bucket wraps shared algorithm state for one key.
type bucket struct {
	algo     ratelimit.Algorithm
	limit    uint64
	interval time.Duration
	state    algo.State
}

func newBucket(
	a ratelimit.Algorithm,
	tokens uint64,
	interval time.Duration,
	now time.Time,
) *bucket {
	return &bucket{
		algo:     a,
		limit:    tokens,
		interval: interval,
		state:    algo.Fresh(a, tokens, interval, now),
	}
}

func (b *bucket) lastAccess() time.Time {
	return b.state.Accessed
}

func (b *bucket) take(now time.Time) algo.Result {
	return algo.Take(
		b.algo,
		b.limit,
		b.interval,
		now,
		&b.state,
	)
}
