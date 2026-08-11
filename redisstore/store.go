package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/redis/go-redis/v9"
)

// Compile-time check.
var _ ratelimit.Store = (*Store)(nil)

// Store is a Redis-backed rate limit store. Limiting math uses
// Redis server TIME so multiple app nodes share one clock.
type Store struct {
	client   redis.Cmdable
	tokens   uint64
	interval time.Duration
	algo     ratelimit.Algorithm
	prefix   string
	script   *redis.Script
	ttl      time.Duration
	stopped  atomic.Bool
}

// New creates a Redis-backed Store from cfg.
func New(cfg Config) (*Store, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	src, err := scriptFor(cfg.Algorithm)
	if err != nil {
		return nil, err
	}
	return &Store{
		client:   cfg.Client,
		tokens:   cfg.Tokens,
		interval: cfg.Interval,
		algo:     cfg.Algorithm,
		prefix:   cfg.KeyPrefix,
		script:   redis.NewScript(src),
		ttl:      3 * cfg.Interval,
	}, nil
}

// Take takes one token for key if available. Elapsed time and
// Reset are derived from Redis TIME, not the local process clock.
func (s *Store) Take(
	ctx context.Context,
	key string,
) (ratelimit.Result, error) {
	if err := ctx.Err(); err != nil {
		return ratelimit.Result{}, err
	}
	if s.stopped.Load() {
		return ratelimit.Result{}, ratelimit.ErrStopped
	}

	intervalMS := s.interval.Milliseconds()
	ttlMS := s.ttl.Milliseconds()
	rate := float64(s.tokens) / s.interval.Seconds()

	raw, err := s.script.Run(
		ctx,
		s.client,
		[]string{s.prefix + key},
		s.tokens,
		rate,
		intervalMS,
		ttlMS,
	).Result()
	if err != nil {
		return ratelimit.Result{}, fmt.Errorf(
			"redisstore: take: %w",
			err,
		)
	}

	vals, ok := raw.([]any)
	if !ok || len(vals) != 4 {
		return ratelimit.Result{}, fmt.Errorf(
			"redisstore: unexpected script result %#v",
			raw,
		)
	}

	allowed, err := asInt64(vals[0])
	if err != nil {
		return ratelimit.Result{}, err
	}
	limit, err := asUint64(vals[1])
	if err != nil {
		return ratelimit.Result{}, err
	}
	remaining, err := asUint64(vals[2])
	if err != nil {
		return ratelimit.Result{}, err
	}
	resetMS, err := asInt64(vals[3])
	if err != nil {
		return ratelimit.Result{}, err
	}

	return ratelimit.Result{
		Limit:     limit,
		Remaining: remaining,
		Reset:     time.UnixMilli(resetMS).UTC(),
		OK:        allowed == 1,
	}, nil
}

// Close marks the store stopped before any wait. It does not
// close the Redis client. Close is idempotent; later Take
// calls return ErrStopped. This backend does not wait on
// owned resources.
func (s *Store) Close(_ context.Context) error {
	s.stopped.Store(true)
	return nil
}

func asInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return 0, fmt.Errorf(
				"redisstore: parse int64: %w",
				err,
			)
		}
		return n, nil
	default:
		return 0, fmt.Errorf(
			"redisstore: want int64, got %T",
			v,
		)
	}
}

func asUint64(v any) (uint64, error) {
	n, err := asInt64(v)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf(
			"redisstore: negative value %d",
			n,
		)
	}
	return uint64(n), nil
}
