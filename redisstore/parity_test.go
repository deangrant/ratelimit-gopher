package redisstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/algo"
	"github.com/deangrant/ratelimit-gopher/redisstore"
	"github.com/redis/go-redis/v9"
)

// TestAlgoRedisParity checks Redis Lua matches algo for a short
// scripted Take sequence at fixed times (guards Lua drift).
func TestAlgoRedisParity(t *testing.T) {
	t.Parallel()
	const (
		tokens   = uint64(2)
		interval = time.Second
		key      = "parity"
	)
	t0 := time.Date(2040, 3, 1, 0, 0, 0, 0, time.UTC)
	steps := []time.Time{
		t0,
		t0,
		t0,
		t0.Add(interval),
	}

	algos := []ratelimit.Algorithm{
		ratelimit.TokenBucket,
		ratelimit.LeakyBucket,
		ratelimit.FixedWindow,
		ratelimit.SlidingWindowLog,
		ratelimit.SlidingWindowCounter,
	}
	for _, a := range algos {
		t.Run(a.String(), func(t *testing.T) {
			t.Parallel()

			mr := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{
				Addr: mr.Addr(),
			})
			t.Cleanup(func() { _ = client.Close() })

			s, err := redisstore.New(redisstore.Config{
				Client:    client,
				Tokens:    tokens,
				Interval:  interval,
				Algorithm: a,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() {
				_ = s.Close(context.Background())
			})

			st := algo.Fresh(a, tokens, interval, t0)
			ctx := context.Background()
			for i, now := range steps {
				mr.SetTime(now)
				want := algo.Take(
					a, tokens, interval, now, &st,
				)
				got, err := s.Take(ctx, key)
				if err != nil {
					t.Fatalf("step %d Take: %v", i, err)
				}
				if got.OK != want.OK {
					t.Fatalf(
						"step %d OK: redis=%v algo=%v",
						i, got.OK, want.OK,
					)
				}
				if got.Limit != want.Limit {
					t.Fatalf(
						"step %d Limit: redis=%d algo=%d",
						i, got.Limit, want.Limit,
					)
				}
				if got.Remaining != want.Remaining {
					t.Fatalf(
						"step %d Remaining: redis=%d algo=%d",
						i, got.Remaining, want.Remaining,
					)
				}
				if !got.Reset.Equal(want.Reset) {
					t.Fatalf(
						"step %d Reset: redis=%v algo=%v",
						i, got.Reset, want.Reset,
					)
				}
			}
		})
	}
}
