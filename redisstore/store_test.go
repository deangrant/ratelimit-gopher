package redisstore_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/redisstore"
	"github.com/redis/go-redis/v9"
)

func newTestStore(
	t *testing.T,
	algo ratelimit.Algorithm,
	tokens uint64,
	interval time.Duration,
) *redisstore.Store {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() { _ = client.Close() })

	s, err := redisstore.New(redisstore.Config{
		Client:    client,
		Tokens:    tokens,
		Interval:  interval,
		Algorithm: algo,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close(context.Background())
	})
	return s
}

func TestAlgorithmsAllowThenDeny(t *testing.T) {
	t.Parallel()
	algos := []ratelimit.Algorithm{
		ratelimit.TokenBucket,
		ratelimit.LeakyBucket,
		ratelimit.FixedWindow,
		ratelimit.SlidingWindowLog,
		ratelimit.SlidingWindowCounter,
	}
	for _, algo := range algos {
		t.Run(algo.String(), func(t *testing.T) {
			t.Parallel()
			s := newTestStore(
				t,
				algo,
				2,
				time.Minute,
			)
			ctx := context.Background()
			for i := 0; i < 2; i++ {
				res, err := s.Take(ctx, "k")
				if err != nil {
					t.Fatalf("Take #%d: %v", i, err)
				}
				if !res.OK {
					t.Fatalf(
						"Take #%d: got OK=false, want true",
						i,
					)
				}
			}
			res, err := s.Take(ctx, "k")
			if err != nil {
				t.Fatalf("Take overflow: %v", err)
			}
			if res.OK {
				t.Fatalf(
					"Take overflow: got OK=true, want false",
				)
			}
			if res.Limit != 2 {
				t.Fatalf(
					"Limit: got %d, want 2",
					res.Limit,
				)
			}
		})
	}
}

func TestConcurrentTake(t *testing.T) {
	t.Parallel()
	const limit = 40
	s := newTestStore(
		t,
		ratelimit.TokenBucket,
		limit,
		time.Minute,
	)
	ctx := context.Background()
	var (
		mu      sync.Mutex
		allowed int
		wg      sync.WaitGroup
	)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.Take(ctx, "shared")
			if err != nil {
				t.Errorf("Take: %v", err)
				return
			}
			if res.OK {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowed != limit {
		t.Fatalf(
			"allowed: got %d, want %d",
			allowed,
			limit,
		)
	}
}

func TestTakeAfterClose(t *testing.T) {
	t.Parallel()
	s := newTestStore(
		t,
		ratelimit.TokenBucket,
		1,
		time.Second,
	)
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err := s.Take(context.Background(), "k")
	if !errors.Is(err, ratelimit.ErrStopped) {
		t.Fatalf(
			"Take after Close: got %v, want %v",
			err,
			ratelimit.ErrStopped,
		)
	}
}

func TestCloseCancelledContextStillStops(t *testing.T) {
	t.Parallel()
	s := newTestStore(
		t,
		ratelimit.TokenBucket,
		1,
		time.Second,
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: got %v, want nil", err)
	}
	_, err := s.Take(context.Background(), "k")
	if !errors.Is(err, ratelimit.ErrStopped) {
		t.Fatalf(
			"Take after cancelled Close: got %v, want %v",
			err,
			ratelimit.ErrStopped,
		)
	}
}

func TestNewValidation(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	_, err := redisstore.New(redisstore.Config{
		Client:   nil,
		Tokens:   1,
		Interval: time.Second,
	})
	if err == nil {
		t.Fatalf("nil client: got nil error")
	}
	_, err = redisstore.New(redisstore.Config{
		Client:   client,
		Tokens:   0,
		Interval: time.Second,
	})
	if err == nil {
		t.Fatalf("zero tokens: got nil error")
	}
	_, err = redisstore.New(redisstore.Config{
		Client:   client,
		Tokens:   1,
		Interval: 500 * time.Microsecond,
	})
	if err == nil {
		t.Fatalf("sub-ms interval: got nil error")
	}
	_, err = redisstore.New(redisstore.Config{
		Client:   client,
		Tokens:   1,
		Interval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("1ms interval: %v", err)
	}
}

func TestSlidingLogKeepsExactCutoff(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() { _ = client.Close() })

	const (
		prefix   = "ratelimit:"
		key      = "cutoff"
		interval = time.Second
	)
	s, err := redisstore.New(redisstore.Config{
		Client:    client,
		Tokens:    1,
		Interval:  interval,
		Algorithm: ratelimit.SlidingWindowLog,
		KeyPrefix: prefix,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close(context.Background())
	})

	ctx := context.Background()
	redisKey := prefix + key
	intervalMS := interval.Milliseconds()
	denied := false
	for range 50 {
		if err := client.Del(ctx, redisKey).Err(); err != nil {
			t.Fatalf("Del: %v", err)
		}
		baseMS := time.Now().UnixMilli()
		cutoff := baseMS - intervalMS
		if err := client.ZAdd(ctx, redisKey, redis.Z{
			Score:  float64(cutoff),
			Member: "seed",
		}).Err(); err != nil {
			t.Fatalf("ZAdd: %v", err)
		}
		res, err := s.Take(ctx, key)
		if err != nil {
			t.Fatalf("Take: %v", err)
		}
		if !res.OK {
			denied = true
			break
		}
	}
	if !denied {
		t.Fatalf(
			"Take with seed at cutoff: never denied (want keep)",
		)
	}
}
