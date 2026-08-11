package memory_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/memory"
)

func TestTokenBucketAllowThenDeny(t *testing.T) {
	t.Parallel()
	s, err := memory.New(memory.Config{
		Tokens:    2,
		Interval:  time.Minute,
		Algorithm: ratelimit.TokenBucket,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		res, err := s.Take(ctx, "k")
		if err != nil {
			t.Fatalf("Take #%d: %v", i, err)
		}
		if !res.OK {
			t.Fatalf("Take #%d: got OK=false, want true", i)
		}
	}
	res, err := s.Take(ctx, "k")
	if err != nil {
		t.Fatalf("Take overflow: %v", err)
	}
	if res.OK {
		t.Fatalf("Take overflow: got OK=true, want false")
	}
	if res.Remaining != 0 {
		t.Fatalf(
			"Remaining: got %d, want 0",
			res.Remaining,
		)
	}
}

func TestLeakyBucketRejectsBurst(t *testing.T) {
	t.Parallel()
	s, err := memory.New(memory.Config{
		Tokens:    2,
		Interval:  time.Minute,
		Algorithm: ratelimit.LeakyBucket,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		res, err := s.Take(ctx, "k")
		if err != nil {
			t.Fatalf("Take #%d: %v", i, err)
		}
		if !res.OK {
			t.Fatalf("Take #%d: got OK=false, want true", i)
		}
	}
	res, err := s.Take(ctx, "k")
	if err != nil {
		t.Fatalf("Take overflow: %v", err)
	}
	if res.OK {
		t.Fatalf("Take overflow: got OK=true, want false")
	}
}

func TestFixedWindowBoundarySpike(t *testing.T) {
	t.Parallel()
	s, err := memory.New(memory.Config{
		Tokens:    2,
		Interval:  100 * time.Millisecond,
		Algorithm: ratelimit.FixedWindow,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		res, err := s.Take(ctx, "k")
		if err != nil {
			t.Fatalf("Take #%d: %v", i, err)
		}
		if !res.OK {
			t.Fatalf("Take #%d: got OK=false, want true", i)
		}
	}
	res, err := s.Take(ctx, "k")
	if err != nil {
		t.Fatalf("Take blocked: %v", err)
	}
	if res.OK {
		t.Fatalf("Take blocked: got OK=true, want false")
	}

	time.Sleep(110 * time.Millisecond)
	allowed := 0
	for i := 0; i < 2; i++ {
		res, err := s.Take(ctx, "k")
		if err != nil {
			t.Fatalf("Take after reset #%d: %v", i, err)
		}
		if res.OK {
			allowed++
		}
	}
	if allowed != 2 {
		t.Fatalf(
			"after window: got %d allows, want 2",
			allowed,
		)
	}
}

func TestSlidingWindowLogPrecision(t *testing.T) {
	t.Parallel()
	s, err := memory.New(memory.Config{
		Tokens:    3,
		Interval:  200 * time.Millisecond,
		Algorithm: ratelimit.SlidingWindowLog,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		res, err := s.Take(ctx, "k")
		if err != nil {
			t.Fatalf("Take #%d: %v", i, err)
		}
		if !res.OK {
			t.Fatalf("Take #%d: got OK=false, want true", i)
		}
	}
	res, err := s.Take(ctx, "k")
	if err != nil {
		t.Fatalf("Take overflow: %v", err)
	}
	if res.OK {
		t.Fatalf("Take overflow: got OK=true, want false")
	}

	time.Sleep(210 * time.Millisecond)
	res, err = s.Take(ctx, "k")
	if err != nil {
		t.Fatalf("Take after window: %v", err)
	}
	if !res.OK {
		t.Fatalf("Take after window: got OK=false, want true")
	}
}

func TestSlidingWindowCounterNoDoubleBoundary(
	t *testing.T,
) {
	t.Parallel()
	s, err := memory.New(memory.Config{
		Tokens:    5,
		Interval:  100 * time.Millisecond,
		Algorithm: ratelimit.SlidingWindowCounter,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		res, err := s.Take(ctx, "k")
		if err != nil {
			t.Fatalf("Take #%d: %v", i, err)
		}
		if !res.OK {
			t.Fatalf("Take #%d: got OK=false, want true", i)
		}
	}
	// Immediately after filling, still denied.
	res, err := s.Take(ctx, "k")
	if err != nil {
		t.Fatalf("Take overflow: %v", err)
	}
	if res.OK {
		t.Fatalf("Take overflow: got OK=true, want false")
	}

	// Near next window start, previous weight still applies.
	time.Sleep(50 * time.Millisecond)
	res, err = s.Take(ctx, "k")
	if err != nil {
		t.Fatalf("Take mid-window: %v", err)
	}
	// May or may not allow depending on rotation; ensure
	// we never get a full fresh 5 immediately at boundary.
	time.Sleep(60 * time.Millisecond)
	allowed := 0
	for i := 0; i < 5; i++ {
		res, err := s.Take(ctx, "k")
		if err != nil {
			t.Fatalf("Take burst #%d: %v", i, err)
		}
		if res.OK {
			allowed++
		}
	}
	if allowed >= 5 {
		t.Fatalf(
			"boundary burst: got %d allows, want < 5",
			allowed,
		)
	}
}

func TestConcurrentTakeRespectsLimit(t *testing.T) {
	t.Parallel()
	const limit = 50
	s, err := memory.New(memory.Config{
		Tokens:   limit,
		Interval: time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	ctx := context.Background()
	var (
		mu      sync.Mutex
		allowed int
		wg      sync.WaitGroup
	)
	for i := 0; i < 200; i++ {
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
	s, err := memory.New(memory.Config{
		Tokens:   1,
		Interval: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = s.Take(context.Background(), "k")
	if !errors.Is(err, ratelimit.ErrStopped) {
		t.Fatalf(
			"Take after Close: got %v, want %v",
			err,
			ratelimit.ErrStopped,
		)
	}
}

func TestNewValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  memory.Config
	}{
		{
			name: "zero tokens",
			cfg: memory.Config{
				Tokens:   0,
				Interval: time.Second,
			},
		},
		{
			name: "tokens above MaxTokens",
			cfg: memory.Config{
				Tokens:   ratelimit.MaxTokens + 1,
				Interval: time.Second,
			},
		},
		{
			name: "zero interval",
			cfg: memory.Config{
				Tokens:   1,
				Interval: 0,
			},
		},
		{
			name: "bad algorithm",
			cfg: memory.Config{
				Tokens:    1,
				Interval:  time.Second,
				Algorithm: ratelimit.Algorithm(99),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := memory.New(tt.cfg)
			if err == nil {
				t.Fatalf("New: got nil error, want error")
			}
		})
	}
}

func TestNewAcceptsMaxTokens(t *testing.T) {
	t.Parallel()
	s, err := memory.New(memory.Config{
		Tokens:   ratelimit.MaxTokens,
		Interval: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
}
