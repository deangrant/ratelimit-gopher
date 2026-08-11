package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/deangrant/ratelimit-gopher"
)

func TestCloseWhileTakesInFlight(t *testing.T) {
	t.Parallel()
	const limit = 100
	s, err := New(Config{
		Tokens:   limit,
		Interval: time.Minute,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

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
				if !errors.Is(err, ratelimit.ErrStopped) {
					t.Errorf("Take: %v", err)
				}
				return
			}
			if res.OK {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	time.Sleep(time.Millisecond)
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()
	if allowed > limit {
		t.Fatalf(
			"allowed: got %d, want <= %d",
			allowed,
			limit,
		)
	}
	_, err = s.Take(ctx, "shared")
	if !errors.Is(err, ratelimit.ErrStopped) {
		t.Fatalf(
			"Take after Close: got %v, want %v",
			err,
			ratelimit.ErrStopped,
		)
	}
}

func TestSweeperDeletesIdleKeys(t *testing.T) {
	t.Parallel()
	s, err := New(Config{
		Tokens:      1,
		Interval:    time.Second,
		SweepMinTTL: time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	start := time.Unix(100, 0).UTC()
	clk := &fakeClock{now: start}
	s.setClock(clk)

	ctx := context.Background()
	if _, err := s.Take(ctx, "idle"); err != nil {
		t.Fatalf("Take idle: %v", err)
	}
	if _, err := s.Take(ctx, "hot"); err != nil {
		t.Fatalf("Take hot: %v", err)
	}

	clk.now = start.Add(2 * time.Second)
	if _, err := s.Take(ctx, "hot"); err != nil {
		t.Fatalf("refresh hot: %v", err)
	}
	s.sweep()

	idleShard := s.shardFor("idle")
	idleShard.mu.Lock()
	_, idleOK := idleShard.data["idle"]
	idleShard.mu.Unlock()
	if idleOK {
		t.Fatalf("idle key still present after sweep")
	}

	hotShard := s.shardFor("hot")
	hotShard.mu.Lock()
	_, hotOK := hotShard.data["hot"]
	hotShard.mu.Unlock()
	if !hotOK {
		t.Fatalf("hot key missing after sweep")
	}
}

func TestCancelledCloseWaitLeavesStopped(t *testing.T) {
	t.Parallel()
	s, err := New(Config{
		Tokens:      1,
		Interval:    time.Second,
		SweepMinTTL: time.Hour, // slow ticker; stop via channel
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = s.Close(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Close: got %v, want nil or Canceled", err)
	}

	_, err = s.Take(context.Background(), "k")
	if !errors.Is(err, ratelimit.ErrStopped) {
		t.Fatalf(
			"Take: got %v, want %v",
			err,
			ratelimit.ErrStopped,
		)
	}

	// Sweeper should exit; second Close is idempotent nil.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("sweeper did not exit")
		default:
			if err := s.Close(context.Background()); err != nil {
				t.Fatalf("second Close: %v", err)
			}
			select {
			case <-s.sweepDone:
				return
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}
