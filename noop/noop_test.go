package noop_test

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/noop"
)

func TestTakeAlwaysAllows(t *testing.T) {
	t.Parallel()
	s := noop.New()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		res, err := s.Take(ctx, "any")
		if err != nil {
			t.Fatalf("Take #%d: %v", i, err)
		}
		if !res.OK {
			t.Fatalf("Take #%d: got OK=false, want true", i)
		}
		if res.Limit != math.MaxUint64 {
			t.Fatalf(
				"Limit: got %d, want MaxUint64",
				res.Limit,
			)
		}
		if res.Remaining != math.MaxUint64 {
			t.Fatalf(
				"Remaining: got %d, want MaxUint64",
				res.Remaining,
			)
		}
	}
}

func TestTakeAfterClose(t *testing.T) {
	t.Parallel()
	s := noop.New()
	ctx := context.Background()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err := s.Take(ctx, "any")
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
	s := noop.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: got %v, want nil", err)
	}
	_, err := s.Take(context.Background(), "any")
	if !errors.Is(err, ratelimit.ErrStopped) {
		t.Fatalf(
			"Take after cancelled Close: got %v, want %v",
			err,
			ratelimit.ErrStopped,
		)
	}
}
