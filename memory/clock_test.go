package memory

import (
	"context"
	"testing"
	"time"

	"github.com/deangrant/ratelimit-gopher"
)

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time { return c.now }

func TestTokenBucketRefillWithClock(t *testing.T) {
	t.Parallel()
	s, err := New(Config{
		Tokens:    1,
		Interval:  time.Second,
		Algorithm: ratelimit.TokenBucket,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	start := time.Unix(0, 0).UTC()
	clk := &fakeClock{now: start}
	s.setClock(clk)

	ctx := context.Background()
	res, err := s.Take(ctx, "k")
	if err != nil {
		t.Fatalf("Take #1: %v", err)
	}
	if !res.OK {
		t.Fatalf("Take #1: got OK=false, want true")
	}
	res, err = s.Take(ctx, "k")
	if err != nil {
		t.Fatalf("Take #2: %v", err)
	}
	if res.OK {
		t.Fatalf("Take #2: got OK=true, want false")
	}

	clk.now = start.Add(time.Second)
	res, err = s.Take(ctx, "k")
	if err != nil {
		t.Fatalf("Take after refill: %v", err)
	}
	if !res.OK {
		t.Fatalf(
			"Take after refill: got OK=false, want true",
		)
	}
}
