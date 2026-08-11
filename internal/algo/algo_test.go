package algo_test

import (
	"testing"
	"time"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/internal/algo"
)

func TestSlidingLogKeepsExactCutoff(t *testing.T) {
	t.Parallel()
	const limit = uint64(1)
	interval := time.Second
	now := time.Date(
		2026, 8, 11, 12, 0, 1, 0, time.UTC,
	)
	st := algo.State{
		LogTimes: []time.Time{now.Add(-interval)},
	}
	res := algo.Take(
		ratelimit.SlidingWindowLog,
		limit,
		interval,
		now,
		&st,
	)
	if res.OK {
		t.Fatalf(
			"Take at exact cutoff: got OK=true, want false",
		)
	}
	if len(st.LogTimes) != 1 {
		t.Fatalf(
			"log len: got %d, want 1 (kept cutoff entry)",
			len(st.LogTimes),
		)
	}
}

func TestTokenResetCeilsMilliseconds(t *testing.T) {
	t.Parallel()
	now := time.Date(
		2026, 8, 11, 12, 0, 0, 0, time.UTC,
	)
	// Empty bucket, capacity 1, interval 1.5ms → wait 1.5ms, ceil 2ms.
	interval := 1500 * time.Microsecond
	st := algo.State{
		Tokens:    0,
		UpdatedAt: now,
	}
	res := algo.Take(
		ratelimit.TokenBucket,
		1,
		interval,
		now,
		&st,
	)
	if res.OK {
		t.Fatalf("Take: got OK=true, want false")
	}
	got := res.Reset.Sub(now)
	want := 2 * time.Millisecond
	if got != want {
		t.Fatalf("Reset wait: got %v, want %v", got, want)
	}
}

func TestLeakyResetCeilsMilliseconds(t *testing.T) {
	t.Parallel()
	now := time.Date(
		2026, 8, 11, 12, 0, 0, 0, time.UTC,
	)
	interval := 1500 * time.Microsecond
	st := algo.State{
		Level:     1,
		UpdatedAt: now,
	}
	res := algo.Take(
		ratelimit.LeakyBucket,
		1,
		interval,
		now,
		&st,
	)
	if res.OK {
		t.Fatalf("Take: got OK=true, want false")
	}
	got := res.Reset.Sub(now)
	want := 2 * time.Millisecond
	if got != want {
		t.Fatalf("Reset wait: got %v, want %v", got, want)
	}
}
