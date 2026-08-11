package algo_test

import (
	"testing"
	"time"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/algo"
)

func TestFakeClockSuite(t *testing.T) {
	t.Parallel()
	base := time.Date(
		2026, 8, 11, 12, 0, 0, 0, time.UTC,
	)

	t.Run("token_refill_exactly_one", func(t *testing.T) {
		t.Parallel()
		interval := time.Second
		st := algo.Fresh(
			ratelimit.TokenBucket, 1, interval, base,
		)
		res := algo.Take(
			ratelimit.TokenBucket, 1, interval, base, &st,
		)
		if !res.OK || res.Remaining != 0 {
			t.Fatalf(
				"first: OK=%v Remaining=%d, want OK true Remaining 0",
				res.OK,
				res.Remaining,
			)
		}
		res = algo.Take(
			ratelimit.TokenBucket,
			1,
			interval,
			base,
			&st,
		)
		if res.OK {
			t.Fatalf("empty: got OK=true, want false")
		}
		if res.Remaining != 0 {
			t.Fatalf(
				"empty Remaining: got %d, want 0",
				res.Remaining,
			)
		}
		if !res.Reset.After(base) {
			t.Fatalf("empty Reset: got %v, want after now", res.Reset)
		}
		later := base.Add(interval)
		res = algo.Take(
			ratelimit.TokenBucket,
			1,
			interval,
			later,
			&st,
		)
		if !res.OK {
			t.Fatalf(
				"after refill: got OK=false, want true",
			)
		}
	})

	t.Run("leaky_drain_frees_capacity", func(t *testing.T) {
		t.Parallel()
		interval := time.Second
		st := algo.Fresh(
			ratelimit.LeakyBucket, 1, interval, base,
		)
		res := algo.Take(
			ratelimit.LeakyBucket, 1, interval, base, &st,
		)
		if !res.OK {
			t.Fatalf("first: got OK=false, want true")
		}
		res = algo.Take(
			ratelimit.LeakyBucket, 1, interval, base, &st,
		)
		if res.OK {
			t.Fatalf("full: got OK=true, want false")
		}
		later := base.Add(interval)
		res = algo.Take(
			ratelimit.LeakyBucket,
			1,
			interval,
			later,
			&st,
		)
		if !res.OK {
			t.Fatalf(
				"after drain: got OK=false, want true",
			)
		}
	})

	t.Run("fixed_window_multi_window_skip", func(t *testing.T) {
		t.Parallel()
		interval := time.Second
		st := algo.Fresh(
			ratelimit.FixedWindow, 2, interval, base,
		)
		for i := 0; i < 2; i++ {
			res := algo.Take(
				ratelimit.FixedWindow,
				2,
				interval,
				base,
				&st,
			)
			if !res.OK {
				t.Fatalf(
					"Take #%d: got OK=false, want true",
					i,
				)
			}
		}
		res := algo.Take(
			ratelimit.FixedWindow, 2, interval, base, &st,
		)
		if res.OK {
			t.Fatalf("blocked: got OK=true, want false")
		}
		// Jump more than one full window past WindowEnd.
		later := st.WindowEnd.Add(2 * interval)
		res = algo.Take(
			ratelimit.FixedWindow,
			2,
			interval,
			later,
			&st,
		)
		if !res.OK {
			t.Fatalf(
				"after skip: got OK=false, want true",
			)
		}
		if st.Count != 1 {
			t.Fatalf(
				"Count after skip: got %d, want 1",
				st.Count,
			)
		}
	})

	t.Run("sliding_log_inclusive_cutoff", func(t *testing.T) {
		t.Parallel()
		interval := time.Second
		now := base.Add(interval)
		st := algo.State{
			LogTimes: []time.Time{now.Add(-interval)},
		}
		res := algo.Take(
			ratelimit.SlidingWindowLog,
			1,
			interval,
			now,
			&st,
		)
		if res.OK {
			t.Fatalf(
				"at cutoff: got OK=true, want false",
			)
		}
		if len(st.LogTimes) != 1 {
			t.Fatalf(
				"log len: got %d, want 1",
				len(st.LogTimes),
			)
		}
		if res.Remaining != 0 {
			t.Fatalf(
				"Remaining: got %d, want 0",
				res.Remaining,
			)
		}
	})

	t.Run("sliding_counter_one_window_rotation", func(t *testing.T) {
		t.Parallel()
		interval := time.Second
		st := algo.Fresh(
			ratelimit.SlidingWindowCounter,
			2,
			interval,
			base,
		)
		for i := 0; i < 2; i++ {
			res := algo.Take(
				ratelimit.SlidingWindowCounter,
				2,
				interval,
				base,
				&st,
			)
			if !res.OK {
				t.Fatalf(
					"fill #%d: got OK=false, want true",
					i,
				)
			}
		}
		// Advance exactly one window: prev=2, curr starts empty
		// but weighted prev still blocks near window start.
		next := base.Add(interval)
		res := algo.Take(
			ratelimit.SlidingWindowCounter,
			2,
			interval,
			next,
			&st,
		)
		if res.OK {
			t.Fatalf(
				"just after rotate: got OK=true, want false",
			)
		}
		if st.Prev != 2 || st.Count != 0 {
			t.Fatalf(
				"after one-window rotate: prev=%d count=%d, want prev=2 count=0",
				st.Prev,
				st.Count,
			)
		}
	})

	t.Run("sliding_counter_multi_window_idle", func(t *testing.T) {
		t.Parallel()
		interval := time.Second
		st := algo.Fresh(
			ratelimit.SlidingWindowCounter,
			2,
			interval,
			base,
		)
		res := algo.Take(
			ratelimit.SlidingWindowCounter,
			2,
			interval,
			base,
			&st,
		)
		if !res.OK {
			t.Fatalf("first: got OK=false, want true")
		}
		later := base.Add(3 * interval)
		res = algo.Take(
			ratelimit.SlidingWindowCounter,
			2,
			interval,
			later,
			&st,
		)
		if !res.OK {
			t.Fatalf(
				"after idle windows: got OK=false, want true",
			)
		}
		if st.Prev != 0 {
			t.Fatalf(
				"Prev after multi-window idle: got %d, want 0",
				st.Prev,
			)
		}
	})

	t.Run("remaining_reset_invariants", func(t *testing.T) {
		t.Parallel()
		algos := []ratelimit.Algorithm{
			ratelimit.TokenBucket,
			ratelimit.LeakyBucket,
			ratelimit.FixedWindow,
			ratelimit.SlidingWindowLog,
			ratelimit.SlidingWindowCounter,
		}
		for _, a := range algos {
			a := a
			t.Run(a.String(), func(t *testing.T) {
				t.Parallel()
				const limit = uint64(3)
				interval := time.Second
				st := algo.Fresh(a, limit, interval, base)
				for i := uint64(0); i < limit; i++ {
					res := algo.Take(
						a, limit, interval, base, &st,
					)
					if !res.OK {
						t.Fatalf(
							"allow #%d: got OK=false",
							i,
						)
					}
					if res.Limit != limit {
						t.Fatalf(
							"Limit: got %d, want %d",
							res.Limit,
							limit,
						)
					}
					if res.Remaining > limit {
						t.Fatalf(
							"Remaining %d > Limit %d",
							res.Remaining,
							limit,
						)
					}
					if res.Reset.IsZero() {
						t.Fatalf("Reset is zero")
					}
				}
				res := algo.Take(
					a, limit, interval, base, &st,
				)
				if res.OK {
					t.Fatalf("deny: got OK=true, want false")
				}
				if res.Remaining != 0 {
					t.Fatalf(
						"deny Remaining: got %d, want 0",
						res.Remaining,
					)
				}
				if res.Reset.IsZero() {
					t.Fatalf("deny Reset is zero")
				}
			})
		}
	})
}

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
