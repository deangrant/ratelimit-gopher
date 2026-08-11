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
