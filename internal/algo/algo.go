// Package algo implements pure rate-limit algorithms shared by
// in-process stores. Redis Lua scripts in redisstore must keep
// matching semantics; change both when altering behavior.
package algo

import (
	"math"
	"time"

	"github.com/deangrant/ratelimit-gopher"
)

// Result is the outcome of one Take against algorithm state.
type Result struct {
	Limit     uint64
	Remaining uint64
	Reset     time.Time
	OK        bool
}

// State holds per-key limiter state for all algorithms.
type State struct {
	Tokens      float64
	Level       float64
	Count       int64
	Prev        int64
	WindowStart time.Time
	WindowEnd   time.Time
	UpdatedAt   time.Time
	LogTimes    []time.Time
	Accessed    time.Time
}

// Fresh returns initial state for algo at now.
func Fresh(
	algo ratelimit.Algorithm,
	tokens uint64,
	interval time.Duration,
	now time.Time,
) State {
	st := State{
		WindowStart: now,
		WindowEnd:   now.Add(interval),
		UpdatedAt:   now,
		Accessed:    now,
	}
	switch algo {
	case ratelimit.TokenBucket:
		st.Tokens = float64(tokens)
	case ratelimit.LeakyBucket:
		st.Level = 0
	default:
		st.Count = 0
		st.Prev = 0
	}
	return st
}

// Take applies one request to st and returns the decision.
func Take(
	algo ratelimit.Algorithm,
	limit uint64,
	interval time.Duration,
	now time.Time,
	st *State,
) Result {
	switch algo {
	case ratelimit.LeakyBucket:
		return takeLeaky(limit, interval, now, st)
	case ratelimit.FixedWindow:
		return takeFixed(limit, interval, now, st)
	case ratelimit.SlidingWindowLog:
		return takeSlidingLog(limit, interval, now, st)
	case ratelimit.SlidingWindowCounter:
		return takeSlidingCounter(limit, interval, now, st)
	default:
		return takeToken(limit, interval, now, st)
	}
}

func takeToken(
	limit uint64,
	interval time.Duration,
	now time.Time,
	st *State,
) Result {
	capacity := float64(limit)
	rate := capacity / interval.Seconds()
	elapsed := now.Sub(st.UpdatedAt).Seconds()
	if elapsed > 0 {
		st.Tokens = math.Min(
			capacity,
			st.Tokens+elapsed*rate,
		)
	}
	st.UpdatedAt = now
	st.Accessed = now
	ok := st.Tokens >= 1
	if ok {
		st.Tokens--
	}
	needed := math.Max(0, 1-st.Tokens)
	reset := resetAfter(now, needed/rate)
	rem := uint64(0)
	if st.Tokens > 0 {
		rem = uint64(math.Floor(st.Tokens))
	}
	return Result{
		Limit:     limit,
		Remaining: rem,
		Reset:     reset,
		OK:        ok,
	}
}

func takeLeaky(
	limit uint64,
	interval time.Duration,
	now time.Time,
	st *State,
) Result {
	capacity := float64(limit)
	rate := capacity / interval.Seconds()
	elapsed := now.Sub(st.UpdatedAt).Seconds()
	if elapsed > 0 {
		st.Level = math.Max(0, st.Level-elapsed*rate)
	}
	st.UpdatedAt = now
	st.Accessed = now
	ok := st.Level+1 <= capacity
	if ok {
		st.Level++
	}
	over := math.Max(0, st.Level-(capacity-1))
	reset := resetAfter(now, over/rate)
	rem := uint64(0)
	left := capacity - st.Level
	if left > 0 {
		rem = uint64(math.Floor(left))
	}
	return Result{
		Limit:     limit,
		Remaining: rem,
		Reset:     reset,
		OK:        ok,
	}
}

// resetAfter returns now plus waitSeconds, rounding the wait up
// to whole milliseconds to match redisstore Lua Reset.
func resetAfter(now time.Time, waitSeconds float64) time.Time {
	if waitSeconds <= 0 {
		return now.UTC()
	}
	ms := math.Ceil(waitSeconds * 1000)
	return now.Add(time.Duration(ms) * time.Millisecond).UTC()
}

func takeFixed(
	limit uint64,
	interval time.Duration,
	now time.Time,
	st *State,
) Result {
	if !now.Before(st.WindowEnd) {
		elapsed := now.Sub(st.WindowEnd)
		windows := elapsed/interval + 1
		st.WindowEnd = st.WindowEnd.Add(windows * interval)
		st.Count = 0
	}
	st.UpdatedAt = now
	st.Accessed = now
	count := uint64Count(st.Count)
	ok := count < limit
	if ok {
		st.Count++
		count++
	}
	rem := uint64(0)
	if count < limit {
		rem = limit - count
	}
	return Result{
		Limit:     limit,
		Remaining: rem,
		Reset:     st.WindowEnd.UTC(),
		OK:        ok,
	}
}

func takeSlidingLog(
	limit uint64,
	interval time.Duration,
	now time.Time,
	st *State,
) Result {
	cutoff := now.Add(-interval)
	kept := st.LogTimes[:0]
	for _, ts := range st.LogTimes {
		if !ts.Before(cutoff) {
			kept = append(kept, ts)
		}
	}
	st.LogTimes = kept
	st.UpdatedAt = now
	st.Accessed = now
	ok := uint64(len(st.LogTimes)) < limit
	if ok {
		st.LogTimes = append(st.LogTimes, now)
	}
	reset := now.UTC()
	if len(st.LogTimes) > 0 {
		reset = st.LogTimes[0].Add(interval).UTC()
	}
	rem := uint64(0)
	if uint64(len(st.LogTimes)) < limit {
		rem = limit - uint64(len(st.LogTimes))
	}
	return Result{
		Limit:     limit,
		Remaining: rem,
		Reset:     reset,
		OK:        ok,
	}
}

func takeSlidingCounter(
	limit uint64,
	interval time.Duration,
	now time.Time,
	st *State,
) Result {
	elapsed := now.Sub(st.WindowStart)
	if elapsed >= interval {
		windows := int(elapsed / interval)
		if windows == 1 {
			st.Prev = st.Count
			st.Count = 0
			st.WindowStart = st.WindowStart.Add(interval)
			elapsed = now.Sub(st.WindowStart)
		} else {
			st.Prev = 0
			st.Count = 0
			st.WindowStart = now
			elapsed = 0
		}
	}
	frac := 1.0 - (float64(elapsed) / float64(interval))
	if frac < 0 {
		frac = 0
	}
	curr := uint64Count(st.Count)
	prev := uint64Count(st.Prev)
	weighted := float64(curr) + float64(prev)*frac
	st.UpdatedAt = now
	st.Accessed = now
	ok := weighted < float64(limit)
	if ok {
		st.Count++
	}
	rem := uint64(0)
	if ok {
		left := float64(limit) - (weighted + 1)
		if left > 0 {
			rem = uint64(math.Floor(left))
		}
	}
	return Result{
		Limit:     limit,
		Remaining: rem,
		Reset:     st.WindowStart.Add(interval).UTC(),
		OK:        ok,
	}
}

func uint64Count(n int64) uint64 {
	if n <= 0 {
		return 0
	}
	return uint64(n)
}
