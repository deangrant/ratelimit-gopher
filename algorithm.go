package ratelimit

// Algorithm selects the rate limiting strategy used by a Store.
type Algorithm int

const (
	// TokenBucket refills continuously and allows bursts up to Tokens.
	TokenBucket Algorithm = iota
	// LeakyBucket meters requests at a constant drain rate and rejects
	// when the bucket level would exceed Tokens.
	LeakyBucket
	// FixedWindow counts requests in fixed Interval windows.
	FixedWindow
	// SlidingWindowLog stores request timestamps for precise limiting.
	SlidingWindowLog
	// SlidingWindowCounter approximates a sliding window with O(1) memory.
	SlidingWindowCounter
)

// Valid reports whether a is a supported algorithm.
func (a Algorithm) Valid() bool {
	switch a {
	case TokenBucket,
		LeakyBucket,
		FixedWindow,
		SlidingWindowLog,
		SlidingWindowCounter:
		return true
	default:
		return false
	}
}

// String returns the algorithm name.
func (a Algorithm) String() string {
	switch a {
	case TokenBucket:
		return "token_bucket"
	case LeakyBucket:
		return "leaky_bucket"
	case FixedWindow:
		return "fixed_window"
	case SlidingWindowLog:
		return "sliding_window_log"
	case SlidingWindowCounter:
		return "sliding_window_counter"
	default:
		return "unknown"
	}
}
