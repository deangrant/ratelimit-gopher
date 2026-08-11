package ratelimit_test

import (
	"testing"

	"github.com/deangrant/ratelimit-gopher"
)

func TestAlgorithmString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		a     ratelimit.Algorithm
		want  string
		valid bool
	}{
		{ratelimit.TokenBucket, "token_bucket", true},
		{ratelimit.LeakyBucket, "leaky_bucket", true},
		{ratelimit.FixedWindow, "fixed_window", true},
		{ratelimit.SlidingWindowLog, "sliding_window_log", true},
		{
			ratelimit.SlidingWindowCounter,
			"sliding_window_counter",
			true,
		},
		{ratelimit.Algorithm(99), "unknown", false},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			t.Parallel()
			if got := tt.a.String(); got != tt.want {
				t.Fatalf(
					"String: got %q, want %q",
					got,
					tt.want,
				)
			}
			if got := tt.a.Valid(); got != tt.valid {
				t.Fatalf(
					"Valid: got %v, want %v",
					got,
					tt.valid,
				)
			}
		})
	}
}
