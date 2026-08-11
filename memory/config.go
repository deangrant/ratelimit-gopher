package memory

import (
	"errors"
	"fmt"
	"time"

	"github.com/deangrant/ratelimit-gopher"
)

// Config configures an in-memory rate limit store.
type Config struct {
	// Tokens is the maximum number of tokens (capacity or window
	// limit). Must be in 1..ratelimit.MaxTokens.
	Tokens uint64
	// Interval is the refill period or window size.
	Interval time.Duration
	// Algorithm selects the limiting strategy. Zero value is TokenBucket.
	Algorithm ratelimit.Algorithm
	// SweepMinTTL is how long an idle key is kept before eviction.
	// If zero, defaults to 3 * Interval.
	SweepMinTTL time.Duration
}

func (c Config) withDefaults() Config {
	if c.SweepMinTTL <= 0 {
		c.SweepMinTTL = 3 * c.Interval
	}
	return c
}

func (c Config) validate() error {
	if c.Tokens == 0 {
		return errors.New("memory: Tokens must be > 0")
	}
	if c.Tokens > ratelimit.MaxTokens {
		return fmt.Errorf(
			"memory: Tokens must be <= %d",
			ratelimit.MaxTokens,
		)
	}
	if c.Interval <= 0 {
		return errors.New("memory: Interval must be > 0")
	}
	if !c.Algorithm.Valid() {
		return fmt.Errorf(
			"memory: unsupported Algorithm %d",
			c.Algorithm,
		)
	}
	return nil
}
