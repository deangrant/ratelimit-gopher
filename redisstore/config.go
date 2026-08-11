// Package redisstore provides a Redis-backed rate limit Store.
package redisstore

import (
	"errors"
	"fmt"
	"time"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/redis/go-redis/v9"
)

// Config configures a Redis-backed rate limit store.
type Config struct {
	// Client is the Redis client. The caller owns it and must close it.
	Client redis.Cmdable
	// Tokens is the maximum number of tokens (capacity or window
	// limit). Must be in 1..ratelimit.MaxTokens.
	Tokens uint64
	// Interval is the refill period or window size.
	// Must be at least 1ms; Redis scripts and PEXPIRE use
	// millisecond resolution.
	Interval time.Duration
	// Algorithm selects the limiting strategy. Zero value is TokenBucket.
	Algorithm ratelimit.Algorithm
	// KeyPrefix namespaces Redis keys. Defaults to "ratelimit:".
	KeyPrefix string
}

func (c Config) withDefaults() Config {
	if c.KeyPrefix == "" {
		c.KeyPrefix = "ratelimit:"
	}
	return c
}

func (c Config) validate() error {
	if c.Client == nil {
		return errors.New("redisstore: Client is nil")
	}
	if c.Tokens == 0 {
		return errors.New("redisstore: Tokens must be > 0")
	}
	if c.Tokens > ratelimit.MaxTokens {
		return fmt.Errorf(
			"redisstore: Tokens must be <= %d",
			ratelimit.MaxTokens,
		)
	}
	if c.Interval < time.Millisecond {
		return errors.New(
			"redisstore: Interval must be >= 1ms",
		)
	}
	if !c.Algorithm.Valid() {
		return fmt.Errorf(
			"redisstore: unsupported Algorithm %d",
			c.Algorithm,
		)
	}
	return nil
}
