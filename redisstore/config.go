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
	// Tokens is the maximum number of tokens (capacity or window limit).
	Tokens uint64
	// Interval is the refill period or window size.
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
	if c.Interval <= 0 {
		return errors.New(
			"redisstore: Interval must be > 0",
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
