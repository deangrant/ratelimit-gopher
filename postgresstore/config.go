// Package postgresstore provides a PostgreSQL-backed rate limit Store.
package postgresstore

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/deangrant/ratelimit-gopher"
)

// Config configures a PostgreSQL-backed rate limit store.
type Config struct {
	// DB is the database handle. The caller owns it and must close it.
	DB *sql.DB
	// Tokens is the maximum number of tokens (capacity or window limit).
	Tokens uint64
	// Interval is the refill period or window size.
	Interval time.Duration
	// Algorithm selects the limiting strategy. Zero value is TokenBucket.
	Algorithm ratelimit.Algorithm
	// Table is the bucket table name. Defaults to "ratelimit_buckets".
	Table string
	// SweepMinTTL is how long an idle key is kept before eviction.
	// If zero, defaults to 3 * Interval.
	SweepMinTTL time.Duration
	// SkipMigrate disables CREATE TABLE in New. When false (default),
	// New runs EnsureSchema with the provided context.
	SkipMigrate bool
}

func (c Config) withDefaults() Config {
	if c.Table == "" {
		c.Table = "ratelimit_buckets"
	}
	if c.SweepMinTTL <= 0 {
		c.SweepMinTTL = 3 * c.Interval
	}
	return c
}

func (c Config) validate() error {
	if c.DB == nil {
		return errors.New("postgresstore: DB is nil")
	}
	if c.Tokens == 0 {
		return errors.New("postgresstore: Tokens must be > 0")
	}
	if c.Interval <= 0 {
		return errors.New(
			"postgresstore: Interval must be > 0",
		)
	}
	if err := validateIdent(c.Table); err != nil {
		return fmt.Errorf("postgresstore: Table: %w", err)
	}
	if !c.Algorithm.Valid() {
		return fmt.Errorf(
			"postgresstore: unsupported Algorithm %d",
			c.Algorithm,
		)
	}
	return nil
}

func validateIdent(name string) error {
	if name == "" {
		return errors.New("empty table name")
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '_' {
			continue
		}
		return fmt.Errorf("invalid identifier %q", name)
	}
	return nil
}
