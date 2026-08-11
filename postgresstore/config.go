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
	// Must be a lowercase unquoted PostgreSQL identifier starting
	// with a letter, not a reserved word, and at most 47 characters
	// (so the updated_at index name fits in 63 bytes).
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
	// Leave room for "_updated_at_idx" within PG's 63-byte limit.
	const maxTableLen = 47
	if len(name) > maxTableLen {
		return fmt.Errorf(
			"identifier too long (max %d)",
			maxTableLen,
		)
	}
	for i, r := range name {
		ok := r >= 'a' && r <= 'z' ||
			r >= '0' && r <= '9' ||
			r == '_'
		if i == 0 {
			ok = r >= 'a' && r <= 'z'
		}
		if !ok {
			return fmt.Errorf("invalid identifier %q", name)
		}
	}
	if _, reserved := reservedIdents[name]; reserved {
		return fmt.Errorf("reserved identifier %q", name)
	}
	return nil
}

// reservedIdents are PostgreSQL keywords unsafe as unquoted
// table names in our interpolated DDL.
var reservedIdents = map[string]struct{}{
	"all": {}, "analyse": {}, "analyze": {}, "and": {},
	"any": {}, "array": {}, "as": {}, "asc": {},
	"asymmetric": {}, "authorization": {}, "binary": {},
	"both": {}, "case": {}, "cast": {}, "check": {},
	"collate": {}, "collation": {}, "column": {},
	"concurrently": {}, "constraint": {}, "create": {},
	"cross": {}, "current_catalog": {}, "current_date": {},
	"current_role": {}, "current_schema": {},
	"current_time": {}, "current_timestamp": {},
	"current_user": {}, "default": {}, "deferrable": {},
	"desc": {}, "distinct": {}, "do": {}, "else": {},
	"end": {}, "except": {}, "false": {}, "fetch": {},
	"for": {}, "foreign": {}, "freeze": {}, "from": {},
	"full": {}, "grant": {}, "group": {}, "having": {},
	"ilike": {}, "in": {}, "initially": {}, "inner": {},
	"intersect": {}, "into": {}, "is": {}, "isnull": {},
	"join": {}, "key": {}, "lateral": {}, "leading": {},
	"left": {}, "like": {}, "limit": {}, "localtime": {},
	"localtimestamp": {}, "natural": {}, "not": {},
	"notnull": {}, "null": {}, "offset": {}, "on": {},
	"only": {}, "or": {}, "order": {}, "outer": {},
	"overlaps": {}, "placing": {}, "primary": {},
	"references": {}, "returning": {}, "right": {},
	"select": {}, "session_user": {}, "similar": {},
	"some": {}, "symmetric": {}, "table": {},
	"tablesample": {}, "then": {}, "to": {}, "trailing": {},
	"true": {}, "union": {}, "unique": {}, "user": {},
	"using": {}, "variadic": {}, "verbose": {}, "when": {},
	"where": {}, "window": {}, "with": {}, "index": {},
}
