package postgresstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/internal/algo"
)

// Compile-time check.
var _ ratelimit.Store = (*Store)(nil)

// Store is a PostgreSQL-backed rate limit store.
type Store struct {
	db       *sql.DB
	tokens   uint64
	interval time.Duration
	algo     ratelimit.Algorithm
	qSelect  string
	qInsert  string
	qUpdate  string
	qCreate  string
	stopped  atomic.Bool
}

// New creates a PostgreSQL-backed Store from cfg. When
// SkipMigrate is false, EnsureSchema runs with ctx.
func New(
	ctx context.Context,
	cfg Config,
) (*Store, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	s := &Store{
		db:       cfg.DB,
		tokens:   cfg.Tokens,
		interval: cfg.Interval,
		algo:     cfg.Algorithm,
		// Table names are validated by validateIdent before use.
		qSelect: fmt.Sprintf(`
SELECT tokens, level, count, prev,
       window_start, window_end, updated_at, log_times
FROM %s WHERE key = $1 FOR UPDATE`, cfg.Table), //nolint:gosec // G201
		qInsert: fmt.Sprintf(`
INSERT INTO %s (
  key, tokens, level, count, prev,
  window_start, window_end, updated_at, log_times
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb
) ON CONFLICT (key) DO NOTHING`, cfg.Table), //nolint:gosec // G201
		qUpdate: fmt.Sprintf(`
UPDATE %s SET
  tokens = $2,
  level = $3,
  count = $4,
  prev = $5,
  window_start = $6,
  window_end = $7,
  updated_at = $8,
  log_times = $9::jsonb
WHERE key = $1`, cfg.Table), //nolint:gosec // G201
		qCreate: fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  key TEXT PRIMARY KEY,
  tokens DOUBLE PRECISION NOT NULL DEFAULT 0,
  level DOUBLE PRECISION NOT NULL DEFAULT 0,
  count BIGINT NOT NULL DEFAULT 0,
  prev BIGINT NOT NULL DEFAULT 0,
  window_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  window_end TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  log_times JSONB NOT NULL DEFAULT '[]'
)`, cfg.Table), //nolint:gosec // G201
	}
	if !cfg.SkipMigrate {
		if err := s.EnsureSchema(ctx); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// EnsureSchema creates the bucket table if it does not exist.
func (s *Store) EnsureSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, s.qCreate); err != nil {
		return fmt.Errorf(
			"postgresstore: ensure schema: %w",
			err,
		)
	}
	return nil
}

// Take takes one token for key if available.
func (s *Store) Take(
	ctx context.Context,
	key string,
) (ratelimit.Result, error) {
	if err := ctx.Err(); err != nil {
		return ratelimit.Result{}, err
	}
	if s.stopped.Load() {
		return ratelimit.Result{}, ratelimit.ErrStopped
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ratelimit.Result{}, fmt.Errorf(
			"postgresstore: begin: %w",
			err,
		)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	st, err := s.lockRow(ctx, tx, key, now)
	if err != nil {
		return ratelimit.Result{}, err
	}

	r := algo.Take(s.algo, s.tokens, s.interval, now, &st)
	if err := s.saveRow(ctx, tx, key, st); err != nil {
		return ratelimit.Result{}, err
	}
	if err := tx.Commit(); err != nil {
		return ratelimit.Result{}, fmt.Errorf(
			"postgresstore: commit: %w",
			err,
		)
	}
	return ratelimit.Result{
		Limit:     r.Limit,
		Remaining: r.Remaining,
		Reset:     r.Reset,
		OK:        r.OK,
	}, nil
}

// Close marks the store stopped before any wait. It does not
// close the database handle. Close is idempotent; later Take
// calls return ErrStopped. This backend does not wait on
// owned resources.
func (s *Store) Close(_ context.Context) error {
	s.stopped.Store(true)
	return nil
}

func (s *Store) lockRow(
	ctx context.Context,
	tx *sql.Tx,
	key string,
	now time.Time,
) (algo.State, error) {
	var st algo.State
	var logRaw []byte
	err := tx.QueryRowContext(ctx, s.qSelect, key).Scan(
		&st.Tokens,
		&st.Level,
		&st.Count,
		&st.Prev,
		&st.WindowStart,
		&st.WindowEnd,
		&st.UpdatedAt,
		&logRaw,
	)
	if err == nil {
		st.LogTimes, err = decodeLogTimes(logRaw)
		if err != nil {
			return algo.State{}, err
		}
		st.Accessed = st.UpdatedAt
		return st, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return algo.State{}, fmt.Errorf(
			"postgresstore: select: %w",
			err,
		)
	}

	st = algo.Fresh(s.algo, s.tokens, s.interval, now)
	logJSON, err := encodeLogTimes(st.LogTimes)
	if err != nil {
		return algo.State{}, err
	}
	if _, err := tx.ExecContext(
		ctx,
		s.qInsert,
		key,
		st.Tokens,
		st.Level,
		st.Count,
		st.Prev,
		st.WindowStart,
		st.WindowEnd,
		st.UpdatedAt,
		string(logJSON),
	); err != nil {
		return algo.State{}, fmt.Errorf(
			"postgresstore: insert: %w",
			err,
		)
	}

	err = tx.QueryRowContext(ctx, s.qSelect, key).Scan(
		&st.Tokens,
		&st.Level,
		&st.Count,
		&st.Prev,
		&st.WindowStart,
		&st.WindowEnd,
		&st.UpdatedAt,
		&logRaw,
	)
	if err != nil {
		return algo.State{}, fmt.Errorf(
			"postgresstore: reselect: %w",
			err,
		)
	}
	st.LogTimes, err = decodeLogTimes(logRaw)
	if err != nil {
		return algo.State{}, err
	}
	st.Accessed = st.UpdatedAt
	return st, nil
}

func (s *Store) saveRow(
	ctx context.Context,
	tx *sql.Tx,
	key string,
	st algo.State,
) error {
	logJSON, err := encodeLogTimes(st.LogTimes)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(
		ctx,
		s.qUpdate,
		key,
		st.Tokens,
		st.Level,
		st.Count,
		st.Prev,
		st.WindowStart,
		st.WindowEnd,
		st.UpdatedAt,
		string(logJSON),
	)
	if err != nil {
		return fmt.Errorf("postgresstore: update: %w", err)
	}
	return nil
}

func encodeLogTimes(times []time.Time) ([]byte, error) {
	if times == nil {
		times = []time.Time{}
	}
	unix := make([]int64, len(times))
	for i, t := range times {
		unix[i] = t.UTC().UnixNano()
	}
	b, err := json.Marshal(unix)
	if err != nil {
		return nil, fmt.Errorf(
			"postgresstore: encode log: %w",
			err,
		)
	}
	return b, nil
}

func decodeLogTimes(raw []byte) ([]time.Time, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var unix []int64
	if err := json.Unmarshal(raw, &unix); err != nil {
		return nil, fmt.Errorf(
			"postgresstore: decode log: %w",
			err,
		)
	}
	out := make([]time.Time, len(unix))
	for i, n := range unix {
		out[i] = time.Unix(0, n).UTC()
	}
	return out, nil
}
