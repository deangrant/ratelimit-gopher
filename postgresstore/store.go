package postgresstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/internal/algo"
)

// Compile-time check.
var _ ratelimit.Store = (*Store)(nil)

// Store is a PostgreSQL-backed rate limit store.
type Store struct {
	db          *sql.DB
	tokens      uint64
	interval    time.Duration
	algo        ratelimit.Algorithm
	sweepTTL    time.Duration
	qSelect     string
	qInsert     string
	qUpdate     string
	qCreate     string
	qIndex      string
	qDeleteIdle string
	stopped     atomic.Bool
	closeOnce   sync.Once
	stopSweep   chan struct{}
	sweepDone   chan struct{}
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
		sweepTTL: cfg.SweepMinTTL,
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
		qIndex: fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS %s_updated_at_idx ON %s (updated_at)`,
			cfg.Table,
			cfg.Table,
		), //nolint:gosec // G201
		qDeleteIdle: fmt.Sprintf(
			`DELETE FROM %s WHERE updated_at < $1`,
			cfg.Table,
		), //nolint:gosec // G201
		stopSweep: make(chan struct{}),
		sweepDone: make(chan struct{}),
	}
	if !cfg.SkipMigrate {
		if err := s.EnsureSchema(ctx); err != nil {
			return nil, err
		}
	}
	go s.sweepLoop()
	return s, nil
}

// EnsureSchema creates the bucket table and updated_at index
// if they do not exist.
func (s *Store) EnsureSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, s.qCreate); err != nil {
		return fmt.Errorf(
			"postgresstore: ensure schema: %w",
			err,
		)
	}
	if _, err := s.db.ExecContext(ctx, s.qIndex); err != nil {
		return fmt.Errorf(
			"postgresstore: ensure index: %w",
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

// Close stops the sweeper and rejects subsequent Take calls.
// Close marks the store stopped before waiting for the sweeper.
// The context bounds the wait; if cancelled, the store stays
// stopped and Close returns ctx.Err(). Close does not close the
// database handle. Close is idempotent.
func (s *Store) Close(ctx context.Context) error {
	var wait <-chan struct{}
	s.closeOnce.Do(func() {
		s.stopped.Store(true)
		close(s.stopSweep)
		wait = s.sweepDone
	})
	if wait == nil {
		if s.stopped.Load() {
			return nil
		}
		return ctx.Err()
	}
	select {
	case <-wait:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) sweepLoop() {
	defer close(s.sweepDone)
	ticker := time.NewTicker(s.sweepTTL)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopSweep:
			return
		case <-ticker.C:
			s.sweep()
		}
	}
}

func (s *Store) sweep() {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		s.sweepTTL,
	)
	defer cancel()
	cutoff := time.Now().UTC().Add(-s.sweepTTL)
	_ = s.deleteIdle(ctx, cutoff)
}

func (s *Store) deleteIdle(
	ctx context.Context,
	cutoff time.Time,
) error {
	_, err := s.db.ExecContext(ctx, s.qDeleteIdle, cutoff)
	if err != nil {
		return fmt.Errorf(
			"postgresstore: delete idle: %w",
			err,
		)
	}
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
