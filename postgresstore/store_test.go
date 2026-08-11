package postgresstore_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/postgresstore"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("RATELIMIT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set RATELIMIT_POSTGRES_DSN to run postgresstore tests",
		)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	return db
}

func TestAlgorithmsAllowThenDeny(t *testing.T) {
	db := openTestDB(t)
	algos := []ratelimit.Algorithm{
		ratelimit.TokenBucket,
		ratelimit.LeakyBucket,
		ratelimit.FixedWindow,
		ratelimit.SlidingWindowLog,
		ratelimit.SlidingWindowCounter,
	}
	for _, algo := range algos {
		t.Run(algo.String(), func(t *testing.T) {
			table := "rl_test_" + algo.String()
			_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
			ctx := context.Background()
			s, err := postgresstore.New(ctx, postgresstore.Config{
				DB:        db,
				Tokens:    2,
				Interval:  time.Minute,
				Algorithm: algo,
				Table:     table,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() {
				_ = s.Close(context.Background())
				_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
			})

			for i := 0; i < 2; i++ {
				res, err := s.Take(ctx, "k")
				if err != nil {
					t.Fatalf("Take #%d: %v", i, err)
				}
				if !res.OK {
					t.Fatalf(
						"Take #%d: got OK=false, want true",
						i,
					)
				}
			}
			res, err := s.Take(ctx, "k")
			if err != nil {
				t.Fatalf("Take overflow: %v", err)
			}
			if res.OK {
				t.Fatalf(
					"Take overflow: got OK=true, want false",
				)
			}
		})
	}
}

func TestConcurrentTake(t *testing.T) {
	db := openTestDB(t)
	table := "rl_test_concurrent"
	_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
	const limit = 30
	ctx := context.Background()
	s, err := postgresstore.New(ctx, postgresstore.Config{
		DB:       db,
		Tokens:   limit,
		Interval: time.Minute,
		Table:    table,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close(context.Background())
		_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
	})

	var (
		mu      sync.Mutex
		allowed int
		wg      sync.WaitGroup
	)
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := s.Take(ctx, "shared")
			if err != nil {
				t.Errorf("Take: %v", err)
				return
			}
			if res.OK {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowed != limit {
		t.Fatalf(
			"allowed: got %d, want %d",
			allowed,
			limit,
		)
	}
}

func TestNewValidation(t *testing.T) {
	t.Parallel()
	_, err := postgresstore.New(
		context.Background(),
		postgresstore.Config{
			DB:       nil,
			Tokens:   1,
			Interval: time.Second,
		},
	)
	if err == nil {
		t.Fatalf("nil DB: got nil error")
	}
}

func TestConfigRejectsBadTable(t *testing.T) {
	t.Parallel()
	db := &sql.DB{}
	_, err := postgresstore.New(
		context.Background(),
		postgresstore.Config{
			DB:          db,
			Tokens:      1,
			Interval:    time.Second,
			Table:       "bad;drop",
			SkipMigrate: true,
		},
	)
	if err == nil {
		t.Fatalf("bad table: got nil error")
	}
}

func TestTakeAfterClose(t *testing.T) {
	t.Parallel()
	s, err := postgresstore.New(
		context.Background(),
		postgresstore.Config{
			DB:          &sql.DB{},
			Tokens:      1,
			Interval:    time.Second,
			Table:       "rl_close",
			SkipMigrate: true,
		},
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = s.Take(context.Background(), "k")
	if !errors.Is(err, ratelimit.ErrStopped) {
		t.Fatalf(
			"Take after Close: got %v, want %v",
			err,
			ratelimit.ErrStopped,
		)
	}
}

func TestCloseCancelledContextStillStops(t *testing.T) {
	t.Parallel()
	s, err := postgresstore.New(
		context.Background(),
		postgresstore.Config{
			DB:          &sql.DB{},
			Tokens:      1,
			Interval:    time.Second,
			Table:       "rl_close_cancel",
			SkipMigrate: true,
		},
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = s.Close(ctx) // may be nil or ctx.Err()
	_, err = s.Take(context.Background(), "k")
	if !errors.Is(err, ratelimit.ErrStopped) {
		t.Fatalf(
			"Take after cancelled Close: got %v, want %v",
			err,
			ratelimit.ErrStopped,
		)
	}
}

func TestNewCancelledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := postgresstore.New(ctx, postgresstore.Config{
		DB:       &sql.DB{},
		Tokens:   1,
		Interval: time.Second,
		Table:    "rl_cancelled",
	})
	// Migrate against a zero DB should fail; cancelled ctx
	// should surface during EnsureSchema ExecContext.
	if err == nil {
		t.Fatalf("New: got nil error, want error")
	}
}
