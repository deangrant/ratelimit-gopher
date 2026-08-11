package postgresstore

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestValidateIdent(t *testing.T) {
	t.Parallel()
	long48 := "a" + strings.Repeat("x", 47)
	tests := []struct {
		name    string
		ident   string
		wantErr bool
	}{
		{name: "ok", ident: "ratelimit_buckets", wantErr: false},
		{name: "ok_alnum", ident: "rl_a1", wantErr: false},
		{name: "empty", ident: "", wantErr: true},
		{name: "spaces", ident: "bad name", wantErr: true},
		{name: "inject", ident: "t;drop", wantErr: true},
		{name: "leading_digit", ident: "1abc", wantErr: true},
		{name: "mixed_case", ident: "User", wantErr: true},
		{name: "reserved_select", ident: "select", wantErr: true},
		{name: "reserved_user", ident: "user", wantErr: true},
		{name: "leading_underscore", ident: "_leading", wantErr: true},
		{name: "too_long", ident: long48, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateIdent(tt.ident)
			if tt.wantErr && err == nil {
				t.Fatalf("got nil error, want error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("got %v, want nil", err)
			}
		})
	}
}

func TestEncodeDecodeLogTimes(t *testing.T) {
	t.Parallel()
	in := []time.Time{
		time.Unix(0, 100).UTC(),
		time.Unix(0, 200).UTC(),
	}
	raw, err := encodeLogTimes(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := decodeLogTimes(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("len: got %d, want %d", len(out), len(in))
	}
	for i := range in {
		if !out[i].Equal(in[i]) {
			t.Fatalf(
				"[%d]: got %v, want %v",
				i,
				out[i],
				in[i],
			)
		}
	}
}

func TestWithDefaultsSweepMinTTL(t *testing.T) {
	t.Parallel()
	cfg := Config{
		DB:       &sql.DB{},
		Tokens:   1,
		Interval: time.Second,
	}.withDefaults()
	want := 3 * time.Second
	if cfg.SweepMinTTL != want {
		t.Fatalf(
			"SweepMinTTL: got %v, want %v",
			cfg.SweepMinTTL,
			want,
		)
	}
	cfg = Config{
		DB:          &sql.DB{},
		Tokens:      1,
		Interval:    time.Second,
		SweepMinTTL: 5 * time.Second,
	}.withDefaults()
	if cfg.SweepMinTTL != 5*time.Second {
		t.Fatalf(
			"SweepMinTTL override: got %v, want 5s",
			cfg.SweepMinTTL,
		)
	}
}

func TestDeleteIdle(t *testing.T) {
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

	const table = "rl_test_delete_idle"
	_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
	s, err := New(ctx, Config{
		DB:          db,
		Tokens:      1,
		Interval:    time.Minute,
		Table:       table,
		SweepMinTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Close(context.Background())
		_, _ = db.Exec("DROP TABLE IF EXISTS " + table)
	})

	if _, err := s.Take(ctx, "idle"); err != nil {
		t.Fatalf("Take: %v", err)
	}
	_, err = db.Exec(
		`UPDATE `+table+` SET updated_at = $1 WHERE key = $2`,
		time.Now().UTC().Add(-2*time.Hour),
		"idle",
	)
	if err != nil {
		t.Fatalf("age row: %v", err)
	}
	if err := s.deleteIdle(
		ctx,
		time.Now().UTC().Add(-time.Hour),
	); err != nil {
		t.Fatalf("deleteIdle: %v", err)
	}
	var n int
	err = db.QueryRow(
		`SELECT COUNT(*) FROM `+table+` WHERE key = $1`,
		"idle",
	).Scan(&n)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("idle row still present, count=%d", n)
	}
}
