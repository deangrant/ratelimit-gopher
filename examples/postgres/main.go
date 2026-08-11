// PostgreSQL store example.
//
//	DATABASE_URL='postgres://localhost/app?sslmode=disable' go run .
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/deangrant/ratelimit-gopher/postgresstore"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("postgres ping: %v", err)
	}

	store, err := postgresstore.New(ctx, postgresstore.Config{
		DB:       db,
		Tokens:   20,
		Interval: time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close(ctx)

	res, err := store.Take(ctx, "user:42")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("ok=%v remaining=%d reset=%s\n",
		res.OK, res.Remaining, res.Reset.UTC().Format(time.RFC3339))
}
