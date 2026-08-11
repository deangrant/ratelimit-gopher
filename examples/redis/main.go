// Redis store example.
//
//	REDIS_ADDR=127.0.0.1:6379 go run .
//
// Defaults to 127.0.0.1:6379 when REDIS_ADDR is unset.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/redisstore"
	"github.com/redis/go-redis/v9"
)

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping %s: %v", addr, err)
	}

	store, err := redisstore.New(redisstore.Config{
		Client:    rdb,
		Tokens:    50,
		Interval:  time.Second,
		Algorithm: ratelimit.FixedWindow,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close(ctx)

	res, err := store.Take(ctx, "api:key")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("ok=%v remaining=%d reset=%s\n",
		res.OK, res.Remaining, res.Reset.UTC().Format(time.RFC3339))
}
