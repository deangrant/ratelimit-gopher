// Memory store example: create a store, Take once, then Close.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/memory"
)

func main() {
	store, err := memory.New(memory.Config{
		Tokens:    10,
		Interval:  time.Minute,
		Algorithm: ratelimit.TokenBucket,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close(context.Background())

	res, err := store.Take(context.Background(), "user:42")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("ok=%v remaining=%d reset=%s\n",
		res.OK, res.Remaining, res.Reset.UTC().Format(time.RFC3339))
}
