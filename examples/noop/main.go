// Noop store example: unlimited Take until Close, then ErrStopped.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/noop"
)

func main() {
	store := noop.New()

	res, err := store.Take(context.Background(), "any")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("before close: ok=%v remaining=%d\n",
		res.OK, res.Remaining)

	if err := store.Close(context.Background()); err != nil {
		log.Fatal(err)
	}

	_, err = store.Take(context.Background(), "any")
	if !errors.Is(err, ratelimit.ErrStopped) {
		log.Fatalf("after close: got %v, want ErrStopped", err)
	}
	fmt.Println("after close: ErrStopped")
}
