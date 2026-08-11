// HTTP middleware example: rate-limit a handler by RemoteAddr.
//
//	go run ./examples/httplimit
//
// Then: curl -i http://127.0.0.1:8080/
package main

import (
	"log"
	"net/http"
	"time"

	"github.com/deangrant/ratelimit-gopher/httplimit"
	"github.com/deangrant/ratelimit-gopher/memory"
)

func main() {
	store, err := memory.New(memory.Config{
		Tokens:   100,
		Interval: time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}

	mw, err := httplimit.NewMiddleware(
		store,
		httplimit.IPKeyFunc(),
	)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})

	addr := ":8080"
	log.Printf("listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mw.Handle(mux)))
}
