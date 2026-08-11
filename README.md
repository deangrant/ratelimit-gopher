# ratelimit-gopher

ratelimit-gopher is a Go library for **store-based rate limiting**. Use it in
application code and with `net/http` servers.

Callers configure a store with a token limit and a reset interval, then call
`Take` with a string key. The result says whether the request is allowed, how
many tokens remain, and when capacity replenishes.

## Features

- Five algorithms: token bucket, leaky bucket, fixed window, sliding-window log,
  and sliding-window counter.
- A small `Store` interface (`Take` / `Close`) shared by all backends.
- In-memory and noop stores in the core module (no third-party dependencies).
- Optional Redis and PostgreSQL stores in separate modules.
- HTTP middleware with standard rate-limit headers and `429` responses.

## Requirements

- Go 1.26.1 or later (see `go.mod`).
- The core module has no required third-party dependencies.
- Redis and PostgreSQL backends are separate modules with their own
  dependencies (`go-redis`, `pgx` via `database/sql`).

## Install

Add the core module.

```bash
go get github.com/deangrant/ratelimit-gopher@latest
```

Add an optional store module when you need it.

```bash
go get github.com/deangrant/ratelimit-gopher/redisstore@latest
go get github.com/deangrant/ratelimit-gopher/postgresstore@latest
```

The module path contains a hyphen. The Go **package name** for the core API is
`ratelimit`.

## Concepts

1. Create a store with a capacity (`Tokens`), an `Interval`, and an
   `Algorithm`.
2. Call `Take(ctx, key)` for each logical client or resource key.
3. Read `Result.OK`, `Remaining`, `Limit`, and `Reset`.
4. Call `Close(ctx)` when you shut down. Later `Take` calls return
   `ratelimit.ErrStopped`.

Keys are stored as provided. Scrub, hash, or HMAC sensitive identifiers before
`Take` when the backing store is shared or durable.

Redis and PostgreSQL clients are owned by the caller. Store `Close` does not
close them.

## Algorithms

Set `Algorithm` on the store config. The zero value is `TokenBucket`.

| Constant | Behavior |
| -------- | -------- |
| `TokenBucket` | Refills continuously. Allows bursts up to `Tokens`. |
| `LeakyBucket` | Meters at a constant drain rate. Rejects when full (not a request queue). |
| `FixedWindow` | Counts requests in fixed `Interval` windows. |
| `SlidingWindowLog` | Stores request timestamps for precise limiting. |
| `SlidingWindowCounter` | Approximates a sliding window with O(1) memory. |

Use `Algorithm.Valid()` when you accept algorithm values from config.

## Usage

Runnable programs live under [`examples/`](examples/).

| Topic | Path | Run |
| ----- | ---- | --- |
| Memory | [`examples/memory`](examples/memory) | `go run ./examples/memory` |
| HTTP | [`examples/httplimit`](examples/httplimit) | `go run ./examples/httplimit` |
| Noop | [`examples/noop`](examples/noop) | `go run ./examples/noop` |
| Redis | [`examples/redis`](examples/redis) | `go run ./examples/redis` |
| Postgres | [`examples/postgres`](examples/postgres) | `go run ./examples/postgres` |

Redis defaults to `REDIS_ADDR=127.0.0.1:6379`. Postgres requires `DATABASE_URL`.

### Memory store

1. Create a store with `memory.New`.
2. Call `Take` with a key.
3. Close the store on shutdown.

See [`examples/memory`](examples/memory) for a full program.

```go
store, err := memory.New(memory.Config{
	Tokens:    10,
	Interval:  time.Minute,
	Algorithm: ratelimit.TokenBucket,
})
res, err := store.Take(context.Background(), "user:42")
```

### HTTP middleware

1. Build any `Store` (it satisfies `httplimit.Taker`).
2. Create middleware with a key function such as `IPKeyFunc`.
3. Wrap your handler. Denied requests get `429` and `Retry-After`.

Headers set on every successful take decision:

- `X-RateLimit-Limit`
- `X-RateLimit-Remaining`
- `X-RateLimit-Reset`

By default the middleware fails closed on Take errors (`500`), except
`ErrStopped` which returns `503`. Use `httplimit.WithFailOpen()` to allow the
request when Take fails.

`IPKeyFunc` keys by `RemoteAddr` only. Behind a trusted reverse proxy, use
`TrustedForwardedIPKeyFunc` with an allowlist of proxy CIDRs.

See [`examples/httplimit`](examples/httplimit) (`go run ./examples/httplimit`, then
`curl -i http://127.0.0.1:8080/`).

### Noop store

Use `noop.New` when you want unlimited takes (tests or temporarily disabled
limiting). After `Close`, Take returns `ErrStopped`.

See [`examples/noop`](examples/noop).

### Redis store

The Redis backend lives in module
`github.com/deangrant/ratelimit-gopher/redisstore`.

- Pass a `redis.Cmdable`. You own the client and must close it.
- `Interval` must be at least `1ms` (script and `PEXPIRE` resolution).
- Algorithms match the pure-Go `algo` package via Lua scripts.

See [`examples/redis`](examples/redis).

### PostgreSQL store

The PostgreSQL backend lives in module
`github.com/deangrant/ratelimit-gopher/postgresstore`.

- Pass a `*sql.DB`. You own the handle and must close it.
- `New(ctx, cfg)` runs schema migration unless `SkipMigrate` is true.
- Default table name is `ratelimit_buckets`.

See [`examples/postgres`](examples/postgres).

## Limits and constraints

- `Tokens` must be in `1..ratelimit.MaxTokens`. `MaxTokens` is
  `(1<<53)-1` so capacities stay exact in float64-backed state.
- Memory and Postgres require `Interval > 0`. Redis requires
  `Interval >= 1ms`.
- `Close` marks the store stopped before waiting on owned background work.
  The context bounds that wait. The store stays stopped even if the wait is
  aborted.
- A brief allow-after-stop race is possible for in-flight takes that observed
  the store as open. Drain callers outside the store if you need a hard cut.

## Package layout

| Import path | Package | Role |
| ----------- | ------- | ---- |
| `github.com/deangrant/ratelimit-gopher` | `ratelimit` | `Store`, `Result`, `Algorithm`, `ErrStopped`, `MaxTokens` |
| `github.com/deangrant/ratelimit-gopher/algo` | `algo` | Shared pure-Go algorithm math |
| `github.com/deangrant/ratelimit-gopher/memory` | `memory` | In-process store |
| `github.com/deangrant/ratelimit-gopher/noop` | `noop` | Always-allow store |
| `github.com/deangrant/ratelimit-gopher/httplimit` | `httplimit` | HTTP middleware |
| `github.com/deangrant/ratelimit-gopher/redisstore` | `redisstore` | Redis store (separate module) |
| `github.com/deangrant/ratelimit-gopher/postgresstore` | `postgresstore` | PostgreSQL store (separate module) |

## Development

Contributor and agent conventions live in [AGENTS.md](AGENTS.md).

To build, race-test, and lint all three modules the way CI does, follow
[`.agents/commands/verify-all.md`](.agents/commands/verify-all.md). Root
`go test ./...` alone does not cover `redisstore` or `postgresstore`.

## License

MIT. See [LICENSE](LICENSE).
