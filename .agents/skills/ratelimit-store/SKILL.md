---
name: ratelimit-store
description: >-
  Apply ratelimit-gopher Store contracts, Result/Algorithm/MaxTokens semantics,
  Close/ErrStopped rules, and memory vs redisstore vs postgresstore asymmetries.
  Use when implementing or reviewing stores, algo/Lua parity, httplimit Taker
  middleware, config validation, or multi-module verify for this library.
trigger: >-
  Store, Take, Close, ErrStopped, MaxTokens, Algorithm, TokenBucket,
  redisstore, postgresstore, Lua parity, httplimit, Taker, SweepMinTTL
---

# ratelimit-store

Domain guidance for **this** repository. For Go style use skill
`google-go-style-guide`. For package/interface design use `solid-go-design`.

## Core API (`package ratelimit`)

Import: `github.com/deangrant/ratelimit-gopher` (package name **`ratelimit`**).

| Symbol | Contract |
| ------ | -------- |
| `Store` | Concurrent-safe `Take` / `Close`. Missing keys created lazily. Backend errors returned; fail-open vs closed is caller policy. |
| `Result` | `Limit`, `Remaining` (`uint64`), `Reset` (UTC), `OK` (`bool`). |
| `ErrStopped` | Returned by `Take` after `Close`. |
| `Close` | Idempotent. Marks stopped **before** waiting on owned resources. Aborted wait may return `ctx.Err()`; store stays stopped. Brief allow-after-stop race is documented. |
| `MaxTokens` | `(1<<53)-1`. Keep Tokens at or below this so float64 state stays exact. |
| `Algorithm` | TokenBucket (default), LeakyBucket (meter, not queue), FixedWindow, SlidingWindowLog, SlidingWindowCounter. Use `Valid()`. |

Keys are stored as provided. Scrub/HMAC sensitive IDs before `Take` when durable or shared.

## Implementations

| Package | Module | Notes |
| ------- | ------ | ----- |
| `memory` | core | Sharded; sweeper; uses `algo`. Interval `> 0`. |
| `noop` | core | Always allow until Close. |
| `redisstore` | own `go.mod` | Lua in `scripts.go`; Redis `TIME` clock; Interval **`>= 1ms`**; does not close Redis client. |
| `postgresstore` | own `go.mod` | Row lock + `algo.Take`; `New(ctx, cfg)`; optional migrate; does not close `*sql.DB`. |
| `httplimit` | core | Depends on **`Taker`** (`Take` only). Headers + 429/`Retry-After`; default fail-closed (500 / ErrStopped→503); `WithFailOpen`. |

Compile-time check: `var _ ratelimit.Store = (*Store)(nil)`.

## Algo vs Redis Lua

- Pure Go reference: package `algo` (`Fresh`, `Take`, `State`).
- Redis must match semantics in `redisstore/scripts.go`.
- After either side changes, run `.agents/commands/parity-redis.md` (`TestAlgoRedisParity`).
- Rule: `.agents/rules/algo-lua-parity.mdc`.

## Multi-module verify

Root `./...` skips `redisstore` and `postgresstore`. Use `.agents/commands/verify-all.md`.

## Do not

- Cite go-limiter or the BackendBytes article in the tree.
- Close caller-owned Redis/`*sql.DB` inside store `Close`.
- Require full `Store` in httplimit when only `Take` is needed.
- Change algorithm math in only one of `algo` / Lua.
