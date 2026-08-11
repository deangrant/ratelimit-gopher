# parity-redis

Guard algo ↔ Redis Lua drift after changing `algo/` or `redisstore/scripts.go`.

## Steps

1. From the repo root:

```bash
go test -C redisstore -run TestAlgoRedisParity -race -count=1
```

2. If it fails, align `algo` and `redisstore/scripts.go` (and related helpers) until the test is green.
3. Prefer also running full `redisstore` tests when the change is non-trivial:

```bash
go test -C redisstore ./... -race -count=1
```
