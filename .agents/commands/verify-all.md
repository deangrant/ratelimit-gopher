# verify-all

Run the multi-module build, race tests, and golangci-lint fmt/run loop that CI expects. Do not stop after the root module.

## Steps

1. From the repo root, with Go on `PATH` (or `/usr/local/go/bin`):

```bash
go build ./...
go build -C redisstore ./...
go build -C postgresstore ./...

go test ./... -race -count=1
go test -C redisstore ./... -race -count=1
go test -C postgresstore ./... -race -count=1

for d in . redisstore postgresstore; do
  (cd "$d" && golangci-lint fmt --diff ./...)
  (cd "$d" && golangci-lint run ./...)
done
```

2. All three modules must pass. Root `./...` alone is insufficient.
3. Report any failing package and fix before claiming done.
