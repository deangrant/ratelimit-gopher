module github.com/deangrant/ratelimit-gopher/redisstore

go 1.26.1

replace github.com/deangrant/ratelimit-gopher => ../

require (
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/deangrant/ratelimit-gopher v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.22.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)
