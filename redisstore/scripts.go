package redisstore

import (
	"fmt"

	"github.com/deangrant/ratelimit-gopher"
)

// Lua scripts encode the same algorithm semantics as
// github.com/deangrant/ratelimit-gopher/algo. Keep them
// in sync when changing limiter math. now is always Redis TIME
// (milliseconds) so multi-node apps share one clock.

func scriptFor(algo ratelimit.Algorithm) (string, error) {
	if !algo.Valid() {
		return "", fmt.Errorf(
			"redisstore: unsupported Algorithm %d",
			algo,
		)
	}
	switch algo {
	case ratelimit.TokenBucket:
		return luaTokenBucket, nil
	case ratelimit.LeakyBucket:
		return luaLeakyBucket, nil
	case ratelimit.FixedWindow:
		return luaFixedWindow, nil
	case ratelimit.SlidingWindowLog:
		return luaSlidingLog, nil
	case ratelimit.SlidingWindowCounter:
		return luaSlidingCounter, nil
	default:
		return luaTokenBucket, nil
	}
}

// redisNowMS is shared Lua that sets now from Redis TIME.
const redisNowMS = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
`

// ARGV: capacity, rate, interval_ms, ttl_ms
// Returns: allowed, limit, remaining, reset_ms
const luaTokenBucket = redisNowMS + `
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local ttl = tonumber(ARGV[4])

local data = redis.call('HMGET', KEYS[1], 'tokens', 'last')
local tokens = tonumber(data[1])
local last = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  last = now
end

local elapsed = math.max(0, (now - last) / 1000.0)
tokens = math.min(capacity, tokens + elapsed * rate)

local allowed = 0
local remaining = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
  remaining = math.floor(tokens)
else
  remaining = 0
end

local needed = math.max(0, 1 - tokens)
local reset_ms = now + math.ceil((needed / rate) * 1000)

redis.call('HSET', KEYS[1], 'tokens', tokens, 'last', now)
redis.call('PEXPIRE', KEYS[1], ttl)
return {allowed, capacity, remaining, reset_ms}
`

const luaLeakyBucket = redisNowMS + `
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local ttl = tonumber(ARGV[4])

local data = redis.call('HMGET', KEYS[1], 'level', 'last')
local level = tonumber(data[1])
local last = tonumber(data[2])
if level == nil then
  level = 0
  last = now
end

local elapsed = math.max(0, (now - last) / 1000.0)
level = math.max(0, level - elapsed * rate)

local allowed = 0
local remaining = 0
if level + 1 <= capacity then
  level = level + 1
  allowed = 1
  remaining = math.floor(capacity - level)
else
  remaining = 0
end

local over = math.max(0, level - (capacity - 1))
local reset_ms = now + math.ceil((over / rate) * 1000)

redis.call('HSET', KEYS[1], 'level', level, 'last', now)
redis.call('PEXPIRE', KEYS[1], ttl)
return {allowed, capacity, remaining, reset_ms}
`

const luaFixedWindow = redisNowMS + `
local limit = tonumber(ARGV[1])
local interval = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local data = redis.call('HMGET', KEYS[1], 'count', 'end')
local count = tonumber(data[1])
local window_end = tonumber(data[2])
if count == nil or window_end == nil or now >= window_end then
  count = 0
  if window_end == nil or now >= window_end then
    if window_end == nil then
      window_end = now + interval
    else
      local elapsed = now - window_end
      local windows = math.floor(elapsed / interval) + 1
      window_end = window_end + windows * interval
    end
  end
end

local allowed = 0
local remaining = 0
if count < limit then
  count = count + 1
  allowed = 1
  remaining = limit - count
else
  remaining = 0
end

redis.call('HSET', KEYS[1], 'count', count, 'end', window_end)
redis.call('PEXPIRE', KEYS[1], ttl)
return {allowed, limit, remaining, window_end}
`

const luaSlidingLog = redisNowMS + `
local limit = tonumber(ARGV[1])
local interval = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local cutoff = now - interval
-- Exclusive max matches Go !ts.Before(cutoff): keep score == cutoff.
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. cutoff)
local count = redis.call('ZCARD', KEYS[1])

local allowed = 0
local remaining = 0
if count < limit then
  redis.call('ZADD', KEYS[1], now, tostring(now) .. '-' .. tostring(count))
  count = count + 1
  allowed = 1
  remaining = limit - count
else
  remaining = 0
end

local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local reset_ms = now
if #oldest >= 2 then
  reset_ms = tonumber(oldest[2]) + interval
end

redis.call('PEXPIRE', KEYS[1], ttl)
return {allowed, limit, remaining, reset_ms}
`

const luaSlidingCounter = redisNowMS + `
local limit = tonumber(ARGV[1])
local interval = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local data = redis.call('HMGET', KEYS[1], 'curr', 'prev', 'start')
local curr = tonumber(data[1])
local prev = tonumber(data[2])
local start = tonumber(data[3])
if curr == nil then
  curr = 0
  prev = 0
  start = now
end

local elapsed = now - start
if elapsed >= interval then
  local windows = math.floor(elapsed / interval)
  if windows == 1 then
    prev = curr
    curr = 0
    start = start + interval
    elapsed = now - start
  else
    prev = 0
    curr = 0
    start = now
    elapsed = 0
  end
end

local frac = 1.0 - (elapsed / interval)
if frac < 0 then frac = 0 end
local weighted = curr + prev * frac

local allowed = 0
local remaining = 0
if weighted < limit then
  curr = curr + 1
  allowed = 1
  local left = limit - (weighted + 1)
  if left > 0 then
    remaining = math.floor(left)
  end
else
  remaining = 0
end

local reset_ms = start + interval
redis.call('HSET', KEYS[1], 'curr', curr, 'prev', prev, 'start', start)
redis.call('PEXPIRE', KEYS[1], ttl)
return {allowed, limit, remaining, reset_ms}
`
