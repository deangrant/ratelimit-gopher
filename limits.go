package ratelimit

// MaxTokens is the largest Tokens value that keeps integer
// capacity exact in float64 (IEEE-754 binary64). Token/leaky
// state, Redis Lua, and Postgres DOUBLE PRECISION all use that
// format; larger limits lose unit resolution.
const MaxTokens = (1 << 53) - 1 // 9007199254740991
