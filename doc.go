// Package ratelimit defines a store-based rate limiting API suitable for
// application code and HTTP servers.
//
// Callers configure a Store with a token limit and reset interval, then call
// Take with a string key to learn whether a request is allowed, how many
// tokens remain, and when capacity replenishes.
//
// Keys are stored as provided. Scrub, hash, or HMAC sensitive identifiers
// before passing them to Take when the backing store is shared or durable.
package ratelimit
