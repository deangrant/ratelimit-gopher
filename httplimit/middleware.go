// Package httplimit provides net/http middleware for rate limiting.
package httplimit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"

	"github.com/deangrant/ratelimit-gopher"
)

const (
	// HeaderRateLimitLimit is the configured limit.
	HeaderRateLimitLimit = "X-RateLimit-Limit"
	// HeaderRateLimitRemaining is tokens left in the current window.
	HeaderRateLimitRemaining = "X-RateLimit-Remaining"
	// HeaderRateLimitReset is when the limit resets (HTTP-date, GMT).
	HeaderRateLimitReset = "X-RateLimit-Reset"
	// HeaderRetryAfter is when to retry after a 429 (HTTP-date, GMT).
	HeaderRetryAfter = "Retry-After"
)

// KeyFunc derives a rate limit key from an HTTP request.
type KeyFunc func(r *http.Request) (string, error)

// Taker is the rate-limit dependency needed by Middleware.
type Taker interface {
	Take(ctx context.Context, key string) (ratelimit.Result, error)
}

// IPKeyFunc keys requests by client IP. Optional headers are checked first
// (for example "X-Forwarded-For"). Header lookup is case-insensitive.
func IPKeyFunc(headers ...string) KeyFunc {
	return func(r *http.Request) (string, error) {
		for _, h := range headers {
			if v := r.Header.Get(h); v != "" {
				return v, nil
			}
		}
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			return "", fmt.Errorf(
				"httplimit: remote addr: %w",
				err,
			)
		}
		return ip, nil
	}
}

// Middleware rate limits HTTP handlers using a Taker and KeyFunc.
type Middleware struct {
	taker   Taker
	keyFunc KeyFunc
}

// NewMiddleware builds Middleware. taker and keyFunc must be non-nil.
func NewMiddleware(
	taker Taker,
	keyFunc KeyFunc,
) (*Middleware, error) {
	if taker == nil {
		return nil, errors.New("httplimit: store is nil")
	}
	if keyFunc == nil {
		return nil, errors.New(
			"httplimit: key function is nil",
		)
	}
	return &Middleware{taker: taker, keyFunc: keyFunc}, nil
}

// Handle wraps next with rate limiting.
func (m *Middleware) Handle(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, err := m.keyFunc(r)
		if err != nil {
			http.Error(
				w,
				http.StatusText(http.StatusInternalServerError),
				http.StatusInternalServerError,
			)
			return
		}

		res, err := m.taker.Take(r.Context(), key)
		if err != nil {
			http.Error(
				w,
				http.StatusText(http.StatusInternalServerError),
				http.StatusInternalServerError,
			)
			return
		}

		reset := res.Reset.UTC().Format(http.TimeFormat)
		w.Header().Set(
			HeaderRateLimitLimit,
			strconv.FormatUint(res.Limit, 10),
		)
		w.Header().Set(
			HeaderRateLimitRemaining,
			strconv.FormatUint(res.Remaining, 10),
		)
		w.Header().Set(HeaderRateLimitReset, reset)

		if !res.OK {
			w.Header().Set(HeaderRetryAfter, reset)
			http.Error(
				w,
				http.StatusText(http.StatusTooManyRequests),
				http.StatusTooManyRequests,
			)
			return
		}
		next.ServeHTTP(w, r)
	})
}
