// Package httplimit provides net/http middleware for rate limiting.
package httplimit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"

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

// IPKeyFunc keys requests by the peer address in RemoteAddr.
// It does not read forwarded headers; those are client-controlled
// unless a trusted proxy rewrites them. Behind reverse proxies,
// either normalize the client IP into RemoteAddr upstream or use
// TrustedForwardedIPKeyFunc.
func IPKeyFunc() KeyFunc {
	return func(r *http.Request) (string, error) {
		return remoteIP(r)
	}
}

// TrustedForwardedIPKeyFunc keys by forwarded client IPs only when
// RemoteAddr is in trustedProxies. Otherwise it keys by RemoteAddr
// and ignores headers. trustedProxies are CIDR or single-IP strings.
// headers must be non-empty; the first non-empty header value wins.
// For comma-separated lists (X-Forwarded-For), addresses are walked
// right to left, skipping trusted hops, to find the client.
func TrustedForwardedIPKeyFunc(
	trustedProxies []string,
	headers ...string,
) (KeyFunc, error) {
	if len(trustedProxies) == 0 {
		return nil, errors.New(
			"httplimit: trustedProxies is empty",
		)
	}
	if len(headers) == 0 {
		return nil, errors.New(
			"httplimit: headers is empty",
		)
	}
	prefixes := make([]netip.Prefix, 0, len(trustedProxies))
	for _, s := range trustedProxies {
		p, err := parseTrustedPrefix(s)
		if err != nil {
			return nil, fmt.Errorf(
				"httplimit: trusted proxy %q: %w",
				s,
				err,
			)
		}
		prefixes = append(prefixes, p)
	}
	hdrs := append([]string(nil), headers...)
	return func(r *http.Request) (string, error) {
		peer, err := remoteIP(r)
		if err != nil {
			return "", err
		}
		peerAddr, err := netip.ParseAddr(peer)
		if err != nil {
			return "", fmt.Errorf(
				"httplimit: remote addr: %w",
				err,
			)
		}
		if !prefixContains(prefixes, peerAddr) {
			return peer, nil
		}
		for _, h := range hdrs {
			v := r.Header.Get(h)
			if v == "" {
				continue
			}
			client, ok, err := clientFromForwarded(
				v,
				prefixes,
			)
			if err != nil {
				return "", err
			}
			if ok {
				return client, nil
			}
			return peer, nil
		}
		return peer, nil
	}, nil
}

func remoteIP(r *http.Request) (string, error) {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return "", fmt.Errorf(
			"httplimit: remote addr: %w",
			err,
		)
	}
	return ip, nil
}

func parseTrustedPrefix(s string) (netip.Prefix, error) {
	if p, err := netip.ParsePrefix(s); err == nil {
		return p, nil
	}
	addr, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Prefix{}, err
	}
	return netip.PrefixFrom(addr, addr.BitLen()), nil
}

func prefixContains(
	prefixes []netip.Prefix,
	ip netip.Addr,
) bool {
	for _, p := range prefixes {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// clientFromForwarded walks a comma-separated forwarded list
// right to left, skipping trusted hops. ok is false when every
// hop was trusted (caller should fall back to the peer).
func clientFromForwarded(
	value string,
	trusted []netip.Prefix,
) (client string, ok bool, err error) {
	parts := strings.Split(value, ",")
	for i := len(parts) - 1; i >= 0; i-- {
		token := strings.TrimSpace(parts[i])
		if token == "" {
			continue
		}
		addr, err := netip.ParseAddr(token)
		if err != nil {
			return "", false, fmt.Errorf(
				"httplimit: forwarded addr %q: %w",
				token,
				err,
			)
		}
		if !prefixContains(trusted, addr) {
			return addr.String(), true, nil
		}
	}
	return "", false, nil
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
