package httplimit_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/deangrant/ratelimit-gopher"
	"github.com/deangrant/ratelimit-gopher/httplimit"
	"github.com/deangrant/ratelimit-gopher/memory"
)

func TestMiddlewareAllowsAndSetsHeaders(t *testing.T) {
	t.Parallel()
	store, err := memory.New(memory.Config{
		Tokens:   5,
		Interval: time.Minute,
	})
	if err != nil {
		t.Fatalf("memory.New: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close(context.Background())
	})

	mw, err := httplimit.NewMiddleware(
		store,
		httplimit.IPKeyFunc(),
	)
	if err != nil {
		t.Fatalf("NewMiddleware: %v", err)
	}

	called := false
	h := mw.Handle(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		},
	))

	req := httptest.NewRequestWithContext(context.Background(),
		http.MethodGet,
		"/",
		nil,
	)
	req.RemoteAddr = "203.0.113.10:1234"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatalf("next handler was not called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rec.Code)
	}
	if got := rec.Header().Get(httplimit.HeaderRateLimitLimit); got != "5" {
		t.Fatalf(
			"Limit header: got %q, want %q",
			got,
			"5",
		)
	}
	if got := rec.Header().Get(
		httplimit.HeaderRateLimitRemaining,
	); got == "" {
		t.Fatalf("Remaining header missing")
	}
	if got := rec.Header().Get(
		httplimit.HeaderRateLimitReset,
	); got == "" {
		t.Fatalf("Reset header missing")
	} else if _, err := time.Parse(http.TimeFormat, got); err != nil {
		t.Fatalf(
			"Reset header format: got %q, want HTTP-date: %v",
			got,
			err,
		)
	}
}

func TestMiddlewareBlocksWithRetryAfter(t *testing.T) {
	t.Parallel()
	store, err := memory.New(memory.Config{
		Tokens:   1,
		Interval: time.Minute,
	})
	if err != nil {
		t.Fatalf("memory.New: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close(context.Background())
	})

	mw, err := httplimit.NewMiddleware(
		store,
		func(*http.Request) (string, error) {
			return "same", nil
		},
	)
	if err != nil {
		t.Fatalf("NewMiddleware: %v", err)
	}

	h := mw.Handle(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
	))

	req := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"/",
		nil,
	)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first status: got %d, want 200", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf(
			"second status: got %d, want 429",
			rec.Code,
		)
	}
	if got := rec.Header().Get(httplimit.HeaderRetryAfter); got == "" {
		t.Fatalf("Retry-After header missing")
	} else if _, err := time.Parse(http.TimeFormat, got); err != nil {
		t.Fatalf(
			"Retry-After format: got %q, want HTTP-date: %v",
			got,
			err,
		)
	}
	reset := rec.Header().Get(httplimit.HeaderRateLimitReset)
	if _, err := time.Parse(http.TimeFormat, reset); err != nil {
		t.Fatalf(
			"Reset format: got %q, want HTTP-date: %v",
			reset,
			err,
		)
	}
}

func TestMiddlewareKeyFuncError(t *testing.T) {
	t.Parallel()
	store, err := memory.New(memory.Config{
		Tokens:   1,
		Interval: time.Minute,
	})
	if err != nil {
		t.Fatalf("memory.New: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close(context.Background())
	})

	mw, err := httplimit.NewMiddleware(
		store,
		func(*http.Request) (string, error) {
			return "", errors.New("boom")
		},
	)
	if err != nil {
		t.Fatalf("NewMiddleware: %v", err)
	}

	called := false
	h := mw.Handle(http.HandlerFunc(
		func(_ http.ResponseWriter, _ *http.Request) {
			called = true
		},
	))
	rec := httptest.NewRecorder()
	h.ServeHTTP(
		rec,
		httptest.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			"/",
			nil,
		),
	)
	if called {
		t.Fatalf("next handler called on key error")
	}
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf(
			"status: got %d, want 500",
			rec.Code,
		)
	}
}

func TestIPKeyFunc(t *testing.T) {
	t.Parallel()
	fn := httplimit.IPKeyFunc()
	req := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"/",
		nil,
	)
	req.Header.Set("X-Forwarded-For", "198.51.100.7")
	req.RemoteAddr = "203.0.113.1:9"
	key, err := fn(req)
	if err != nil {
		t.Fatalf("IPKeyFunc: %v", err)
	}
	if key != "203.0.113.1" {
		t.Fatalf(
			"key: got %q, want RemoteAddr host (ignore XFF)",
			key,
		)
	}

	req2 := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"/",
		nil,
	)
	req2.RemoteAddr = "203.0.113.2:9"
	key, err = fn(req2)
	if err != nil {
		t.Fatalf("IPKeyFunc remote: %v", err)
	}
	if key != "203.0.113.2" {
		t.Fatalf(
			"key: got %q, want %q",
			key,
			"203.0.113.2",
		)
	}
}

func TestTrustedForwardedIPKeyFunc(t *testing.T) {
	t.Parallel()
	fn, err := httplimit.TrustedForwardedIPKeyFunc(
		[]string{"10.0.0.0/8"},
		"X-Forwarded-For",
	)
	if err != nil {
		t.Fatalf("TrustedForwardedIPKeyFunc: %v", err)
	}

	req := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"/",
		nil,
	)
	req.RemoteAddr = "10.0.0.1:9"
	req.Header.Set(
		"X-Forwarded-For",
		"198.51.100.7, 10.0.0.1",
	)
	key, err := fn(req)
	if err != nil {
		t.Fatalf("trusted peer: %v", err)
	}
	if key != "198.51.100.7" {
		t.Fatalf(
			"trusted peer key: got %q, want %q",
			key,
			"198.51.100.7",
		)
	}

	spoof := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"/",
		nil,
	)
	spoof.RemoteAddr = "203.0.113.50:9"
	spoof.Header.Set("X-Forwarded-For", "198.51.100.7")
	key, err = fn(spoof)
	if err != nil {
		t.Fatalf("untrusted peer: %v", err)
	}
	if key != "203.0.113.50" {
		t.Fatalf(
			"untrusted peer key: got %q, want RemoteAddr",
			key,
		)
	}

	emptyHdr := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"/",
		nil,
	)
	emptyHdr.RemoteAddr = "10.0.0.2:9"
	emptyHdr.Header.Set("X-Forwarded-For", "")
	key, err = fn(emptyHdr)
	if err != nil {
		t.Fatalf("empty forwarded header: %v", err)
	}
	if key != "10.0.0.2" {
		t.Fatalf(
			"empty XFF key: got %q, want RemoteAddr",
			key,
		)
	}
}

func TestTrustedForwardedIPKeyFuncValidation(t *testing.T) {
	t.Parallel()
	if _, err := httplimit.TrustedForwardedIPKeyFunc(
		nil,
		"X-Forwarded-For",
	); err == nil {
		t.Fatalf("empty trustedProxies: got nil error")
	}
	if _, err := httplimit.TrustedForwardedIPKeyFunc(
		[]string{"10.0.0.0/8"},
	); err == nil {
		t.Fatalf("empty headers: got nil error")
	}
	if _, err := httplimit.TrustedForwardedIPKeyFunc(
		[]string{"not-a-cidr"},
		"X-Forwarded-For",
	); err == nil {
		t.Fatalf("invalid CIDR: got nil error")
	}
}

func TestNewMiddlewareNilArgs(t *testing.T) {
	t.Parallel()
	if _, err := httplimit.NewMiddleware(
		nil,
		httplimit.IPKeyFunc(),
	); err == nil {
		t.Fatalf("nil taker: got nil error")
	}
	store := &stubStore{}
	if _, err := httplimit.NewMiddleware(
		store,
		nil,
	); err == nil {
		t.Fatalf("nil keyFunc: got nil error")
	}
}

func TestMiddlewareErrStoppedReturns503(t *testing.T) {
	t.Parallel()
	mw, err := httplimit.NewMiddleware(
		&errStore{err: ratelimit.ErrStopped},
		func(*http.Request) (string, error) {
			return "k", nil
		},
	)
	if err != nil {
		t.Fatalf("NewMiddleware: %v", err)
	}
	called := false
	h := mw.Handle(http.HandlerFunc(
		func(http.ResponseWriter, *http.Request) {
			called = true
		},
	))
	rec := httptest.NewRecorder()
	h.ServeHTTP(
		rec,
		httptest.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			"/",
			nil,
		),
	)
	if called {
		t.Fatalf("next handler called on ErrStopped")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"status: got %d, want 503",
			rec.Code,
		)
	}
}

func TestMiddlewareTakeErrorReturns500(t *testing.T) {
	t.Parallel()
	mw, err := httplimit.NewMiddleware(
		&errStore{err: errors.New("redis down")},
		func(*http.Request) (string, error) {
			return "k", nil
		},
	)
	if err != nil {
		t.Fatalf("NewMiddleware: %v", err)
	}
	called := false
	h := mw.Handle(http.HandlerFunc(
		func(http.ResponseWriter, *http.Request) {
			called = true
		},
	))
	rec := httptest.NewRecorder()
	h.ServeHTTP(
		rec,
		httptest.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			"/",
			nil,
		),
	)
	if called {
		t.Fatalf("next handler called on Take error")
	}
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf(
			"status: got %d, want 500",
			rec.Code,
		)
	}
}

func TestMiddlewareFailOpenAllowsOnTakeError(t *testing.T) {
	t.Parallel()
	mw, err := httplimit.NewMiddleware(
		&errStore{err: errors.New("redis down")},
		func(*http.Request) (string, error) {
			return "k", nil
		},
		httplimit.WithFailOpen(),
	)
	if err != nil {
		t.Fatalf("NewMiddleware: %v", err)
	}
	called := false
	h := mw.Handle(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		},
	))
	rec := httptest.NewRecorder()
	h.ServeHTTP(
		rec,
		httptest.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			"/",
			nil,
		),
	)
	if !called {
		t.Fatalf("next handler not called with FailOpen")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf(
			"status: got %d, want 200",
			rec.Code,
		)
	}
}

type stubStore struct{}

func (stubStore) Take(
	context.Context,
	string,
) (ratelimit.Result, error) {
	return ratelimit.Result{OK: true, Limit: 1}, nil
}

type errStore struct {
	err error
}

func (s *errStore) Take(
	context.Context,
	string,
) (ratelimit.Result, error) {
	return ratelimit.Result{}, s.err
}
