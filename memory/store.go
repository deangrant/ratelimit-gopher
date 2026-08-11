package memory

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deangrant/ratelimit-gopher"
)

// Compile-time check.
var _ ratelimit.Store = (*Store)(nil)

const numShards = 64

// Store is an in-memory rate limit store.
type Store struct {
	tokens    uint64
	interval  time.Duration
	algo      ratelimit.Algorithm
	sweepTTL  time.Duration
	clock     clock
	shards    [numShards]shard
	stopped   atomic.Bool
	closeOnce sync.Once
	stopSweep chan struct{}
	sweepDone chan struct{}
}

type shard struct {
	mu   sync.Mutex
	data map[string]*bucket
}

// New creates an in-memory Store from cfg.
func New(cfg Config) (*Store, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	s := &Store{
		tokens:    cfg.Tokens,
		interval:  cfg.Interval,
		algo:      cfg.Algorithm,
		sweepTTL:  cfg.SweepMinTTL,
		clock:     realClock{},
		stopSweep: make(chan struct{}),
		sweepDone: make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i].data = make(map[string]*bucket)
	}
	go s.sweepLoop()
	return s, nil
}

// Take takes one token for key if available.
func (s *Store) Take(
	ctx context.Context,
	key string,
) (ratelimit.Result, error) {
	if err := ctx.Err(); err != nil {
		return ratelimit.Result{}, err
	}
	if s.stopped.Load() {
		return ratelimit.Result{}, ratelimit.ErrStopped
	}

	now := s.clock.Now()
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	b, ok := sh.data[key]
	if !ok {
		b = newBucket(s.algo, s.tokens, s.interval, now)
		sh.data[key] = b
	}
	return b.take(now), nil
}

// Close stops the sweeper and rejects subsequent Take calls.
// Close marks the store stopped before waiting for the sweeper.
// The context bounds the wait; if cancelled, the store stays stopped
// and Close returns ctx.Err(). Close is idempotent.
func (s *Store) Close(ctx context.Context) error {
	var wait <-chan struct{}
	s.closeOnce.Do(func() {
		s.stopped.Store(true)
		close(s.stopSweep)
		wait = s.sweepDone
	})
	if wait == nil {
		return nil
	}
	select {
	case <-wait:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) shardFor(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &s.shards[h.Sum32()%numShards]
}

func (s *Store) sweepLoop() {
	defer close(s.sweepDone)
	ticker := time.NewTicker(s.sweepTTL)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopSweep:
			return
		case <-ticker.C:
			s.sweep()
		}
	}
}

func (s *Store) sweep() {
	cutoff := s.clock.Now().Add(-s.sweepTTL)
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for k, b := range sh.data {
			if b.lastAccess().Before(cutoff) {
				delete(sh.data, k)
			}
		}
		sh.mu.Unlock()
	}
}

// setClock replaces the clock. For tests only.
func (s *Store) setClock(c clock) {
	s.clock = c
}
