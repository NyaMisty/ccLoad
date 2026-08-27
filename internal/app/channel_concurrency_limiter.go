package app

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"

	"ccLoad/internal/model"
)

type keyConcurrencyExceededError struct {
	active int
	limit  int
}

func (e *keyConcurrencyExceededError) Error() string {
	return ErrKeyConcurrencyExceeded.Error()
}

func (e *keyConcurrencyExceededError) Unwrap() error {
	return ErrKeyConcurrencyExceeded
}

type keyConcurrencyExhaustedError struct {
	cause               error
	checkedKeys         int
	totalKeys           int
	concurrencyLimited  int
	upstreamAttempts    int
	maxUpstreamAttempts int
	perKeyLimit         int
}

func (e *keyConcurrencyExhaustedError) Error() string {
	return fmt.Sprintf(
		"Key 检查=%d/%d，并发满载=%d，上游尝试=%d/%d，单 Key 上限=%d",
		e.checkedKeys,
		e.totalKeys,
		e.concurrencyLimited,
		e.upstreamAttempts,
		e.maxUpstreamAttempts,
		e.perKeyLimit,
	)
}

func (e *keyConcurrencyExhaustedError) Unwrap() error {
	if e.cause != nil {
		return e.cause
	}
	return ErrKeyConcurrencyExceeded
}

// keyConcurrencyID avoids retaining plaintext credentials in the counter maps.
type keyConcurrencyID struct {
	channelID int64
	keyHash   [sha256.Size]byte
}

func newKeyConcurrencyID(channelID int64, apiKey string) keyConcurrencyID {
	return keyConcurrencyID{
		channelID: channelID,
		keyHash:   sha256.Sum256([]byte(apiKey)),
	}
}

type channelConcurrencyLimiter struct {
	mu      sync.Mutex
	active  map[keyConcurrencyID]int
	changed map[keyConcurrencyID]chan struct{}
}

func newChannelConcurrencyLimiter() *channelConcurrencyLimiter {
	return &channelConcurrencyLimiter{
		active:  make(map[keyConcurrencyID]int),
		changed: make(map[keyConcurrencyID]chan struct{}),
	}
}

func (l *channelConcurrencyLimiter) acquire(channelID int64, apiKey string, limit int) (release func(), active, max int, ok bool) {
	if l == nil || channelID <= 0 || limit <= 0 {
		return func() {}, 0, 0, true
	}
	id := newKeyConcurrencyID(channelID, apiKey)

	l.mu.Lock()
	current := l.active[id]
	if current >= limit {
		l.mu.Unlock()
		return nil, current, limit, false
	}
	next := current + 1
	l.active[id] = next
	l.mu.Unlock()

	return l.releaseFunc(id), next, limit, true
}

func (l *channelConcurrencyLimiter) acquireContext(ctx context.Context, channelID int64, apiKey string, limit int) (func(), error) {
	if l == nil || channelID <= 0 || limit <= 0 {
		return func() {}, nil
	}
	id := newKeyConcurrencyID(channelID, apiKey)

	for {
		l.mu.Lock()
		current := l.active[id]
		if current < limit {
			l.active[id] = current + 1
			l.mu.Unlock()
			return l.releaseFunc(id), nil
		}
		changed := l.changed[id]
		if changed == nil {
			changed = make(chan struct{})
			l.changed[id] = changed
		}
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (l *channelConcurrencyLimiter) releaseFunc(id keyConcurrencyID) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			current := l.active[id]
			if current <= 1 {
				delete(l.active, id)
			} else {
				l.active[id] = current - 1
			}
			if changed := l.changed[id]; changed != nil {
				close(changed)
				delete(l.changed, id)
			}
			l.mu.Unlock()
		})
	}
}

func (s *Server) acquireKeyConcurrencySlot(cfg *model.Config, apiKey string) (release func(), err error) {
	if cfg == nil || cfg.MaxConcurrency <= 0 {
		return func() {}, nil
	}
	if s == nil || s.channelConcurrencyLimiter == nil {
		return func() {}, nil
	}

	release, active, limit, ok := s.channelConcurrencyLimiter.acquire(cfg.ID, apiKey, cfg.MaxConcurrency)
	if ok {
		return release, nil
	}
	return nil, &keyConcurrencyExceededError{active: active, limit: limit}
}

func (s *Server) waitForKeyConcurrencySlot(ctx context.Context, cfg *model.Config, apiKey string) (func(), error) {
	if cfg == nil || cfg.MaxConcurrency <= 0 {
		return func() {}, nil
	}
	if s == nil || s.channelConcurrencyLimiter == nil {
		return func() {}, nil
	}
	return s.channelConcurrencyLimiter.acquireContext(ctx, cfg.ID, apiKey, cfg.MaxConcurrency)
}

type releaseOnCloseReadCloser struct {
	io.ReadCloser
	release func()
	once    sync.Once
}

func (rc *releaseOnCloseReadCloser) Close() error {
	var closeErr error
	rc.once.Do(func() {
		closeErr = rc.ReadCloser.Close()
		if rc.release != nil {
			rc.release()
		}
	})
	return closeErr
}

func keyConcurrencyLimit(err error) (active, limit int, ok bool) {
	var concurrencyErr *keyConcurrencyExceededError
	if errors.As(err, &concurrencyErr) {
		return concurrencyErr.active, concurrencyErr.limit, true
	}
	return 0, 0, false
}

func keyConcurrencySkipReason(err error) string {
	var exhaustedErr *keyConcurrencyExhaustedError
	if errors.As(err, &exhaustedErr) {
		return exhaustedErr.Error()
	}
	if active, limit, ok := keyConcurrencyLimit(err); ok {
		return fmt.Sprintf("当前 Key 活跃并发=%d，单 Key 上限=%d", active, limit)
	}
	return "未提供并发限制明细"
}
