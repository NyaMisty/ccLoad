package app

import (
	"crypto/sha256"
	"errors"
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

// keyConcurrencyID avoids retaining plaintext credentials in the counter map.
type keyConcurrencyID struct {
	channelID int64
	keyHash   [sha256.Size]byte
}

// channelConcurrencyLimiter applies the channel setting to each API key independently.
type channelConcurrencyLimiter struct {
	mu     sync.Mutex
	active map[keyConcurrencyID]int
}

func newChannelConcurrencyLimiter() *channelConcurrencyLimiter {
	return &channelConcurrencyLimiter{
		active: make(map[keyConcurrencyID]int),
	}
}

func (l *channelConcurrencyLimiter) acquire(channelID int64, apiKey string, limit int) (release func(), active, max int, ok bool) {
	if l == nil || channelID <= 0 || limit <= 0 {
		return func() {}, 0, 0, true
	}

	id := keyConcurrencyID{
		channelID: channelID,
		keyHash:   sha256.Sum256([]byte(apiKey)),
	}

	l.mu.Lock()
	current := l.active[id]
	if current >= limit {
		l.mu.Unlock()
		return nil, current, limit, false
	}
	next := current + 1
	l.active[id] = next
	l.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			current := l.active[id]
			if current <= 1 {
				delete(l.active, id)
				return
			}
			l.active[id] = current - 1
		})
	}, next, limit, true
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
