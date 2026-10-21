package app

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"ccLoad/internal/model"
)

type keyConcurrencyExceededError struct {
	active int
	limit  int
	// preempted 表示槽位本有空闲，但被更高优先级等待者的反偷窃规则拦下。
	preempted bool
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
	// channelWaiters 是渠道级 Key 槽位优先等待队列（按渠道分组）。
	// 释放时把槽位精确授予最高优先级等待者；存在严格更高优先级等待者时，
	// 新到请求不得获取空闲槽位（反偷窃）。
	channelWaiters map[int64][]*channelSlotWaiter
	channelSeq    atomic.Uint64
}

func newChannelConcurrencyLimiter() *channelConcurrencyLimiter {
	return &channelConcurrencyLimiter{
		active:        make(map[keyConcurrencyID]int),
		changed:       make(map[keyConcurrencyID]chan struct{}),
		channelWaiters: make(map[int64][]*channelSlotWaiter),
	}
}

// channelSlotWaiter 是渠道 Key 槽位等待队列中的一项。
// ready 在被授予槽位时关闭；granted 等待者**保留在队列**直到它真实 acquire 到槽位
// 或放弃——被授予但尚未落座的等待者仍然占用优先级，新到请求不得偷走它的槽位。
// abandoned 标记等待者已放弃（ctx 取消），惰性清理。
type channelSlotWaiter struct {
	priority  int
	seq       uint64
	ready     chan struct{}
	granted   bool
	acquired  bool
	abandoned bool
}

// enqueueChannelSlotWait 入队等待该渠道的下一个释放槽位。
// 返回 true 表示被授予（本调用方可直接重试获取槽位，成功获取后槽位真正落座）；
// false 表示 ctx 结束（取消或 500ms 轮询节拍到期），调用方自行决定重试或放弃。
// 被授予但未落座的等待者保留在队列里持有优先级，防止授予→落座间隙被新到者偷槽。
func (l *channelConcurrencyLimiter) enqueueChannelSlotWait(ctx context.Context, channelID int64, priority int) *channelSlotWaiter {
	if l == nil || channelID <= 0 {
		return nil
	}

	l.mu.Lock()
	waiter := &channelSlotWaiter{
		priority: priority,
		seq:      l.channelSeq.Add(1),
		ready:    make(chan struct{}),
	}
	l.channelWaiters[channelID] = append(l.channelWaiters[channelID], waiter)
	l.mu.Unlock()

	select {
	case <-waiter.ready:
		return waiter
	case <-ctx.Done():
		l.mu.Lock()
		waiter.abandoned = true
		l.pruneChannelWaitersLocked(channelID)
		l.mu.Unlock()
		return nil
	}
}

// settleChannelSlotWaiter 在被授予的等待者真实 acquire 到槽位（acquired=true）
// 或最终放弃（acquired=false，超时/换渠道）时调用，解除它对队列的优先级占用。
func (l *channelConcurrencyLimiter) settleChannelSlotWaiter(waiter *channelSlotWaiter, acquired bool) {
	if l == nil || waiter == nil {
		return
	}
	l.mu.Lock()
	waiter.acquired = acquired
	l.pruneChannelWaitersOnSettleLocked(waiter)
	l.mu.Unlock()
}

// grantBestChannelWaiterLocked 把刚释放的槽位授予该渠道优先级最高（同优先级最早入队）的等待者。
// 必须持 l.mu 调用；一次释放只授予一人。被授予者保留在队列（granted=true）直到落座或放弃。
func (l *channelConcurrencyLimiter) grantBestChannelWaiterLocked(channelID int64) {
	waiters := l.channelWaiters[channelID]
	best := -1
	for i, w := range waiters {
		if w.abandoned || w.granted {
			continue
		}
		if best < 0 ||
			w.priority > waiters[best].priority ||
			(w.priority == waiters[best].priority && w.seq < waiters[best].seq) {
			best = i
		}
	}
	if best < 0 {
		return
	}
	waiters[best].granted = true
	close(waiters[best].ready)
}

// hasHigherChannelWaiterLocked 报告该渠道是否存在严格更高优先级的有效等待者
// （含已被授予但尚未落座的：它的槽位不容偷取）。
// 仅严格更高才构成反偷窃：同级到达允许竞速，
// 避免同优先级洪峰在无更高等待者时自我串行化。
func (l *channelConcurrencyLimiter) hasHigherChannelWaiterLocked(channelID int64, priority int) bool {
	for _, w := range l.channelWaiters[channelID] {
		if !w.abandoned && !w.acquired && w.priority > priority {
			return true
		}
	}
	return false
}

// pruneChannelWaitersLocked 移除已放弃/已落座的等待者（惰性清理）。
// granted 但未落座的等待者必须保留：它仍持有优先级占用，直到 settle。
func (l *channelConcurrencyLimiter) pruneChannelWaitersLocked(channelID int64) {
	waiters := l.channelWaiters[channelID]
	kept := 0
	for _, w := range waiters {
		if w.abandoned || w.acquired {
			continue
		}
		waiters[kept] = w
		kept++
	}
	if kept == 0 {
		delete(l.channelWaiters, channelID)
		return
	}
	l.channelWaiters[channelID] = waiters[:kept]
}

// pruneChannelWaitersOnSettleLocked 在等待者落座/放弃后把它从队列移除。
func (l *channelConcurrencyLimiter) pruneChannelWaitersOnSettleLocked(waiter *channelSlotWaiter) {
	for channelID, waiters := range l.channelWaiters {
		for i, w := range waiters {
			if w != waiter {
				continue
			}
			l.channelWaiters[channelID] = append(waiters[:i], waiters[i+1:]...)
			if len(l.channelWaiters[channelID]) == 0 {
				delete(l.channelWaiters, channelID)
			}
			return
		}
	}
}

func (l *channelConcurrencyLimiter) acquire(channelID int64, apiKey string, limit int, priority int) (release func(), active, max int, preempted, ok bool) {
	if l == nil || channelID <= 0 || limit <= 0 {
		return func() {}, 0, 0, false, true
	}
	id := newKeyConcurrencyID(channelID, apiKey)

	l.mu.Lock()
	current := l.active[id]
	if current >= limit {
		l.mu.Unlock()
		return nil, current, limit, false, false
	}
	// 反偷窃：空闲容量属于正在排队的更高优先级等待者，新到者不得插队获取。
	if l.hasHigherChannelWaiterLocked(channelID, priority) {
		l.mu.Unlock()
		return nil, current, limit, true, false
	}
	next := current + 1
	l.active[id] = next
	l.mu.Unlock()

	return l.releaseFunc(id), next, limit, false, true
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
			// 槽位释放：精确授予该渠道优先级最高的等待者（授予式，不广播竞速）。
			l.grantBestChannelWaiterLocked(id.channelID)
			l.mu.Unlock()
		})
	}
}

func (s *Server) acquireKeyConcurrencySlot(cfg *model.Config, apiKey string, priority int) (release func(), err error) {
	if cfg == nil || cfg.MaxConcurrency <= 0 {
		return func() {}, nil
	}
	if s == nil || s.channelConcurrencyLimiter == nil {
		return func() {}, nil
	}

	release, active, limit, preempted, ok := s.channelConcurrencyLimiter.acquire(cfg.ID, apiKey, cfg.MaxConcurrency, priority)
	if ok {
		return release, nil
	}
	return nil, &keyConcurrencyExceededError{active: active, limit: limit, preempted: preempted}
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
