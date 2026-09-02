package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ccLoad/internal/config"
	"ccLoad/internal/model"
	"ccLoad/internal/storage"
)

type debugCleanupResult struct {
	deleted int64
	err     error
}

type debugCleanupStore struct {
	storage.Store
	mu             sync.Mutex
	results        []debugCleanupResult
	limits         []int
	cutoffs        []time.Time
	callTimes      []time.Time
	enabledValue   string
	retentionValue string
	done           chan struct{}
	once           sync.Once
}

func (s *debugCleanupStore) GetSetting(_ context.Context, key string) (*model.SystemSetting, error) {
	switch key {
	case "debug_log_enabled":
		value := s.enabledValue
		if value == "" {
			value = "true"
		}
		return &model.SystemSetting{Key: key, Value: value}, nil
	case "debug_log_retention_minutes":
		value := s.retentionValue
		if value == "" {
			value = "60"
		}
		return &model.SystemSetting{Key: key, Value: value}, nil
	default:
		return nil, fmt.Errorf("unexpected setting: %s", key)
	}
}

func (s *debugCleanupStore) CleanupDebugLogsBatch(_ context.Context, cutoff time.Time, limit int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.limits = append(s.limits, limit)
	s.cutoffs = append(s.cutoffs, cutoff)
	s.callTimes = append(s.callTimes, time.Now())
	resultIndex := len(s.limits) - 1
	if len(s.limits) == len(s.results) {
		s.once.Do(func() { close(s.done) })
	}
	if resultIndex >= len(s.results) {
		return 0, errors.New("unexpected extra debug cleanup batch")
	}
	result := s.results[resultIndex]
	return result.deleted, result.err
}

func TestStartCleanupLoopAcceptsNumericBoolAndUsesRetentionDefault(t *testing.T) {
	shutdownCh := make(chan struct{})
	var wg sync.WaitGroup
	store := &debugCleanupStore{
		results:        []debugCleanupResult{{deleted: 0}},
		enabledValue:   "1",
		retentionValue: "0",
		done:           make(chan struct{}),
	}
	svc := NewLogService(store, 10, 0, 3, shutdownCh, &wg)
	t.Cleanup(func() {
		close(shutdownCh)
		wg.Wait()
	})

	svc.StartCleanupLoop()
	select {
	case <-store.done:
	case <-time.After(time.Second):
		t.Fatal("debug log cleanup did not run")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.cutoffs) != 1 {
		t.Fatalf("cutoffs=%d, want 1", len(store.cutoffs))
	}
	retention := time.Since(store.cutoffs[0])
	want := time.Duration(config.DefaultDebugLogRetentionMinutes) * time.Minute
	if retention < want-5*time.Second || retention > want+5*time.Second {
		t.Fatalf("retention=%v, want about %v", retention, want)
	}
}

type blockingDebugTruncateStore struct {
	storage.Store
	started chan struct{}
	release chan struct{}
	done    chan struct{}
	calls   atomic.Int32
}

func (s *blockingDebugTruncateStore) GetSetting(_ context.Context, key string) (*model.SystemSetting, error) {
	if key != "debug_log_enabled" {
		return nil, fmt.Errorf("unexpected setting: %s", key)
	}
	return &model.SystemSetting{Key: key, Value: "false"}, nil
}

func (s *blockingDebugTruncateStore) TruncateDebugLogs(ctx context.Context) error {
	s.calls.Add(1)
	close(s.started)
	select {
	case <-s.release:
		close(s.done)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TestAddLogAsync_NormalDelivery 验证正常投递日志到 channel
func TestAddLogAsync_NormalDelivery(t *testing.T) {
	shutdownCh := make(chan struct{})
	var wg sync.WaitGroup

	svc := NewLogService(nil, 10, 0, 3, shutdownCh, &wg)

	entry := &model.LogEntry{
		Time:       model.JSONTime{Time: time.Now()},
		Model:      "test-model",
		StatusCode: 200,
		Message:    "test",
	}

	svc.AddLogAsync(entry)

	// 应该能从 logChan 中取到
	select {
	case got := <-svc.logChan:
		if got.Model != "test-model" {
			t.Fatalf("期望 model=test-model, 实际=%s", got.Model)
		}
	case <-time.After(time.Second):
		t.Fatal("超时：日志未投递到 channel")
	}
}

func TestAddLogAsync_ChannelFullAppliesBackpressureWithoutDropping(t *testing.T) {
	shutdownCh := make(chan struct{})
	var wg sync.WaitGroup

	svc := NewLogService(nil, 1, 0, 3, shutdownCh, &wg)
	first := &model.LogEntry{Model: "first"}
	second := &model.LogEntry{Model: "second"}
	svc.AddLogAsync(first)

	enqueued := make(chan struct{})
	go func() {
		svc.AddLogAsync(second)
		close(enqueued)
	}()
	select {
	case <-enqueued:
		t.Fatal("full queue accepted a second log without backpressure")
	case <-time.After(30 * time.Millisecond):
	}

	if got := <-svc.logChan; got != first {
		t.Fatalf("first queued log=%+v", got)
	}
	select {
	case <-enqueued:
	case <-time.After(time.Second):
		t.Fatal("blocked log producer did not resume after queue capacity became available")
	}
	if got := <-svc.logChan; got != second {
		t.Fatalf("second queued log=%+v", got)
	}
	if metrics := svc.runtimeMetrics(); metrics.DroppedEntries != 0 {
		t.Fatalf("dropped entries=%d, want 0", metrics.DroppedEntries)
	}
}

func TestCloseInputDoesNotBlockShutdownBehindFullQueue(t *testing.T) {
	shutdownCh := make(chan struct{})
	var wg sync.WaitGroup
	svc := NewLogService(nil, 1, 0, 3, shutdownCh, &wg)
	first := &model.LogEntry{Model: "first"}
	second := &model.LogEntry{Model: "second"}
	svc.AddLogAsync(first)

	secondEnqueued := make(chan struct{})
	go func() {
		svc.AddLogAsync(second)
		close(secondEnqueued)
	}()
	select {
	case <-secondEnqueued:
		t.Fatal("test setup did not fill the queue")
	case <-time.After(30 * time.Millisecond):
	}

	returned := make(chan struct{})
	go func() {
		svc.CloseInput()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("CloseInput blocked behind a full queue")
	}

	if got := <-svc.logChan; got != first {
		t.Fatalf("first queued log=%+v", got)
	}
	select {
	case <-secondEnqueued:
	case <-time.After(time.Second):
		t.Fatal("second producer did not resume")
	}
	if got := <-svc.logChan; got != second {
		t.Fatalf("second queued log=%+v", got)
	}
	select {
	case _, ok := <-svc.logChan:
		if ok {
			t.Fatal("log input remained open")
		}
	case <-time.After(time.Second):
		t.Fatal("log input was not closed after producers drained")
	}
}

func TestAddLogAsyncBeforeCloseInputIsAccepted(t *testing.T) {
	shutdownCh := make(chan struct{})
	var wg sync.WaitGroup

	svc := NewLogService(nil, 10, 0, 3, shutdownCh, &wg)
	entry := &model.LogEntry{Model: "shutdown-race"}
	svc.AddLogAsync(entry)

	select {
	case got := <-svc.logChan:
		if got != entry {
			t.Fatalf("queued log=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown flag caused a pending log to be discarded")
	}
}

type recordingLogStore struct {
	storage.Store
	mu      sync.Mutex
	entries []*model.LogEntry
}

func (s *recordingLogStore) BatchAddLogs(_ context.Context, entries []*model.LogEntry) error {
	s.mu.Lock()
	s.entries = append(s.entries, entries...)
	s.mu.Unlock()
	return nil
}

func TestCloseInputDrainsAllAcceptedLogs(t *testing.T) {
	shutdownCh := make(chan struct{})
	var wg sync.WaitGroup
	store := &recordingLogStore{}
	svc := NewLogService(store, 2, 1, 3, shutdownCh, &wg)
	svc.StartWorkers()

	for i := 0; i < 5; i++ {
		svc.AddLogAsync(&model.LogEntry{Model: fmt.Sprintf("log-%d", i)})
	}
	svc.CloseInput()
	svc.CloseInput()
	wg.Wait()

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.entries) != 5 {
		t.Fatalf("persisted entries=%d, want 5", len(store.entries))
	}
	for index, entry := range store.entries {
		if entry.Model != fmt.Sprintf("log-%d", index) {
			t.Fatalf("entry %d model=%q", index, entry.Model)
		}
	}
}

// failThenSucceedStore 前 failN 次返回错误，之后返回 nil
type failThenSucceedStore struct {
	storage.Store
	attempts int
	failN    int
}

func (s *failThenSucceedStore) BatchAddLogs(_ context.Context, _ []*model.LogEntry) error {
	s.attempts++
	if s.attempts <= s.failN {
		return fmt.Errorf("simulated transient error (attempt %d)", s.attempts)
	}
	return nil
}

func TestFlushLogs_RetrySucceeds(t *testing.T) {
	shutdownCh := make(chan struct{})
	var wg sync.WaitGroup

	store := &failThenSucceedStore{failN: 3}
	svc := NewLogService(store, 10, 0, 3, shutdownCh, &wg)

	entry := &model.LogEntry{
		Time:       model.JSONTime{Time: time.Now()},
		Model:      "test-model",
		StatusCode: 200,
	}
	svc.flushLogs([]*model.LogEntry{entry})

	if store.attempts != 4 {
		t.Fatalf("期望持续重试后成功 (attempts=4)，实际=%d", store.attempts)
	}
	if failed := svc.runtimeMetrics().PersistenceFailedEntries; failed != 3 {
		t.Fatalf("failed entry attempts=%d, want 3", failed)
	}
}

func TestLogFlushRetryBackoffIsCapped(t *testing.T) {
	if got := logFlushRetryBackoff(1); got != config.LogFlushRetryBackoff {
		t.Fatalf("first retry backoff=%v", got)
	}
	if got := logFlushRetryBackoff(1_000_000); got != logFlushMaxRetryBackoff {
		t.Fatalf("capped retry backoff=%v", got)
	}
}

func TestStartCleanupLoop_StopsCurrentDebugCleanupRunAfterFailure(t *testing.T) {
	shutdownCh := make(chan struct{})
	var wg sync.WaitGroup
	store := &debugCleanupStore{
		results: []debugCleanupResult{
			{err: errors.New("batch 200 failed")},
		},
		done: make(chan struct{}),
	}
	svc := NewLogService(store, 10, 0, 3, shutdownCh, &wg)
	t.Cleanup(func() {
		close(shutdownCh)
		wg.Wait()
	})

	svc.StartCleanupLoop()

	select {
	case <-store.done:
	case <-time.After(time.Second):
		t.Fatal("debug log cleanup did not run immediately")
	}
	time.Sleep(50 * time.Millisecond)

	store.mu.Lock()
	defer store.mu.Unlock()
	want := []int{200}
	if fmt.Sprint(store.limits) != fmt.Sprint(want) {
		t.Fatalf("cleanup limits=%v, want %v", store.limits, want)
	}
}

func TestStartCleanupLoop_WaitsBetweenSuccessfulFullDebugLogBatches(t *testing.T) {
	shutdownCh := make(chan struct{})
	var wg sync.WaitGroup
	store := &debugCleanupStore{
		results: []debugCleanupResult{
			{deleted: debugLogCleanupBatchSize},
			{deleted: 0},
		},
		done: make(chan struct{}),
	}
	svc := NewLogService(store, 10, 0, 3, shutdownCh, &wg)
	t.Cleanup(func() {
		close(shutdownCh)
		wg.Wait()
	})

	svc.StartCleanupLoop()

	select {
	case <-store.done:
	case <-time.After(time.Second):
		t.Fatal("debug log cleanup did not start its second batch")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	want := []int{200, 200}
	if fmt.Sprint(store.limits) != fmt.Sprint(want) {
		t.Fatalf("cleanup limits=%v, want %v", store.limits, want)
	}
	if elapsed := store.callTimes[1].Sub(store.callTimes[0]); elapsed < 90*time.Millisecond {
		t.Fatalf("successful debug cleanup batches were not yielded: interval=%v", elapsed)
	}
}

func TestStartCleanupLoop_DoesNotBlockWhileTruncatingDisabledDebugLogs(t *testing.T) {
	shutdownCh := make(chan struct{})
	var wg sync.WaitGroup
	store := &blockingDebugTruncateStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	svc := NewLogService(store, 10, 0, 3, shutdownCh, &wg)
	t.Cleanup(func() {
		close(shutdownCh)
		wg.Wait()
	})

	returned := make(chan struct{})
	go func() {
		svc.StartCleanupLoop()
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("StartCleanupLoop blocked on debug log truncation")
	}
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("debug log truncation did not start")
	}
	close(store.release)
	select {
	case <-store.done:
	case <-time.After(time.Second):
		t.Fatal("debug log truncation did not finish")
	}
	if calls := store.calls.Load(); calls != 1 {
		t.Fatalf("truncate calls=%d, want 1", calls)
	}
}
