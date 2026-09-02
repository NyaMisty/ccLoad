package app

import (
	"context"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"ccLoad/internal/config"
	"ccLoad/internal/model"
	"ccLoad/internal/storage"
)

const (
	debugLogCleanupBatchSize  = 200
	debugLogCleanupBatchYield = 100 * time.Millisecond
	logFlushMaxRetryBackoff   = 5 * time.Second
)

// LogService 日志管理服务
//
// 职责：处理所有日志相关的业务逻辑
// - 异步日志记录（批量写入）
// - 日志 Worker 管理
// - 日志清理（定时任务）
// - 优雅关闭
//
// 遵循 SRP 原则：仅负责日志管理，不涉及代理、认证、管理 API
type LogService struct {
	store storage.Store

	// 日志队列和 Worker
	logChan        chan *model.LogEntry
	logWorkers     int
	logInputMu     sync.RWMutex
	logInputClosed bool
	logInputClose  sync.Once
	logFailCount   atomic.Uint64 // failed entry attempts; every entry remains queued for retry

	// 日志保留天数（启动时确定，修改后重启生效）
	retentionDays int

	// 最近写入日志的重试元数据，纯内存保存，不改变 logs 表结构。
	attemptIndexCache *attemptIndexCache

	// 优雅关闭
	shutdownCh chan struct{}
	wg         *sync.WaitGroup
}

type logRuntimeMetrics struct {
	DroppedEntries           uint64 `json:"dropped_entries"`
	PersistenceFailedEntries uint64 `json:"persistence_failed_entries"`
	BacklogEntries           int    `json:"backlog_entries"`
	QueueCapacityEntries     int    `json:"queue_capacity_entries"`
}

func (s *LogService) runtimeMetrics() logRuntimeMetrics {
	if s == nil {
		return logRuntimeMetrics{}
	}
	return logRuntimeMetrics{
		// Retained for API compatibility. Queue pressure now applies backpressure,
		// so the service has no code path that increments this value.
		DroppedEntries:           0,
		PersistenceFailedEntries: s.logFailCount.Load(),
		BacklogEntries:           len(s.logChan),
		QueueCapacityEntries:     cap(s.logChan),
	}
}

// NewLogService 创建日志服务实例
func NewLogService(
	store storage.Store,
	logBufferSize int,
	logWorkers int,
	retentionDays int, // 启动时确定，修改后重启生效
	shutdownCh chan struct{},
	wg *sync.WaitGroup,
) *LogService {
	return &LogService{
		store:             store,
		logChan:           make(chan *model.LogEntry, logBufferSize),
		logWorkers:        logWorkers,
		retentionDays:     retentionDays,
		attemptIndexCache: newAttemptIndexCache(3000),
		shutdownCh:        shutdownCh,
		wg:                wg,
	}
}

// ============================================================================
// Worker 管理
// ============================================================================

// StartWorkers 启动日志 Worker
func (s *LogService) StartWorkers() {
	for i := 0; i < s.logWorkers; i++ {
		s.wg.Add(1)
		go s.logWorker()
	}
}

// logWorker 日志 Worker（后台协程）
func (s *LogService) logWorker() {
	defer s.wg.Done()

	batch := make([]*model.LogEntry, 0, config.LogBatchSize)
	ticker := time.NewTicker(config.LogBatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case entry, ok := <-s.logChan:
			if !ok {
				// CloseInput closes the producer side only after in-flight
				// submissions finish. Everything accepted is therefore drained.
				s.flushIfNeeded(batch)
				return
			}

			batch = append(batch, entry)
			if len(batch) >= config.LogBatchSize {
				s.flushLogs(batch)
				batch = batch[:0]
				ticker.Reset(config.LogBatchTimeout)
			}

		case <-ticker.C:
			// 定时直接刷当前批次；logChan 关闭会在下一轮被识别。
			s.flushIfNeeded(batch)
			batch = batch[:0]
		}
	}
}

// flushLogs 批量写入日志
func (s *LogService) flushLogs(logs []*model.LogEntry) {
	if len(logs) == 0 {
		return
	}

	timeout := time.Duration(config.LogFlushTimeoutMs) * time.Millisecond
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := s.store.BatchAddLogs(ctx, logs)
		cancel()
		if err == nil {
			s.recordAttemptIndexes(logs)
			if attempt > 1 {
				log.Printf("[WARN] 日志批量写入重试成功 (attempt=%d, batch_size=%d)", attempt, len(logs))
			}
			return
		}

		failedEntries := s.logFailCount.Add(uint64(len(logs)))
		if attempt == 1 || attempt%10 == 0 {
			log.Printf(
				"[ERROR] 日志批量写入失败，保留原批次并继续重试 (attempt=%d, batch_size=%d, failed_entry_attempts=%d): %v",
				attempt,
				len(logs),
				failedEntries,
				err,
			)
		}

		time.Sleep(logFlushRetryBackoff(attempt))
	}
}

// flushIfNeeded 辅助函数：当batch非空时执行flush
func (s *LogService) flushIfNeeded(batch []*model.LogEntry) {
	if len(batch) > 0 {
		s.flushLogs(batch)
	}
}

// ============================================================================
// 日志记录方法
// ============================================================================

// AddLogAsync 异步添加日志
func (s *LogService) AddLogAsync(entry *model.LogEntry) {
	if s == nil || entry == nil {
		return
	}

	s.logInputMu.RLock()
	if s.logInputClosed {
		s.logInputMu.RUnlock()
		// A late internal producer must not silently lose its entry. Normal
		// server shutdown quiesces request producers before CloseInput.
		s.flushLogs([]*model.LogEntry{entry})
		return
	}
	s.logChan <- entry
	s.logInputMu.RUnlock()
}

// CloseInput stops queue producers and lets workers drain every accepted log.
// Closing runs asynchronously so a producer waiting on database backpressure
// cannot prevent Server.Shutdown from honoring its context deadline.
func (s *LogService) CloseInput() {
	if s == nil {
		return
	}
	s.logInputClose.Do(func() {
		go func() {
			s.logInputMu.Lock()
			s.logInputClosed = true
			close(s.logChan)
			s.logInputMu.Unlock()
		}()
	})
}

func logFlushRetryBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return config.LogFlushRetryBackoff
	}
	maxMultiplier := int(logFlushMaxRetryBackoff / config.LogFlushRetryBackoff)
	if attempt > maxMultiplier {
		attempt = maxMultiplier
	}
	return time.Duration(attempt) * config.LogFlushRetryBackoff
}

func (s *LogService) recordAttemptIndexes(logs []*model.LogEntry) {
	if s == nil || s.attemptIndexCache == nil {
		return
	}
	for _, entry := range logs {
		if entry == nil || entry.ID <= 0 {
			continue
		}
		if entry.IsTerminalOverride {
			s.attemptIndexCache.recordFinal(entry.ID, entry.RequestID)
			continue
		}
		if entry.AttemptIndex > 0 {
			s.attemptIndexCache.record(entry.ID, entry.RequestID, entry.AttemptIndex)
		}
	}
}

// LookupAttemptIndex returns transient retry metadata for a recent persisted log.
func (s *LogService) LookupAttemptIndex(logID int64) (index int32, isFinal bool, ok bool) {
	if s == nil || s.attemptIndexCache == nil || logID <= 0 {
		return 0, false, false
	}
	return s.attemptIndexCache.lookup(logID)
}

// ============================================================================
// 日志清理
// ============================================================================

// StartCleanupLoop 分别启动普通日志和调试日志清理后台协程。
// 普通日志每小时检查一次，调试日志启动后立即检查并按独立周期运行。
// 支持优雅关闭
func (s *LogService) StartCleanupLoop() {
	s.wg.Add(2)
	go s.cleanupOldLogsLoop()
	go s.cleanupDebugLogsLoop()
}

// cleanupOldLogsLoop 日志清理后台协程（私有方法）
func (s *LogService) cleanupOldLogsLoop() {
	defer s.wg.Done()

	logTicker := time.NewTicker(config.LogCleanupInterval)
	defer logTicker.Stop()

	for {
		select {
		case <-logTicker.C:
			if s.retentionDays > 0 {
				// 使用带超时的context，避免日志清理阻塞关闭流程。
				// [FIX] P0-4: WithTimeout 的 cancel 必须在每次循环内执行，不能在循环里 defer 到 goroutine 退出。
				func() {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					cutoff := time.Now().AddDate(0, 0, -s.retentionDays)
					_ = s.store.CleanupLogsBefore(ctx, cutoff)
				}()
			}

		case <-s.shutdownCh:
			return
		}
	}
}

// cleanupDebugLogsLoop 使用独立协程清理调试日志，避免阻塞普通日志维护任务。
func (s *LogService) cleanupDebugLogsLoop() {
	defer s.wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-s.shutdownCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	debugCleanupDone := s.cleanupDebugLogs(ctx)
	for ctx.Err() == nil {
		timer := time.NewTimer(config.DebugLogCleanupInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}

		if !debugCleanupDone {
			debugCleanupDone = s.cleanupDebugLogs(ctx)
		}
	}
}

// cleanupDebugLogs 执行一轮调试日志清理。返回 true 表示 Debug 已关闭且表已清空。
func (s *LogService) cleanupDebugLogs(ctx context.Context) bool {
	setting, err := s.store.GetSetting(ctx, "debug_log_enabled")
	if err != nil {
		log.Printf("[WARN] 读取 Debug 日志开关失败: %v", err)
		return false
	}
	enabled := false
	if setting != nil {
		enabled, _ = parseSettingBool(setting.Value)
	}
	if !enabled {
		if err := s.store.TruncateDebugLogs(ctx); err != nil {
			log.Printf("[WARN] 清空调试日志失败: %v", err)
			return false
		}
		log.Printf("[INFO] 调试日志未启用，已清空历史调试日志")
		return true
	}

	retentionMinutes := config.DefaultDebugLogRetentionMinutes
	if setting, err := s.store.GetSetting(ctx, "debug_log_retention_minutes"); err != nil {
		log.Printf("[WARN] 读取 Debug 日志保留时间失败: %v", err)
		return false
	} else if setting != nil {
		if value, err := strconv.Atoi(setting.Value); err == nil && value >= 1 && value <= 1440 {
			retentionMinutes = value
		}
	}
	cutoff := time.Now().Add(-time.Duration(retentionMinutes) * time.Minute)

	for {
		deleted, err := s.store.CleanupDebugLogsBatch(ctx, cutoff, debugLogCleanupBatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			log.Printf("[WARN] 清理调试日志失败，本轮停止 (batch_size=%d): %v", debugLogCleanupBatchSize, err)
			return false
		}
		if deleted < int64(debugLogCleanupBatchSize) {
			return false
		}

		select {
		case <-time.After(debugLogCleanupBatchYield):
		case <-ctx.Done():
			return false
		}
	}
}
