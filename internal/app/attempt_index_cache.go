package app

import (
	"container/list"
	"sync"
)

// attemptIndexCache keeps retry metadata for recent persisted logs without
// adding transient fields to the logs table.
type attemptIndexCache struct {
	mu           sync.RWMutex
	logs         map[int64]*list.Element
	requests     map[int64]*list.Element
	logOrder     *list.List
	requestOrder *list.List
	capacity     int
}

type attemptIndexEntry struct {
	logID         int64
	requestID     int64
	index         int32
	finalOverride bool
}

type requestAttemptEntry struct {
	requestID int64
	maxIndex  int32
	finalLog  int64
}

func newAttemptIndexCache(capacity int) *attemptIndexCache {
	if capacity <= 0 {
		capacity = 3000
	}
	return &attemptIndexCache{
		logs:         make(map[int64]*list.Element, capacity),
		requests:     make(map[int64]*list.Element, capacity),
		logOrder:     list.New(),
		requestOrder: list.New(),
		capacity:     capacity,
	}
}

func (c *attemptIndexCache) record(logID, requestID int64, index int32) {
	if logID <= 0 || index <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.updateRequestLocked(requestID, index, 0)
	entry := attemptIndexEntry{
		logID:     logID,
		requestID: requestID,
		index:     index,
	}
	if existing := c.logs[logID]; existing != nil {
		entry.finalOverride = existing.Value.(attemptIndexEntry).finalOverride
		existing.Value = entry
		c.logOrder.MoveToBack(existing)
		return
	}
	c.evictOldestLogLocked()
	c.logs[logID] = c.logOrder.PushBack(entry)
}

// recordFinal marks the log that produced the client-visible result. A
// requestID of zero is valid for standalone failures before routing begins.
func (c *attemptIndexCache) recordFinal(logID, requestID int64) {
	if logID <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.updateRequestLocked(requestID, 0, logID)
	entry := attemptIndexEntry{
		logID:         logID,
		requestID:     requestID,
		finalOverride: true,
	}
	if existing := c.logs[logID]; existing != nil {
		current := existing.Value.(attemptIndexEntry)
		entry.index = current.index
		if entry.requestID == 0 {
			entry.requestID = current.requestID
		}
		existing.Value = entry
		c.logOrder.MoveToBack(existing)
		return
	}
	c.evictOldestLogLocked()
	c.logs[logID] = c.logOrder.PushBack(entry)
}

func (c *attemptIndexCache) lookup(logID int64) (index int32, isFinal bool, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	element := c.logs[logID]
	if element == nil {
		return 0, false, false
	}
	entry := element.Value.(attemptIndexEntry)
	if request := c.requests[entry.requestID]; request != nil && entry.requestID > 0 {
		state := request.Value.(requestAttemptEntry)
		if state.finalLog > 0 {
			return entry.index, entry.logID == state.finalLog, true
		}
		return entry.index, entry.index > 0 && entry.index == state.maxIndex, true
	}
	if entry.finalOverride {
		return entry.index, true, true
	}
	return entry.index, false, true
}

func (c *attemptIndexCache) updateRequestLocked(requestID int64, index int32, finalLog int64) {
	if requestID <= 0 {
		return
	}
	if element := c.requests[requestID]; element != nil {
		entry := element.Value.(requestAttemptEntry)
		if index > entry.maxIndex {
			entry.maxIndex = index
		}
		if finalLog > 0 {
			entry.finalLog = finalLog
		}
		element.Value = entry
		c.requestOrder.MoveToBack(element)
		return
	}

	for c.requestOrder.Len() >= c.capacity {
		oldest := c.requestOrder.Front()
		if oldest == nil {
			break
		}
		entry := oldest.Value.(requestAttemptEntry)
		delete(c.requests, entry.requestID)
		c.requestOrder.Remove(oldest)
	}
	entry := requestAttemptEntry{
		requestID: requestID,
		maxIndex:  index,
		finalLog:  finalLog,
	}
	c.requests[requestID] = c.requestOrder.PushBack(entry)
}

func (c *attemptIndexCache) evictOldestLogLocked() {
	for c.logOrder.Len() >= c.capacity {
		oldest := c.logOrder.Front()
		if oldest == nil {
			return
		}
		entry := oldest.Value.(attemptIndexEntry)
		delete(c.logs, entry.logID)
		c.logOrder.Remove(oldest)
	}
}
