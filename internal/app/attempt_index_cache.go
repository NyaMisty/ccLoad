// Package app 实现 ccLoad 应用的核心业务逻辑
package app

import (
	"container/list"
	"sync"
)

// attemptIndexCache 缓存最近 N 条日志的 attempt_index（纯内存态，不持久化）。
//
// 用途：在不改 logs 表 schema 的前提下，让 /admin/logs 仍能展示每条日志
// 所属请求的重试尝试次数。
//
// 关联键：数据库自增 ID。日志写库后由存储层回填 entry.ID，LogService 在
// flushLogs 后记录 ID→attempt_index；HandleErrors 查询时按 log.ID 回查。
// ID 全局唯一，无指纹冲突风险。
//
// 局限：容量超限后按 LRU 淘汰最旧条目，淘汰后的日志查询时拿不到 index（返回 0）。
type attemptIndexCache struct {
	mu    sync.RWMutex
	data  map[int64]*list.Element // log ID -> 链表节点
	order *list.List              // LRU 顺序（队首=最旧，待淘汰）
	cap   int
}

type attemptIndexEntry struct {
	id  int64
	idx int32
}

func newAttemptIndexCache(capacity int) *attemptIndexCache {
	if capacity <= 0 {
		capacity = 3000
	}
	return &attemptIndexCache{
		data:  make(map[int64]*list.Element, capacity),
		order: list.New(),
		cap:   capacity,
	}
}

// record 记录 log ID 对应的 attempt_index，容量超限时按 LRU 淘汰最旧条目。
func (c *attemptIndexCache) record(id int64, idx int32) {
	if id <= 0 || idx <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.data[id]; ok {
		el.Value = attemptIndexEntry{id: id, idx: idx}
		c.order.MoveToBack(el)
		return
	}

	for c.order.Len() >= c.cap {
		front := c.order.Front()
		if front == nil {
			break
		}
		v := front.Value.(attemptIndexEntry)
		c.order.Remove(front)
		delete(c.data, v.id)
	}

	el := c.order.PushBack(attemptIndexEntry{id: id, idx: idx})
	c.data[id] = el
}

// lookup 查询 log ID 对应的 attempt_index（线程安全，读锁）。
func (c *attemptIndexCache) lookup(id int64) (int32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	el, ok := c.data[id]
	if !ok {
		return 0, false
	}
	return el.Value.(attemptIndexEntry).idx, true
}
