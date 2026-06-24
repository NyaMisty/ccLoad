// Package app 实现 ccLoad 应用的核心业务逻辑
package app

import (
	"container/list"
	"sync"
)

// attemptIndexCache 缓存最近 N 条日志的 attempt_index（纯内存态，不持久化）。
//
// 用途：在不改 logs 表 schema 的前提下，让 /admin/logs 仍能展示每条日志
// 所属请求的重试尝试次数，并标记出每个请求链的「最后一条」日志。
//
// 关联键：数据库自增 ID。日志写库后由存储层回填 entry.ID，LogService 在
// flushLogs 后记录 ID→(reqID, attempt_index)；HandleErrors 查询时按 log.ID 回查。
//
// 「最后一条」判定：优先认 final override（recordFinal 标记的「真正返回 client」日志，
// 如汇总/兜底日志）；无 override 时回退到同一请求链（activeReqID）内 attempt_index 最大者。
// 通过独立维护 reqMax（reqID→链最大 idx + finalLogID override）实现，无需遍历。
//
// 局限：容量超限后按 LRU 淘汰最旧条目，淘汰后的日志查询时拿不到信息（返回 0/false）。
type attemptIndexCache struct {
	mu       sync.RWMutex
	data     map[int64]*list.Element // log ID -> attemptIndexEntry
	reqMax   map[int64]*list.Element // reqID -> reqMaxEntry（链最大 idx）
	order    *list.List              // data 的 LRU 顺序（队首=最旧）
	reqOrder *list.List              // reqMax 的 LRU 顺序
	cap      int
}

type attemptIndexEntry struct {
	id    int64 // log ID
	reqID int64 // 所属请求链 ID（activeReqID）
	idx   int32 // 该次尝试的 attempt_index
}

type reqMaxEntry struct {
	reqID      int64
	maxIdx     int32 // 该请求链内已记录的最大 attempt_index
	finalLogID int64 // 该请求链「真正返回 client」的日志 ID（override）；0 表示回退 maxIdx 判定
}

func newAttemptIndexCache(capacity int) *attemptIndexCache {
	if capacity <= 0 {
		capacity = 3000
	}
	return &attemptIndexCache{
		data:     make(map[int64]*list.Element, capacity),
		reqMax:   make(map[int64]*list.Element, capacity),
		order:    list.New(),
		reqOrder: list.New(),
		cap:      capacity,
	}
}

// record 记录 log ID 对应的 attempt_index 与请求链 ID，并更新该链的最大 idx。
// 容量超限时 data 与 reqMax 各自按 LRU 淘汰最旧条目。
func (c *attemptIndexCache) record(id, reqID int64, idx int32) {
	if id <= 0 || idx <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// 更新请求链的最大 attempt_index（reqID<=0 时仅记 log 维度，不维护链最大值）
	if reqID > 0 {
		if el, ok := c.reqMax[reqID]; ok {
			me := el.Value.(reqMaxEntry)
			if idx > me.maxIdx {
				me.maxIdx = idx
				el.Value = me
			}
			c.reqOrder.MoveToBack(el)
		} else {
			for c.reqOrder.Len() >= c.cap {
				front := c.reqOrder.Front()
				if front == nil {
					break
				}
				v := front.Value.(reqMaxEntry)
				c.reqOrder.Remove(front)
				delete(c.reqMax, v.reqID)
			}
			c.reqMax[reqID] = c.reqOrder.PushBack(reqMaxEntry{reqID: reqID, maxIdx: idx})
		}
	}

	// 记录 log 维度
	if el, ok := c.data[id]; ok {
		el.Value = attemptIndexEntry{id: id, reqID: reqID, idx: idx}
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
	c.data[id] = c.order.PushBack(attemptIndexEntry{id: id, reqID: reqID, idx: idx})
}

// recordFinal 把 logID 标记为所属请求链「真正返回给 client 的最终结果」，覆盖 maxIdx 派生判定。
// 用于汇总/兜底日志（writeFinalProxyResponse、no available upstream）：这些日志没有
// attempt_index，却是真正返回 client 的那条；同时撤销同链上游失败日志（maxIdx 落在它们身上）
// 被错标为「终」。做两件事：reqMax[reqID].finalLogID = logID（override）；data[logID] 占位
// （idx=0）使 lookup 能查到该 logID 并读到 override。
func (c *attemptIndexCache) recordFinal(id, reqID int64) {
	if id <= 0 || reqID <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1) 标记 final override（reqMax 不存在则新建，例如 no available upstream 无上游日志）
	if el, ok := c.reqMax[reqID]; ok {
		me := el.Value.(reqMaxEntry)
		me.finalLogID = id
		el.Value = me
		c.reqOrder.MoveToBack(el)
	} else {
		for c.reqOrder.Len() >= c.cap {
			front := c.reqOrder.Front()
			if front == nil {
				break
			}
			v := front.Value.(reqMaxEntry)
			c.reqOrder.Remove(front)
			delete(c.reqMax, v.reqID)
		}
		c.reqMax[reqID] = c.reqOrder.PushBack(reqMaxEntry{reqID: reqID, finalLogID: id})
	}

	// 2) 占位进 data，使 lookup(logID) 能命中并读到 override
	if dEl, ok := c.data[id]; ok {
		dEl.Value = attemptIndexEntry{id: id, reqID: reqID, idx: 0}
		c.order.MoveToBack(dEl)
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
	c.data[id] = c.order.PushBack(attemptIndexEntry{id: id, reqID: reqID, idx: 0})
}

// lookup 查询 log ID 的 attempt_index，并判断它是否为所属请求链「真正返回 client 的最终结果」。
// 判定优先级：若该链存在 final override（recordFinal 标记），则只有 override 指向的 logID 是终；
// 否则回退 maxIdx 派生（idx 等于该链已记录的最大 idx）。线程安全（读锁）。
func (c *attemptIndexCache) lookup(id int64) (idx int32, isFinal bool, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	el, exists := c.data[id]
	if !exists {
		return 0, false, false
	}
	e := el.Value.(attemptIndexEntry)
	if e.reqID > 0 {
		if mel, mok := c.reqMax[e.reqID]; mok {
			me := mel.Value.(reqMaxEntry)
			// override 优先：显式标记的「返回 client」日志才是终，覆盖 maxIdx 派生
			if me.finalLogID != 0 {
				return e.idx, e.id == me.finalLogID, true
			}
			return e.idx, e.idx == me.maxIdx && me.maxIdx > 0, true
		}
	}
	return e.idx, false, true
}
