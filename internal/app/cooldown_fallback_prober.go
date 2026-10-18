package app

import (
	"sync"
	"time"
)

// cooldownFallbackProbeInterval 是“全冷却兜底”探测的最小间隔（按渠道计）。
//
// 兜底探测会真实请求仍在冷却的上游，意义只是发现“上游提前恢复/冷却状态失真”；
// 不限频时，每个入站请求都会打穿冷却（实测生产环境单小时 ~4 万次上游 429），
// 冷却形同虚设。窗口期内只允许一次探测，其余请求在选路阶段快速失败（503）。
const cooldownFallbackProbeInterval = 15 * time.Second

// cooldownFallbackProber 按渠道限频兜底探测。零值可用；纯内存状态，
// 进程重启即清零（重启后冷却探测本就需要重新建立，无需持久化）。
type cooldownFallbackProber struct {
	mu       sync.Mutex
	probable map[int64]time.Time
	interval time.Duration    // 0 时使用 cooldownFallbackProbeInterval（测试可缩短）
	now      func() time.Time // nil 时使用 time.Now（测试可注入）
}

// allowProbe 检查并登记一次兜底探测；渠道在窗口期内已探测过时返回 false。
// 登记发生在选路阶段而非真实上游请求时：探测即使因本地原因未发出，
// 也只是把下一次探测推迟一个窗口，可接受。
func (p *cooldownFallbackProber) allowProbe(channelID int64) bool {
	if p == nil || channelID <= 0 {
		return true
	}

	nowFn := p.now
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()

	interval := p.interval
	if interval <= 0 {
		interval = cooldownFallbackProbeInterval
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.probable == nil {
		p.probable = make(map[int64]time.Time)
	}
	if last, ok := p.probable[channelID]; ok && now.Sub(last) < interval {
		return false
	}
	p.probable[channelID] = now
	return true
}

// RemoveChannel 清理渠道的探测记录（渠道删除时调用，避免 map 无界增长）。
func (p *cooldownFallbackProber) RemoveChannel(channelID int64) {
	if p == nil || channelID <= 0 {
		return
	}
	p.mu.Lock()
	delete(p.probable, channelID)
	p.mu.Unlock()
}
