package app

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"ccLoad/internal/cooldown"
	"ccLoad/internal/model"
	"ccLoad/internal/protocol"
)

// ============================================================================
// 渠道 Key 槽位调度器（slot scheduler）
//
// 完全接管代理请求循环的槽位调度：
//  1. 请求在候选渠道 Key 并发满载时**跨渠道并发排队**——同时在全部候选渠道
//     各排一个等待者，谁先释放就去谁那重试（请求级墙钟预算共享）；
//  2. 高优先级令牌（priority > 默认档位）预算耗尽仍失败时，为该**全局档位**
//     登记槽位预留（每档位至多一条）：幽灵等待者在全部候选渠道并发排队，
//     先授予者落座持槽（HELD），之后到达的同档位请求直接借用（BORROWED，
//     跳过 acquire）；只有借用请求成功才消费槽位，失败自动续预留（有界）。
//
// 锁序：scheduler.mu → channelConcurrencyLimiter.mu 单向嵌套，禁止反向。
//（settleChannelSlotWaiter/abandonSlotWaiter/acquireSlotByID 只拿 limiter.mu。）
// ============================================================================

// slotSchedulerWindow 是槽位预留的默认时长：排队成功起 60s 内未被成功消费
// 则到期释放。测试经 scheduler.window 缩短。
const slotSchedulerWindow = 60 * time.Second

// slotSchedulerPoll 是跨渠道扇出的轮询节拍：授予是事件驱动的（释放即授予），
// 轮询只兜住「其他 Key 冷却到期」这类不产生释放事件的恢复。
const slotSchedulerPoll = 500 * time.Millisecond

// slotSchedulerMaxRearms 限制单个 hold 生命周期内的续预留次数，
// 防止持续失败的上游把槽位无限期锁在该档位上。
const slotSchedulerMaxRearms = 3

// slotSchedulerExpiredGrace 是 BORROWED 到期后仍未结算时的硬兜底宽限：
// 借用中的在途请求正常会在结算时释放，但任何漏掉结算的出口都不能把 +1
// 变成永久泄漏——宽限过后强制释放（借用请求侧已是 no-op release）。
const slotSchedulerExpiredGrace = 30 * time.Second

// slotSchedulerMinPriority 是触发预留的最低令牌优先级：仅严格高于默认档位
//（100）的令牌才值得长期持有槽位，默认档位流量排队耗尽照旧溢出。
const slotSchedulerMinPriority = model.AuthTokenPriorityDefault

// slotRequestBudget 是单请求的跨渠道等待预算：墙钟一次设定、跨渠道共享，
// 不做 5s×N 叠加（对齐一期 channelKeySlotWaitBudget 量级）。
const slotRequestBudget = 5 * time.Second

type slotHoldState int

const (
	slotHoldArmed slotHoldState = iota
	slotHoldHeld
	slotHoldBorrowed
)

// String 供 runtime-metrics 快照使用。
func (st slotHoldState) String() string {
	switch st {
	case slotHoldArmed:
		return "armed"
	case slotHoldHeld:
		return "held"
	case slotHoldBorrowed:
		return "borrowed"
	default:
		return "unknown"
	}
}

// slotHoldCandidate 是 arm 时从路由候选快照的渠道并发配置。
type slotHoldCandidate struct {
	channelID      int64
	maxConcurrency int
}

// slotHoldBorrowToken 是借用凭据：mint 时绑定具体 hold（或请求级主动获取的
// 槽位）与 key 身份，settle 时凭 token 结算。
//
// 不变量：token.key.channelID 必须与使用它的每次获取的渠道一致。
// keyConcurrencyID 跨渠道全等比较永不成立是防线而非巧合——丢掉这层校验
// 会把渠道 X 的 +1 预留给渠道 Y 的请求用，容量记账错渠道。
type slotHoldBorrowToken struct {
	hold    *slotHold // nil = 请求级主动获取的槽位（不参与 hold 结算）
	key     keyConcurrencyID
	release func() // 请求级主动获取时持有的真实释放函数
}

// slotHoldBorrowContextKey 携带借用凭据进入上游请求 ctx，使
// reserveUpstreamRequest 能识别「本次获取已被预留覆盖」而跳过 acquire
//（仿 claudeAffinityConcurrencyWaitContextKey 先例）。
type slotHoldBorrowContextKey struct{}

type slotHold struct {
	tier        int
	state       slotHoldState
	channelID   int64 // HELD/BORROWED 期持有槽位的渠道
	key         keyConcurrencyID
	release     func() // 落座时拿到的真实释放函数，scheduler 唯一持有
	candidates  []slotHoldCandidate
	armKeys     map[int64]string // 各渠道满载 Key 明文：无授予事件时的主动落座目标
	armDeadline time.Time        // ARMED 状态的排队窗口截止
	deadline    time.Time        // HELD/BORROWED 状态的有效截止
	rearms      int
	expired     bool // BORROWED 到期标记：结算时释放而非续期
	borrow      *slotHoldBorrowToken
}

// slotRequestState 是单请求的调度状态，挂在 proxyRequestContext 上。
type slotRequestState struct {
	deadline      time.Time // 跨渠道等待预算的墙钟截止（零值=未开始）
	saturatedKeys map[int64]string // 渠道 → 因并发满载失败的 Key 明文
}

// channelSlotScheduler 管理全局档位 hold 注册表并完全控制渠道尝试循环。
// 纯内存、进程重启即清零（重启后排队本就需要重建，无需持久化）。
type channelSlotScheduler struct {
	server  *Server
	limiter *channelConcurrencyLimiter
	baseCtx context.Context
	mu      sync.Mutex
	holds   map[int]*slotHold // 键 = tier，每档位至多一条
	window  time.Duration     // 0 用 slotSchedulerWindow（测试缩短）
	grace   time.Duration     // 0 用 slotSchedulerExpiredGrace
	now     func() time.Time  // nil 用 time.Now（测试注入）
}

func newChannelSlotScheduler(server *Server, limiter *channelConcurrencyLimiter) *channelSlotScheduler {
	return &channelSlotScheduler{
		server:  server,
		limiter: limiter,
		holds:   make(map[int]*slotHold),
	}
}

func (s *channelSlotScheduler) clock() func() time.Time {
	if s.now != nil {
		return s.now
	}
	return time.Now
}

func (s *channelSlotScheduler) holdWindow() time.Duration {
	if s.window > 0 {
		return s.window
	}
	return slotSchedulerWindow
}

func (s *channelSlotScheduler) expiredGrace() time.Duration {
	if s.grace > 0 {
		return s.grace
	}
	return slotSchedulerExpiredGrace
}

// armDeadlineFrom 返回 ARMED 状态的排队窗口截止：窗口必须覆盖「排队 + 落座」
// 整段（轮询节拍与排队等待都在其中），取 holdWindow 的 4 倍。
func (s *channelSlotScheduler) armDeadlineFrom(now time.Time) time.Time {
	return now.Add(4 * s.holdWindow())
}

func (s *channelSlotScheduler) aliveLocked(hold *slotHold) bool {
	entry, ok := s.holds[hold.tier]
	return ok && entry == hold
}

// ============================================================================
// 运行状态快照（/admin/runtime-metrics）
// ============================================================================

// slotHoldStats 是单条 hold 的可观测快照。
type slotHoldStats struct {
	Tier            int    `json:"tier"`
	State           string `json:"state"` // armed|held|borrowed
	ChannelID       int64  `json:"channel_id,omitempty"`
	SlotsHeld       int    `json:"slots_held"`           // armed=0，held/borrowed=1
	Rearms          int    `json:"rearms"`
	RearmLimit      int    `json:"rearm_limit"`
	ExpiresInMs     int64  `json:"expires_in_ms"`        // 当前状态窗口剩余（≤0 已过期）
	Candidates      int    `json:"candidates"`           // armed 期的候选渠道数
}

// slotSchedulerStats 是调度器整体快照：各档位 hold + 渠道 Key 槽位占用。
type slotSchedulerStats struct {
	Holds        []*slotHoldStats `json:"holds"`
	HeldSlots    int              `json:"held_slots"`     // 所有档位合计占用的槽位数
	ChannelKeys  []slotKeyStats   `json:"channel_keys"`   // 各渠道 Key 的 active/limit/等待者
	TotalWaiters int              `json:"total_waiters"`  // 渠道级优先队列中的等待者总数
}

// slotKeyStats 是单个 (渠道, Key) 的槽位占用快照。Key 以哈希前 8 位标识，
// 不落明文。Limit 来自最近一次 acquire 携带的渠道 MaxConcurrency（进程内观测值）。
type slotKeyStats struct {
	ChannelID     int64  `json:"channel_id"`
	Key           string `json:"key"`
	Active        int    `json:"active"`
	Limit         int    `json:"limit,omitempty"`
	Waiters       int    `json:"waiters"`
}

// stats 生成调度器与 limiter 的只读快照。持锁采集，惰性推进到期
//（快照即当前真实状态，ARMED/HELD 到期的 hold 在此清理）。
func (s *channelSlotScheduler) stats() slotSchedulerStats {
	out := slotSchedulerStats{Holds: []*slotHoldStats{}}
	if s == nil {
		return out
	}
	now := s.clock()()
	s.mu.Lock()
	for _, hold := range s.holds {
		s.expireLocked(hold)
		if !s.aliveLocked(hold) {
			continue
		}
		entry := &slotHoldStats{
			Tier:       hold.tier,
			State:      hold.state.String(),
			ChannelID:  hold.channelID,
			SlotsHeld:  0,
			Rearms:     hold.rearms,
			RearmLimit: slotSchedulerMaxRearms,
			Candidates: len(hold.candidates),
		}
		switch hold.state {
		case slotHoldHeld, slotHoldBorrowed:
			entry.SlotsHeld = 1
			entry.ExpiresInMs = hold.deadline.Sub(now).Milliseconds()
			if entry.ExpiresInMs < 0 {
				entry.ExpiresInMs = 0
			}
		case slotHoldArmed:
			entry.ExpiresInMs = hold.armDeadline.Sub(now).Milliseconds()
			if entry.ExpiresInMs < 0 {
				entry.ExpiresInMs = 0
			}
		}
		out.Holds = append(out.Holds, entry)
		out.HeldSlots += entry.SlotsHeld
	}
	s.mu.Unlock()

	out.ChannelKeys, out.TotalWaiters = s.limiter.slotKeyStatsSnapshot()
	return out
}

// slotKeyStatsSnapshot 生成各 (渠道, Key) 槽位占用的只读快照。
// Key 以哈希前 8 位十六进制标识（不落明文）；limit 是最近一次 acquire 携带的
// 观测值。按 (channelID, key) 排序保证输出稳定。
func (l *channelConcurrencyLimiter) slotKeyStatsSnapshot() ([]slotKeyStats, int) {
	if l == nil {
		return []slotKeyStats{}, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	keys := make([]keyConcurrencyID, 0, len(l.active))
	for id := range l.active {
		keys = append(keys, id)
	}
	// keyLimits 可能含已释放（active 无记录）的 key：一并纳入，活跃数自然为 0。
	for id := range l.keyLimits {
		if _, active := l.active[id]; !active {
			keys = append(keys, id)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].channelID != keys[j].channelID {
			return keys[i].channelID < keys[j].channelID
		}
		return bytes.Compare(keys[i].keyHash[:], keys[j].keyHash[:]) < 0
	})

	stats := make([]slotKeyStats, 0, len(keys))
	totalWaiters := 0
	waitersByChannel := make(map[int64]int, len(l.channelWaiters))
	for channelID, waiters := range l.channelWaiters {
		count := 0
		for _, w := range waiters {
			if !w.abandoned && !w.acquired {
				count++
			}
		}
		waitersByChannel[channelID] = count
		totalWaiters += count
	}
	for _, id := range keys {
		stats = append(stats, slotKeyStats{
			ChannelID: id.channelID,
			Key:       hex.EncodeToString(id.keyHash[:4]),
			Active:    l.active[id],
			Limit:     l.keyLimits[id],
			Waiters:   waitersByChannel[id.channelID],
		})
	}
	return stats, totalWaiters
}

// ============================================================================
// limiter 原语扩展（同包方法；不改上游 enqueueChannelSlotWait 签名）
// ============================================================================

// enqueueSlotWait 入队等待指定渠道的下一个释放槽位。ghost=true 表示槽位
// 预留的幽灵等待者（入队即标记，授予随时可能发生，事后设置会错过授予时刻
// 的 tie-break）。limit > 0 时入队即做「授予早于入队丢失」补救：该渠道存在
// 计数低于 limit 的 key（容量空闲）时立即按授予协议授予。返回 nil 表示
// ctx 结束（轮询节拍/取消），等待者已自行清理。
func (l *channelConcurrencyLimiter) enqueueSlotWait(ctx context.Context, channelID int64, priority int, ghost bool, limit int) *channelSlotWaiter {
	if l == nil || channelID <= 0 {
		return nil
	}

	l.mu.Lock()
	waiter := &channelSlotWaiter{
		priority: priority,
		seq:      l.channelSeq.Add(1),
		ready:    make(chan struct{}),
		ghost:    ghost,
	}
	l.channelWaiters[channelID] = append(l.channelWaiters[channelID], waiter)
	if limit > 0 {
		if idle, ok := l.idleChannelKeyLocked(channelID, limit); ok {
			// 容量空闲：把它当作一次「释放」立即授予（不违反反偷窃——授予给
			// 队列中最高优先级者；若已有更高者在队，授予归它，本等待者继续等）。
			l.grantBestChannelWaiterLocked(channelID, idle)
		}
	}
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

// idleChannelKeyLocked 报告该渠道是否存在计数低于 limit 的 key（即有空闲
// 容量的 key），并返回其 id。必须持 l.mu 调用。active 中存在计数的 key
// 必然配置了并发上限（无上限 key 不进 active），count 即当前并发。
func (l *channelConcurrencyLimiter) idleChannelKeyLocked(channelID int64, limit int) (keyConcurrencyID, bool) {
	if limit <= 0 {
		return keyConcurrencyID{}, false
	}
	for id, count := range l.active {
		if id.channelID == channelID && count < limit {
			return id, true
		}
	}
	return keyConcurrencyID{}, false
}

// acquireSlotByID 按 keyConcurrencyID 直接获取槽位（带容量与反偷窃检查），
// 供幽灵等待者与请求级主动获取使用。limit ≤ 0 视为无上限（防的是配置缩小
// 后的极端窗口）；正常路径必须传渠道 MaxConcurrency——limit=0 会在 Key
// 满载时超卖。
func (l *channelConcurrencyLimiter) acquireSlotByID(id keyConcurrencyID, limit int, priority int) (release func(), ok bool) {
	if l == nil {
		return func() {}, true
	}

	l.mu.Lock()
	if limit > 0 {
		l.keyLimits[id] = limit // 观测值：runtime-metrics 快照展示 active/limit
	}
	current := l.active[id]
	if limit > 0 && current >= limit {
		l.mu.Unlock()
		return nil, false
	}
	if l.hasHigherChannelWaiterLocked(id.channelID, priority) {
		l.mu.Unlock()
		return nil, false
	}
	l.active[id] = current + 1
	l.mu.Unlock()

	return l.releaseFunc(id), true
}

// ============================================================================
// 渠道尝试循环（完全接管）
// ============================================================================

// runAttemptLoop 按候选顺序尝试渠道，满载时跨渠道并发排队等待槽位。
// 前身 runProxyAttemptLoopWithFailureBoundary 的循环体迁入于此；
// 错误分支语义保持一致。
func (s *channelSlotScheduler) runAttemptLoop(
	ctx context.Context,
	cands []*model.Config,
	reqCtx *proxyRequestContext,
	w http.ResponseWriter,
	stopAfterFailure func(current, next *model.Config, result *proxyResult) bool,
) (lastResult *proxyResult, succeeded bool) {
	if s == nil {
		return nil, false
	}
	if reqCtx != nil && reqCtx.tokenPriority > slotSchedulerMinPriority {
		cands = s.prioritizeHeldChannel(cands, reqCtx.tokenPriority)
	}
	sawAlphaSearchUnsupported := false
	for index, cfg := range cands {
		result, err := s.server.tryChannelWithKeys(ctx, cfg, reqCtx, w)

		// 跨渠道槽位调度：仅因并发满载失败时进入并发排队等待
		//（fanoutAndWait 内部处理预算耗尽与溢出）。
		if err != nil && errors.Is(err, ErrKeyConcurrencyExceeded) && s.shouldFanout(cfg, reqCtx) {
			result, err = s.fanoutAndWait(ctx, cands, index, reqCtx, w)
		}

		if err != nil && errors.Is(err, errClaudeAffinityTargetAlreadyTried) {
			continue
		}

		// 所有Key冷却：触发渠道级冷却(503)，防止后续请求重复尝试
		if err != nil && errors.Is(err, ErrAllKeysUnavailable) {
			s.server.applyCooldownDecision(ctx, cfg, httpErrorInputFromParts(cfg.ID, cooldown.NoKeyIndex, 503, nil, nil))
			continue
		}

		if err != nil && errors.Is(err, ErrAllKeysExhausted) {
			log.Printf("[WARN] 渠道 %s (ID=%d) 所有Key验证失败，跳过该渠道", cfg.Name, cfg.ID)
			continue
		}

		if err != nil && errors.Is(err, ErrChannelRPMExceeded) {
			log.Printf("[INFO] 渠道 %s (ID=%d) 已达到RPM限制，跳过该渠道", cfg.Name, cfg.ID)
			continue
		}

		if err != nil && errors.Is(err, ErrKeyConcurrencyExceeded) {
			log.Printf(
				"[INFO] 渠道 %s (ID=%d) 已达到单 Key 并发限制，跳过该渠道：%s",
				cfg.Name, cfg.ID, keyConcurrencySkipReason(err),
			)
			continue
		}

		if result != nil {
			if result.protocolCapabilityMissing && protocol.DetectRequestFamily(reqCtx.requestPath) == protocol.RequestFamilyAlphaSearch {
				sawAlphaSearchUnsupported = true
			}
			if result.succeeded {
				return result, true
			}

			lastResult = result

			// 客户端已取消：别再浪费资源"重试"了。
			if result.isClientCanceled {
				break
			}

			if shouldStopTryingChannels(result) {
				break
			}

			if stopAfterFailure != nil && index+1 < len(cands) &&
				stopAfterFailure(cfg, cands[index+1], result) {
				break
			}
		}
	}
	if sawAlphaSearchUnsupported &&
		protocol.DetectRequestFamily(reqCtx.requestPath) == protocol.RequestFamilyAlphaSearch &&
		(lastResult == nil || (!lastResult.isClientCanceled && lastResult.nextAction != cooldown.ActionReturnClient)) {
		writeEmptyAlphaSearchResponse(w)
		return &proxyResult{status: http.StatusOK, succeeded: true, nextAction: cooldown.ActionReturnClient}, true
	}

	return lastResult, false
}

// requestBudget 返回请求级等待预算：Server.preferredChannelKeySlotWait
// 为测试缩短保留（0 用 slotRequestBudget 默认值）。
func (s *channelSlotScheduler) requestBudget() time.Duration {
	if s.server != nil && s.server.preferredChannelKeySlotWait > 0 {
		return s.server.preferredChannelKeySlotWait
	}
	return slotRequestBudget
}

// shouldFanout 判断是否进入跨渠道并发排队：全冷却兜底候选与亲和探测各有
// 专属语义，不排队；预算未初始化或仍有剩余才值得等。
func (s *channelSlotScheduler) shouldFanout(cfg *model.Config, reqCtx *proxyRequestContext) bool {
	if s == nil || cfg == nil || reqCtx == nil {
		return false
	}
	if cfg.CooldownFallback || reqCtx.claudeAffinityProbe {
		return false
	}
	if reqCtx.slotState == nil || reqCtx.slotState.deadline.IsZero() {
		return true
	}
	return s.clock()().Before(reqCtx.slotState.deadline)
}

// requestState 返回（必要时初始化）请求的调度状态。
func (s *channelSlotScheduler) requestState(reqCtx *proxyRequestContext) *slotRequestState {
	if reqCtx == nil {
		return nil
	}
	if reqCtx.slotState == nil {
		reqCtx.slotState = &slotRequestState{}
	}
	return reqCtx.slotState
}

// fanoutCandidates 返回满载渠道起（含）的可等待候选，排除全冷却兜底候选
//（探测语义不允许排队）与未配置并发上限的渠道（不存在满载）。
func fanoutCandidates(cands []*model.Config, index int) []*model.Config {
	var waitable []*model.Config
	for i := index; i < len(cands); i++ {
		cfg := cands[i]
		if cfg == nil || cfg.CooldownFallback || cfg.MaxConcurrency <= 0 {
			continue
		}
		waitable = append(waitable, cfg)
	}
	return waitable
}

// fanoutAndWait 跨渠道并发排队等待槽位：对当前及后续候选各排一个等待者，
// 任一渠道授予后先解除其余等待者、再去授予渠道重试。预算是请求级墙钟：
// 一次设定，重扇出不重置。预算耗尽（或 ctx 取消）时为高优档位 arm 幽灵预留，
// 然后返回最后的并发错误交回循环按「跳过该渠道」继续。
func (s *channelSlotScheduler) fanoutAndWait(
	ctx context.Context,
	cands []*model.Config,
	index int,
	reqCtx *proxyRequestContext,
	w http.ResponseWriter,
) (*proxyResult, error) {
	state := s.requestState(reqCtx)
	if state == nil {
		return nil, ErrKeyConcurrencyExceeded
	}
	if state.deadline.IsZero() {
		state.deadline = s.clock()().Add(s.requestBudget())
	}
	// 重试链路里 acquire 的优先级来自 ctx value（doUpstreamRequest 读取）。
	// 生产路径在请求入口已包装；这里幂等补包一次，覆盖直接调用本循环的路径
	//（测试/未来调用点），保证重试按本请求令牌的优先级参与反偷窃与排队。
	ctx = withChannelSlotPriority(ctx, reqCtx.tokenPriority)

	lastErr := error(ErrKeyConcurrencyExceeded)
	for {
		if ctx.Err() != nil || !s.clock()().Before(state.deadline) {
			s.armHold(cands, reqCtx)
			return nil, lastErr
		}
		waitable := fanoutCandidates(cands, index)
		if len(waitable) == 0 {
			s.armHold(cands, reqCtx)
			return nil, lastErr
		}

		beat := slotSchedulerPoll
		if remaining := time.Until(state.deadline); remaining < beat {
			beat = remaining
		}
		round := newSlotFanoutRound(ctx, s.limiter, waitable, reqCtx.tokenPriority, false, beat)
		ev := round.wait()
		if ev == nil {
			// 轮询节拍到期或 ctx 取消。先试主动获取（授予事件可能早于入队
			// 而丢失——满载 Key 在入队前已释放），再进入下一轮或收尾。
			if ctxErr := ctx.Err(); ctxErr != nil {
				s.armHold(cands, reqCtx)
				return nil, lastErr
			}
			if token, ok := s.tryIdleBorrow(waitable, reqCtx); ok {
				result, err := s.server.tryChannelWithKeys(
					context.WithValue(ctx, slotHoldBorrowContextKey{}, token),
					token.channelCfg, reqCtx, w,
				)
				token.finish(err == nil || (result != nil && result.succeeded))
				if !errors.Is(err, ErrKeyConcurrencyExceeded) {
					return result, err
				}
				lastErr = err
			}
			continue
		}

		// 先解除本轮全部等待者（输家越早解除，其他渠道的低优先级越早不被
		// 误拦），再去赢家渠道重试。
		round.close(ev)
		result, err := s.server.tryChannelWithKeys(ctx, ev.cfg, reqCtx, w)
		round.settleWinner(ev, err == nil)
		if !errors.Is(err, ErrKeyConcurrencyExceeded) {
			return result, err
		}
		lastErr = err
		// 赢家渠道被同档位竞速抢走：回扇出入口重扇出，墙钟预算继续。
	}
}

// ============================================================================
// 借用侧（key 循环 / reserve 挂钩）
// ============================================================================

// noteKeySaturation 记录渠道因单 Key 并发满载失败的 Key 明文，
// 供请求级主动获取（授予丢失兜底）与幽灵 armKey 兜底使用。
func (s *channelSlotScheduler) noteKeySaturation(reqCtx *proxyRequestContext, channelID int64, apiKey string) {
	if s == nil || reqCtx == nil || apiKey == "" {
		return
	}
	state := s.requestState(reqCtx)
	if state.saturatedKeys == nil {
		state.saturatedKeys = make(map[int64]string)
	}
	state.saturatedKeys[channelID] = apiKey
}

// pinBorrowedKey 返回该渠道可借用的预留 Key 明文：档位 HELD hold 落座在
// 本渠道、Key 健康（未禁用/未冷却）且未被本次请求试过。
// tier 化后 holdable 已显式校验 hold.channelID == cfg.ID——
// keyConcurrencyID 跨渠道全等不成立是防线而非巧合。
func (s *channelSlotScheduler) pinBorrowedKey(cfg *model.Config, apiKeys []*model.APIKey, triedKeys map[int]bool, tier int, now time.Time) (string, bool) {
	if s == nil || cfg == nil || tier <= slotSchedulerMinPriority {
		return "", false
	}
	keyID, ok := s.holdable(cfg.ID, tier)
	if !ok {
		return "", false
	}
	for _, apiKey := range apiKeys {
		if apiKey == nil || apiKey.Disabled || apiKey.IsCoolingDown(now) || triedKeys[apiKey.KeyIndex] {
			continue
		}
		if newKeyConcurrencyID(cfg.ID, apiKey.APIKey) == keyID {
			return apiKey.APIKey, true
		}
	}
	return "", false
}

// borrowFor 尝试为渠道借用预留：成功返回凭据（挂进 ctx 后 reserve 侧短路）。
// 幂等：同凭据重复借用返回同一凭据。
func (s *channelSlotScheduler) borrowFor(cfg *model.Config, tier int, existing *slotHoldBorrowToken) *slotHoldBorrowToken {
	if s == nil || cfg == nil {
		return nil
	}
	return s.borrow(cfg.ID, tier, existing)
}

// settleBorrowed 结算一次借用：成功消费 / 失败续预留（Key 不健康不续）。
func (s *channelSlotScheduler) settleBorrowed(cfg *model.Config, apiKeys []*model.APIKey, token *slotHoldBorrowToken, succeeded bool) {
	if s == nil || token == nil || token.hold == nil {
		return
	}
	keyHealthy := false
	now := time.Now()
	for _, apiKey := range apiKeys {
		if apiKey != nil && !apiKey.Disabled && !apiKey.IsCoolingDown(now) &&
			newKeyConcurrencyID(cfg.ID, apiKey.APIKey) == token.key {
			keyHealthy = true
			break
		}
	}
	s.settleBorrow(token, succeeded, keyHealthy)
}

// borrowedRelease 是 reserveUpstreamRequest 的借用短路入口：ctx 携带的凭据
// 仍被调度器覆盖（BORROWED 未超硬宽限、同渠道）时返回 no-op release，true；
// 否则 false，调用方走正常 acquire。必须经 covers 查注册表状态——只比
// token 相等而不查状态，会把任意 bug 直接放大成容量超卖。
func (s *channelSlotScheduler) borrowedRelease(ctx context.Context, cfg *model.Config) func() {
	if s == nil || cfg == nil {
		return nil
	}
	token, _ := ctx.Value(slotHoldBorrowContextKey{}).(*slotHoldBorrowToken)
	if token == nil || !s.covers(token, cfg.ID) {
		return nil
	}
	return func() {}
}

// idleBorrow 是请求级主动获取的槽位借用（授予丢失兜底）：真实 acquire 一次，
// 持有 release 直到本次渠道重试结束。
type idleBorrow struct {
	token      *slotHoldBorrowToken
	channelCfg *model.Config
}

// tryIdleBorrow 对等待集合逐渠道尝试主动获取满载 Key 的槽位。
// 带渠道 MaxConcurrency 检查，不会超卖。成功返回携带真实 release 的借用。
func (s *channelSlotScheduler) tryIdleBorrow(waitable []*model.Config, reqCtx *proxyRequestContext) (*idleBorrow, bool) {
	state := s.requestState(reqCtx)
	if state == nil {
		return nil, false
	}
	for _, cfg := range waitable {
		key, ok := state.saturatedKeys[cfg.ID]
		if !ok || key == "" {
			continue
		}
		release, acquired := s.limiter.acquireSlotByID(newKeyConcurrencyID(cfg.ID, key), cfg.MaxConcurrency, reqCtx.tokenPriority)
		if !acquired {
			continue
		}
		return &idleBorrow{
			token:      &slotHoldBorrowToken{key: newKeyConcurrencyID(cfg.ID, key), release: release},
			channelCfg: cfg,
		}, true
	}
	return nil, false
}

// finish 结束一次请求级借用：释放主动获取的槽位。
func (b *idleBorrow) finish(succeeded bool) {
	if b != nil && b.token != nil && b.token.release != nil {
		b.token.release()
		b.token.release = nil
	}
}

// ============================================================================
// 扇出轮次（请求级与幽灵共用协议）
// ============================================================================

// slotGrantEvent 是一次授予事件：授予发生在具体渠道，等待者已 granted。
type slotGrantEvent struct {
	cfg    *model.Config
	waiter *channelSlotWaiter
}

// slotFanoutRound 是一轮跨渠道扇出排队：每个候选渠道一个 wrapper goroutine，
// 第一个授予者获胜；拆除时统一 settle 全部等待者。
//
// 拆除协议（四个硬约束，违反任一 → goroutine 泄漏 / 死锁 / 双 settle）：
//  1. grantedCh 有缓冲（len=候选数）：授予后 wrapper 的发送永不阻塞；
//  2. close 在收到赢家后立即关闭 done + cancel，wrapper 侧 select done 自行 settle；
//  3. 已授予的等待者只能 settle 不能 abandon；ctx 超时返回 nil 的已自行清理；
//  4. 不持 scheduler.mu 调 wg 类等待（本实现无 wg——wrapper 由 done channel
//     收敛，round 生命周期由调用方闭合）。
type slotFanoutRound struct {
	limiter   *channelConcurrencyLimiter
	grantedCh chan *slotGrantEvent
	done      chan struct{}
	cancel    context.CancelFunc
	pollDone  <-chan struct{}
	closed    atomic.Bool
}

func newSlotFanoutRound(
	ctx context.Context,
	limiter *channelConcurrencyLimiter,
	cfgs []*model.Config,
	priority int,
	ghost bool,
	beat time.Duration,
) *slotFanoutRound {
	round := &slotFanoutRound{
		limiter:   limiter,
		grantedCh: make(chan *slotGrantEvent, len(cfgs)),
		done:      make(chan struct{}),
	}
	pollCtx, cancel := context.WithTimeout(ctx, beat)
	round.cancel = cancel
	round.pollDone = pollCtx.Done()
	for _, cfg := range cfgs {
		go func(cfg *model.Config) {
			// limit 传入做「授予早于入队丢失」补救：入队时容量已空闲则立即授予。
			w := limiter.enqueueSlotWait(pollCtx, cfg.ID, priority, ghost, cfg.MaxConcurrency)
			if w == nil {
				return // 轮询节拍/取消：等待者已自行清理
			}
			if round.closed.Load() {
				// 轮次已关闭：自行 settle，清理移出主循环。
				limiter.settleChannelSlotWaiter(w, false)
				return
			}
			select {
			case round.grantedCh <- &slotGrantEvent{cfg: cfg, waiter: w}:
				// 事件已入队；等待者的 settle 由轮次所有者负责
				//（close 或 settleWinner），wrapper 到此为止。
			case <-round.done:
				limiter.settleChannelSlotWaiter(w, false)
			}
		}(cfg)
	}
	return round
}

// wait 阻塞直到第一个授予（返回赢家）或轮次节拍/ctx 结束（返回 nil）。
func (r *slotFanoutRound) wait() *slotGrantEvent {
	select {
	case ev := <-r.grantedCh:
		return ev
	case <-r.pollDone:
		return nil
	}
}

// close 关闭轮次并解除除赢家外的全部授予等待者。已送达 grantedCh 的事件
// 逐个 settle(false)；仍在队的等待者由 pollCtx 取消自行清理。
func (r *slotFanoutRound) close(winner *slotGrantEvent) {
	r.closed.Store(true)
	r.cancel()
	close(r.done)
	for {
		select {
		case ev := <-r.grantedCh:
			if ev == winner {
				continue
			}
			r.limiter.settleChannelSlotWaiter(ev.waiter, false)
		default:
			return
		}
	}
}

// settleWinner 结算赢家等待者（真实拿到槽位 acquired=true，被抢走 false）。
func (r *slotFanoutRound) settleWinner(winner *slotGrantEvent, acquired bool) {
	if winner != nil {
		r.limiter.settleChannelSlotWaiter(winner.waiter, acquired)
	}
}

// ============================================================================
// 全局档位 hold
// ============================================================================

// armHold 为高优档位登记全局预留（幂等：同档位已有 hold 则 no-op，
// 先到先得——第一个触发请求的候选快照绑定整条 hold）。
func (s *channelSlotScheduler) armHold(cands []*model.Config, reqCtx *proxyRequestContext) {
	if s == nil || reqCtx == nil || reqCtx.tokenPriority <= slotSchedulerMinPriority {
		return
	}
	if reqCtx.claudeAffinityProbe {
		return
	}
	candidates := make([]slotHoldCandidate, 0, len(cands))
	for _, cfg := range cands {
		if cfg == nil || cfg.CooldownFallback {
			continue
		}
		candidates = append(candidates, slotHoldCandidate{channelID: cfg.ID, maxConcurrency: cfg.MaxConcurrency})
	}
	if len(candidates) == 0 {
		return
	}

	s.mu.Lock()
	if _, exists := s.holds[reqCtx.tokenPriority]; exists {
		s.mu.Unlock()
		return
	}
	state := s.requestState(reqCtx)
	armKeys := make(map[int64]string, len(state.saturatedKeys))
	for id, key := range state.saturatedKeys {
		armKeys[id] = key
	}
	hold := &slotHold{
		tier:        reqCtx.tokenPriority,
		candidates:  candidates,
		armKeys:     armKeys,
		armDeadline: s.armDeadlineFrom(s.clock()()),
	}
	s.holds[reqCtx.tokenPriority] = hold
	s.mu.Unlock()

	log.Printf(
		"[INFO] 优先级 %d 档位 Key 槽位预留：排队耗尽仍失败，幽灵跨 %d 个候选渠道排队预留（≤%.0fs）",
		hold.tier, len(candidates), s.holdWindow().Seconds(),
	)

	go s.runGhostWaiter(s.baseCtx, hold)
}

// runGhostWaiter 是幽灵等待者主循环：每轮对全部候选渠道扇出排队，
// 先授予者落座；轮询节拍上对满载过的渠道试主动落座（授予丢失兜底）。
func (s *channelSlotScheduler) runGhostWaiter(ctx context.Context, hold *slotHold) {
	ctx, cancel := context.WithDeadline(ctx, s.armDeadlineFrom(s.clock()()))
	defer cancel()

	for ctx.Err() == nil {
		s.mu.Lock()
		alive := s.aliveLocked(hold) && hold.state == slotHoldArmed
		s.mu.Unlock()
		if !alive {
			return
		}

		round := s.newGhostRound(ctx, hold)
		ev := round.wait()
		if ev != nil {
			round.close(ev)
			if s.trySeatGhost(hold, ev, round) {
				return
			}
			continue // 落座失败（被更高优先级抢占/limit 满）：下一轮重扇出
		}
		round.close(nil)
		if ctx.Err() != nil {
			return
		}
		// 轮询节拍：先试各候选渠道的满载 Key 主动落座（带渠道 limit，不超卖）。
		s.mu.Lock()
		alive = s.aliveLocked(hold) && hold.state == slotHoldArmed
		s.mu.Unlock()
		if !alive {
			return
		}
		if s.trySeatGhostIdle(hold) {
			return
		}
	}
}

// newGhostRound 为 hold 的候选渠道构造幽灵扇出轮次。
// candidates/armKeys 快照在锁内取——RemoveChannel 会并发替换这两个字段。
func (s *channelSlotScheduler) newGhostRound(ctx context.Context, hold *slotHold) *ghostFanoutRound {
	s.mu.Lock()
	candidates := make([]slotHoldCandidate, len(hold.candidates))
	copy(candidates, hold.candidates)
	s.mu.Unlock()

	cfgs := make([]*model.Config, 0, len(candidates))
	limits := make(map[int64]int, len(candidates))
	for _, cand := range candidates {
		cfgs = append(cfgs, &model.Config{ID: cand.channelID})
		limits[cand.channelID] = cand.maxConcurrency
	}
	return &ghostFanoutRound{
		round:  newSlotFanoutRound(ctx, s.limiter, cfgs, hold.tier, true, slotSchedulerPoll),
		limits: limits,
	}
}

// ghostFanoutRound 组合通用扇出轮次与幽灵落座需要的渠道 limit 表。
type ghostFanoutRound struct {
	round  *slotFanoutRound
	limits map[int64]int
}

func (g *ghostFanoutRound) wait() *slotGrantEvent    { return g.round.wait() }
func (g *ghostFanoutRound) close(ev *slotGrantEvent) { g.round.close(ev) }
func (g *ghostFanoutRound) settleWinner(ev *slotGrantEvent, acquired bool) {
	g.round.settleWinner(ev, acquired)
}

// trySeatGhost 让 hold 以授予事件指定的 key 落座（置 HELD）。
// 返回 true 表示落座成功或 hold 已终结（调用方退出）。
func (s *channelSlotScheduler) trySeatGhost(hold *slotHold, ev *slotGrantEvent, round *ghostFanoutRound) bool {
	limit := round.limits[ev.cfg.ID]
	release, ok := s.limiter.acquireSlotByID(ev.waiter.grantedKey, limit, hold.tier)
	s.mu.Lock()
	if !ok || !s.aliveLocked(hold) || hold.state != slotHoldArmed {
		s.mu.Unlock()
		round.settleWinner(ev, false)
		if ok {
			release() // 落座成功但 hold 已死：立即归还，不留孤儿 +1
		}
		return true
	}
	hold.state = slotHoldHeld
	hold.channelID = ev.cfg.ID
	hold.key = ev.waiter.grantedKey
	hold.release = release
	hold.deadline = s.clock()().Add(s.holdWindow())
	s.mu.Unlock()
	round.settleWinner(ev, true)

	log.Printf(
		"[INFO] 优先级 %d 档位槽位预留：已在渠道 %d 持有槽位（有效期 %.0fs）",
		hold.tier, hold.channelID, s.holdWindow().Seconds(),
	)
	return true
}

// trySeatGhostIdle 幽灵主动落座：对各候选渠道试其满载 Key（授予丢失兜底）。
// 带 arm 时快照的渠道 MaxConcurrency，Key 满载时不会超卖。
// candidates/armKeys 快照在锁内取——RemoveChannel 会并发替换这两个字段。
func (s *channelSlotScheduler) trySeatGhostIdle(hold *slotHold) bool {
	s.mu.Lock()
	candidates := make([]slotHoldCandidate, len(hold.candidates))
	copy(candidates, hold.candidates)
	armKeys := make(map[int64]string, len(hold.armKeys))
	for id, key := range hold.armKeys {
		armKeys[id] = key
	}
	s.mu.Unlock()

	for _, cand := range candidates {
		key, ok := armKeys[cand.channelID]
		if !ok || key == "" {
			continue
		}
		release, acquired := s.limiter.acquireSlotByID(newKeyConcurrencyID(cand.channelID, key), cand.maxConcurrency, hold.tier)
		if !acquired {
			continue
		}
		s.mu.Lock()
		if !s.aliveLocked(hold) || hold.state != slotHoldArmed {
			s.mu.Unlock()
			release()
			return true
		}
		hold.state = slotHoldHeld
		hold.channelID = cand.channelID
		hold.key = newKeyConcurrencyID(cand.channelID, key)
		hold.release = release
		hold.deadline = s.clock()().Add(s.holdWindow())
		s.mu.Unlock()
		log.Printf(
			"[INFO] 优先级 %d 档位槽位预留：已在渠道 %d 持有槽位（有效期 %.0fs）",
			hold.tier, cand.channelID, s.holdWindow().Seconds(),
		)
		return true
	}
	return false
}

// holdable 查询渠道可借用的预留 Key 身份（HELD hold 落座在本渠道）。
// tier 化后必须显式校验 hold.channelID == channelID——keyConcurrencyID 跨渠道
// 全等不成立是防线而非巧合。惰性执行到期检查。
func (s *channelSlotScheduler) holdable(channelID int64, tier int) (keyConcurrencyID, bool) {
	if s == nil {
		return keyConcurrencyID{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	hold, ok := s.holds[tier]
	if !ok {
		return keyConcurrencyID{}, false
	}
	s.expireLocked(hold)
	if !s.aliveLocked(hold) || hold.state != slotHoldHeld || hold.channelID != channelID {
		return keyConcurrencyID{}, false
	}
	return hold.key, true
}

// borrow 把 HELD hold 借给调用方：状态 CAS 到 BORROWED 并 mint 凭据。
// 同一借用方（同凭据）重复借用幂等返回同一凭据。
func (s *channelSlotScheduler) borrow(channelID int64, tier int, borrower *slotHoldBorrowToken) *slotHoldBorrowToken {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	hold, ok := s.holds[tier]
	if !ok {
		return nil
	}
	s.expireLocked(hold)
	if !s.aliveLocked(hold) || hold.channelID != channelID {
		return nil
	}
	switch {
	case hold.state == slotHoldBorrowed:
		if borrower != nil && hold.borrow == borrower {
			return hold.borrow // 同一借用方：幂等
		}
		return nil // 已借给他人
	case hold.state != slotHoldHeld:
		return nil
	}
	hold.state = slotHoldBorrowed
	hold.borrow = &slotHoldBorrowToken{hold: hold, key: hold.key}
	log.Printf("[INFO] 优先级 %d 档位槽位预留：借用给请求（渠道 %d）", tier, channelID)
	return hold.borrow
}

// covers 报告 token 是否仍是当前有效的借用凭据（BORROWED 未超硬宽限、同渠道）。
// reserveUpstreamRequest 的跳过 acquire 判定必须经过这里——只比 token 相等
// 而不查状态，会把任意 bug 直接放大成容量超卖。
func (s *channelSlotScheduler) covers(token *slotHoldBorrowToken, channelID int64) bool {
	if s == nil || token == nil {
		return false
	}
	// 渠道绑定不变量：凭据的 key 渠道必须与本次获取的渠道一致。
	// keyConcurrencyID 跨渠道全等不成立是防线而非巧合。
	if token.key.channelID != channelID {
		return false
	}
	if token.hold == nil {
		// 请求级主动获取的借用：release 由 idleBorrow.finish 管理，
		// 覆盖期即本次渠道重试全程（调用方保证同步）。
		return token.release != nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	hold := token.hold
	s.expireLocked(hold) // 惰性推进到期标记：超窗的 BORROWED 立即进入 expired 语义
	if !s.aliveLocked(hold) || hold.state != slotHoldBorrowed || hold.borrow != token {
		return false
	}
	if hold.expired && s.clock()().After(hold.deadline.Add(s.expiredGrace())) {
		return false // 硬宽限已过：强制释放路径即将回收，不再覆盖新的获取
	}
	return true
}

// settleBorrow 结算一次借用：成功则消费槽位（-1 删记录），失败则续预留。
// 只有 token 匹配的借用才能结算；过期 hold 结算时释放而非续期。
// keyHealthy 为 false（key 已删除/禁用/冷却）或续期次数用尽时同样释放。
func (s *channelSlotScheduler) settleBorrow(token *slotHoldBorrowToken, succeeded bool, keyHealthy bool) {
	if s == nil || token == nil || token.hold == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	hold := token.hold
	if !s.aliveLocked(hold) || hold.state != slotHoldBorrowed || hold.borrow != token {
		return
	}
	s.expireLocked(hold) // 惰性推进到期：expired 标记必须在续期判定前生效
	if !s.aliveLocked(hold) || hold.state != slotHoldBorrowed || hold.borrow != token {
		return // 到期硬释放已在 expireLocked 内完成
	}
	hold.borrow = nil
	if succeeded {
		s.releaseLocked(hold, "借用请求成功，槽位已消费")
		return
	}
	if hold.expired || !keyHealthy || hold.rearms >= slotSchedulerMaxRearms {
		why := "预留到期"
		switch {
		case !keyHealthy:
			why = "Key 不可用（删除/禁用/冷却）"
		case hold.rearms >= slotSchedulerMaxRearms:
			why = "续预留次数用尽"
		}
		s.releaseLocked(hold, why)
		return
	}
	hold.rearms++
	hold.state = slotHoldHeld
	hold.expired = false
	hold.deadline = s.clock()().Add(s.holdWindow())
	log.Printf(
		"[INFO] 优先级 %d 档位槽位预留：借用请求失败，续预留（第 %d/%d 次，剩余 %.0fs）",
		hold.tier, hold.rearms, slotSchedulerMaxRearms, s.holdWindow().Seconds(),
	)
}

// expireLocked 执行惰性到期检查（持 s.mu 调用）：
// ARMED 到期 → 删记录；HELD 到期 → 释放删记录；
// BORROWED 到期 → 标记 expired 等结算，超硬宽限 → 强制释放。
func (s *channelSlotScheduler) expireLocked(hold *slotHold) {
	if !s.aliveLocked(hold) {
		return
	}
	now := s.clock()()
	switch hold.state {
	case slotHoldArmed:
		if now.After(hold.armDeadline) {
			s.dropLocked(hold, "排队窗口到期")
		}
	case slotHoldHeld:
		if now.After(hold.deadline) {
			s.releaseLocked(hold, "预留到期")
		}
	case slotHoldBorrowed:
		if !hold.expired && now.After(hold.deadline) {
			hold.expired = true
		}
		if hold.expired && now.After(hold.deadline.Add(s.expiredGrace())) {
			log.Printf(
				"[WARN] 优先级 %d 档位槽位预留：借用到期 %.0fs 后仍未结算，强制释放",
				hold.tier, s.expiredGrace().Seconds(),
			)
			s.releaseLocked(hold, "借用结算超时硬兜底")
		}
	}
}

// dropLocked 删除一条仍处 ARMED 的 hold（持 s.mu 调用）。
// 幽灵等待者由其 pollCtx/轮次关闭自行清理，无跨锁引用需要解除。
func (s *channelSlotScheduler) dropLocked(hold *slotHold, reason string) {
	delete(s.holds, hold.tier)
	log.Printf("[INFO] 优先级 %d 档位槽位预留：取消（%s）", hold.tier, reason)
}

// releaseLocked 释放 hold 持有的槽位并删除记录（持 s.mu 调用）。
// limiter.mu 在 scheduler.mu 之内获取（releaseFunc），遵守单向锁序。
func (s *channelSlotScheduler) releaseLocked(hold *slotHold, reason string) {
	delete(s.holds, hold.tier)
	if hold.release != nil {
		hold.release()
		hold.release = nil
	}
	log.Printf("[INFO] 优先级 %d 档位槽位预留：释放（%s，渠道 %d）", hold.tier, reason, hold.channelID)
}

// RemoveChannel 清理渠道相关的全部 hold（渠道删除时调用）：
// ARMED → 从 candidates 剔除该渠道（幽灵不再给死渠道排队，防止落座死渠道
// 把整个档位的预留独占到到期）；剔除后候选空则整条 drop。
// HELD/BORROWED 落在该渠道 → 整条释放。
// 渠道禁用（非删除）不经过此路径：stale 候选只是幽灵空排队，无害。
func (s *channelSlotScheduler) RemoveChannel(channelID int64) {
	if s == nil || channelID <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for tier, hold := range s.holds {
		switch hold.state {
		case slotHoldArmed:
			// 新建 slice：幽灵 goroutine 可能正持旧快照遍历（runGhostWaiter
			// 在锁内取快照，但本轮 round 已在锁外使用），原地压缩会与其竞争。
			kept := make([]slotHoldCandidate, 0, len(hold.candidates))
			for _, cand := range hold.candidates {
				if cand.channelID != channelID {
					kept = append(kept, cand)
				}
			}
			hold.candidates = kept
			if len(hold.candidates) == 0 {
				s.dropLocked(hold, "候选渠道已全部删除")
			}
		case slotHoldHeld, slotHoldBorrowed:
			if hold.channelID == channelID {
				s.releaseLocked(hold, "渠道删除")
			}
		}
		_ = tier
	}
}

// prioritizeHeldChannel 把档位 HELD hold 所在渠道提到候选首位
//（仿 prioritizePinnedCodexChannel 先例）：同档位请求先经过持有预留的渠道，
// 把路由顺序与借用点对齐。hold 渠道不在候选中（如被令牌限制排除）时不重排。
func (s *channelSlotScheduler) prioritizeHeldChannel(cands []*model.Config, tier int) []*model.Config {
	s.mu.Lock()
	hold, ok := s.holds[tier]
	channelID := int64(0)
	if ok && hold != nil {
		s.expireLocked(hold)
		if s.aliveLocked(hold) && hold.state == slotHoldHeld {
			channelID = hold.channelID
		}
	}
	s.mu.Unlock()
	if channelID == 0 {
		return cands
	}
	for index, cfg := range cands {
		if cfg == nil || cfg.ID != channelID || index == 0 {
			continue
		}
		ordered := make([]*model.Config, 0, len(cands))
		ordered = append(ordered, cfg)
		ordered = append(ordered, cands[:index]...)
		ordered = append(ordered, cands[index+1:]...)
		return ordered
	}
	return cands
}
