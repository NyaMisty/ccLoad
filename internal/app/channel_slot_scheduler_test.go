package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"ccLoad/internal/model"
)

// ============================================================================
// channelSlotScheduler 单元测试
// ============================================================================

// newSchedulerForTest 构造独立 scheduler + limiter（挂到最小 Server 上）。
func newSchedulerForTest() (scheduler *channelSlotScheduler, limiter *channelConcurrencyLimiter) {
	limiter = newChannelConcurrencyLimiter()
	scheduler = newChannelSlotScheduler(nil, limiter)
	scheduler.baseCtx = context.Background()
	scheduler.window = 3 * time.Second
	scheduler.grace = 500 * time.Millisecond
	return scheduler, limiter
}

// waitForHoldState 轮询等待档位 hold 到达期望状态（或超时）。
// 状态读取全程持锁——hold 字段被幽灵 goroutine 并发写。
func waitForHoldState(t testing.TB, s *channelSlotScheduler, tier int, want slotHoldState) (*slotHold, bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		s.mu.Lock()
		hold, ok := s.holds[tier]
		matched := ok && hold.state == want
		s.mu.Unlock()
		if matched {
			return hold, true
		}
		if time.Now().After(deadline) {
			return hold, false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// armHoldSaturated 在 arm 前记录各渠道满载 Key——真实链路里 arm 必然发生在
// 满载之后（noteKeySaturation 已记录），测试直接构造等价状态。
func armHoldSaturated(s *channelSlotScheduler, cands []*model.Config, reqCtx *proxyRequestContext, keys map[int64]string) {
	for id, key := range keys {
		s.noteKeySaturation(reqCtx, id, key)
	}
	s.armHold(cands, reqCtx)
}

// TestSlotScheduler_GhostSeatsOnFirstGrant 验证全局档位幽灵：arm 后幽灵在多个
// 候选渠道排队，任一渠道释放即在该渠道落座（HELD），槽位 +1。
func TestSlotScheduler_GhostSeatsOnFirstGrant(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	const chA, chB = int64(1), int64(2)

	// 两个渠道都占满
	blockA, _, _, _, okA := limiter.acquire(chA, "key-a", 1, model.AuthTokenPriorityDefault)
	if !okA {
		t.Fatal("pre-acquire A failed")
	}
	blockB, _, _, _, okB := limiter.acquire(chB, "key-b", 1, model.AuthTokenPriorityDefault)
	if !okB {
		t.Fatal("pre-acquire B failed")
	}

	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	cands := []*model.Config{
		{ID: chA, Name: "a", MaxConcurrency: 1},
		{ID: chB, Name: "b", MaxConcurrency: 1},
	}
	armHoldSaturated(scheduler, cands, reqCtx, map[int64]string{chA: "key-a", chB: "key-b"})

	// B 先释放：幽灵应在 B 落座
	blockB()
	hold, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld)
	if !seated {
		t.Fatal("ghost never seated after B release")
	}
	scheduler.mu.Lock()
	seatedChannel := hold.channelID
	scheduler.mu.Unlock()
	if seatedChannel != chB {
		t.Fatalf("hold channel=%d, want %d (first released)", seatedChannel, chB)
	}

	// 落座渠道的槽位被 hold 占用：默认优先级获取 B 被拒
	if _, _, _, _, okAcq := limiter.acquire(chB, "key-b", 1, model.AuthTokenPriorityDefault); okAcq {
		t.Fatal("held slot must be occupied")
	}
	// A 不受影响
	blockA()
	if _, _, _, _, okA2 := limiter.acquire(chA, "key-a", 1, model.AuthTokenPriorityDefault); !okA2 {
		t.Fatal("channel A must be free")
	}
}

// TestSlotScheduler_TierKeySingleHold 验证 tier 键：同档位 arm 幂等（先到先得），
// 不同档位各自独立。
func TestSlotScheduler_TierKeySingleHold(t *testing.T) {
	t.Parallel()

	scheduler, _ := newSchedulerForTest()
	reqCtx := channelSlotTestRequestContext("prio-token", 200)

	scheduler.armHold([]*model.Config{{ID: 1, Name: "a", MaxConcurrency: 1}}, reqCtx)
	scheduler.armHold([]*model.Config{{ID: 2, Name: "b", MaxConcurrency: 1}}, reqCtx) // 同档位：no-op

	scheduler.mu.Lock()
	count := len(scheduler.holds)
	hold := scheduler.holds[200]
	scheduler.mu.Unlock()
	if count != 1 || hold.candidates[0].channelID != 1 {
		t.Fatalf("holds=%d first=%d, want 1 hold bound to channel 1 (first arm wins)", count, hold.candidates[0].channelID)
	}
}

// TestSlotScheduler_HoldableChannelBinding 验证 tier 化后的渠道绑定：
// hold 落座渠道 B 后，渠道 A 的 holdable 查询必须返回 false。
func TestSlotScheduler_HoldableChannelBinding(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	const chA, chB = int64(1), int64(2)

	blockA, _, _, _, okA := limiter.acquire(chA, "key-a", 1, model.AuthTokenPriorityDefault)
	if !okA {
		t.Fatal("pre-acquire A failed")
	}
	defer blockA()
	blockB, _, _, _, okB := limiter.acquire(chB, "key-b", 1, model.AuthTokenPriorityDefault)
	if !okB {
		t.Fatal("pre-acquire B failed")
	}
	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{
		{ID: chA, Name: "a", MaxConcurrency: 1},
		{ID: chB, Name: "b", MaxConcurrency: 1},
	}, reqCtx, map[int64]string{chA: "key-a", chB: "key-b"})
	blockB()

	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost never seated")
	}
	if _, borrowable := scheduler.holdable(chA, 200); borrowable {
		t.Fatal("hold seated on B must not be borrowable from A")
	}
	if _, borrowable := scheduler.holdable(chB, 200); !borrowable {
		t.Fatal("hold must be borrowable from its seated channel")
	}
}

// TestSlotScheduler_ArmKeyIdleSeatRespectsLimit 回归验证超卖修复：
// 满 Key（已达渠道 MaxConcurrency）上幽灵主动落座必须失败，不能 +1 超卖。
func TestSlotScheduler_ArmKeyIdleSeatRespectsLimit(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	const ch = int64(1)

	// 渠道 limit=2，已被占满
	r1, _, _, _, ok1 := limiter.acquire(ch, "key-a", 2, model.AuthTokenPriorityDefault)
	if !ok1 {
		t.Fatal("pre-acquire 1 failed")
	}
	r2, _, _, _, ok2 := limiter.acquire(ch, "key-a", 2, model.AuthTokenPriorityDefault)
	if !ok2 {
		t.Fatal("pre-acquire 2 failed")
	}
	_ = r1

	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	scheduler.noteKeySaturation(reqCtx, ch, "key-a")
	scheduler.armHold([]*model.Config{{ID: ch, Name: "a", MaxConcurrency: 2}}, reqCtx)

	// 等一两个轮询节拍，让幽灵尝试 armKey 兜底（limit=0 超卖 bug 会在这里 +1）
	time.Sleep(700 * time.Millisecond)
	limiter.mu.Lock()
	active := limiter.active[newKeyConcurrencyID(ch, "key-a")]
	limiter.mu.Unlock()
	if active > 2 {
		t.Fatalf("active=%d exceeds limit 2: idle seat oversold", active)
	}

	// 释放一个槽位后，兜底应在下一拍成功落座
	r2()
	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost should seat after slot freed")
	}
}

// TestSlotScheduler_BorrowLifecycle 验证借用生命周期：
// HELD → borrow（BORROWED，covers=true）→ settle 成功（释放，槽位归还）。
func TestSlotScheduler_BorrowLifecycle(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	const ch = int64(1)

	block, _, _, _, ok := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault)
	if !ok {
		t.Fatal("pre-acquire failed")
	}
	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{{ID: ch, Name: "a", MaxConcurrency: 1}}, reqCtx, map[int64]string{ch: "key-a"})
	block()

	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost never seated")
	}

	token := scheduler.borrow(ch, 200, nil)
	if token == nil {
		t.Fatal("borrow failed")
	}
	if !scheduler.covers(token, ch) {
		t.Fatal("fresh borrow must be covered")
	}
	if scheduler.covers(token, ch+1) {
		t.Fatal("borrow must not cover a different channel")
	}
	// 已借出：第二个借用者拿不到
	if second := scheduler.borrow(ch, 200, nil); second != nil {
		t.Fatal("double borrow must be rejected")
	}

	// settle 成功 → 槽位释放
	scheduler.settleBorrow(token, true, true)
	if _, held := scheduler.holdable(ch, 200); held {
		t.Fatal("hold must be consumed after successful settle")
	}
	if _, _, _, _, okAcq := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault); !okAcq {
		t.Fatal("slot must be free after consume")
	}
}

// TestSlotScheduler_SettleFailureRearms 验证失败结算续预留（≤ 上限）。
func TestSlotScheduler_SettleFailureRearms(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	const ch = int64(1)

	block, _, _, _, ok := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault)
	if !ok {
		t.Fatal("pre-acquire failed")
	}
	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{{ID: ch, Name: "a", MaxConcurrency: 1}}, reqCtx, map[int64]string{ch: "key-a"})
	block()

	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost never seated")
	}

	for i := 0; i <= slotSchedulerMaxRearms; i++ {
		token := scheduler.borrow(ch, 200, nil)
		if token == nil {
			t.Fatalf("round %d: borrow failed", i)
		}
		scheduler.settleBorrow(token, false, true)
	}
	if _, held := scheduler.holdable(ch, 200); held {
		t.Fatal("hold must be released after exceeding rearm limit")
	}
	if _, _, _, _, okAcq := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault); !okAcq {
		t.Fatal("slot must be free after rearm limit")
	}
}

// TestSlotScheduler_HeldExpiryAndBorrowedGrace 验证 HELD 到期释放与
// BORROWED 到期「宽限内覆盖、超宽限强释」两段语义（注入时钟）。
func TestSlotScheduler_HeldExpiryAndBorrowedGrace(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	const ch = int64(1)

	now := time.Now()
	scheduler.now = func() time.Time { return now }

	block, _, _, _, ok := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault)
	if !ok {
		t.Fatal("pre-acquire failed")
	}
	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{{ID: ch, Name: "a", MaxConcurrency: 1}}, reqCtx, map[int64]string{ch: "key-a"})
	block()

	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost never seated")
	}

	// HELD 到期 → holdable 惰性触发释放
	now = now.Add(scheduler.holdWindow() + time.Second)
	if _, held := scheduler.holdable(ch, 200); held {
		t.Fatal("hold must expire after window")
	}

	// 重建 hold 验证 BORROWED 两段语义
	block2, _, _, _, ok2 := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault)
	if !ok2 {
		t.Fatal("re-acquire failed")
	}
	reqCtx2 := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{{ID: ch, Name: "a", MaxConcurrency: 1}}, reqCtx2, map[int64]string{ch: "key-a"})
	block2()
	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost never seated (second round)")
	}
	token := scheduler.borrow(ch, 200, nil)
	if token == nil {
		t.Fatal("borrow failed")
	}
	// 到期但未超宽限：covers 仍真
	now = now.Add(scheduler.holdWindow() + 10*time.Millisecond)
	if !scheduler.covers(token, ch) {
		t.Fatal("borrowed token must stay covered within grace")
	}
	// 结算（失败）→ 释放而非续期
	scheduler.settleBorrow(token, false, true)
	if _, held := scheduler.holdable(ch, 200); held {
		t.Fatal("expired hold must release on settle")
	}

	// 第三轮：超硬宽限 → covers 变 false，惰性强释
	block3, _, _, _, ok3 := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault)
	if !ok3 {
		t.Fatal("third acquire failed")
	}
	reqCtx3 := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{{ID: ch, Name: "a", MaxConcurrency: 1}}, reqCtx3, map[int64]string{ch: "key-a"})
	block3()
	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost never seated (third round)")
	}
	token3 := scheduler.borrow(ch, 200, nil)
	if token3 == nil {
		t.Fatal("borrow failed (third round)")
	}
	now = now.Add(scheduler.holdWindow() + scheduler.expiredGrace() + time.Second)
	if scheduler.covers(token3, ch) {
		t.Fatal("token beyond hard grace must not be covered")
	}
	if _, held := scheduler.holdable(ch, 200); held {
		t.Fatal("hold must be force-released beyond grace")
	}
	if _, _, _, _, okAcq := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault); !okAcq {
		t.Fatal("slot must be free after force release")
	}
}

// TestSlotScheduler_RemoveChannelPrunesCandidates 验证死渠道剔除：
// ARMED 中删除候选渠道 → candidates 剔除该渠道；全部剔除 → hold 消失。
// 防止幽灵落座死渠道把整个档位预留独占到到期。
func TestSlotScheduler_RemoveChannelPrunesCandidates(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	const chA, chB = int64(1), int64(2)

	blockA, _, _, _, ok := limiter.acquire(chA, "key-a", 1, model.AuthTokenPriorityDefault)
	if !ok {
		t.Fatal("pre-acquire A failed")
	}
	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	scheduler.armHold([]*model.Config{
		{ID: chA, Name: "a", MaxConcurrency: 1},
		{ID: chB, Name: "b", MaxConcurrency: 1},
	}, reqCtx)
	defer func() { blockA() }()

	// 删除 A：candidates 只剩 B，幽灵只能落座 B
	scheduler.RemoveChannel(chA)
	scheduler.mu.Lock()
	hold := scheduler.holds[200]
	pruned := hold != nil && len(hold.candidates) == 1 && hold.candidates[0].channelID == chB
	scheduler.mu.Unlock()
	if !pruned {
		t.Fatalf("candidates must be pruned to [B], got %+v", hold)
	}

	// A 释放（渠道已删）：幽灵不落座 A
	blockA()
	time.Sleep(700 * time.Millisecond)
	scheduler.mu.Lock()
	state := scheduler.holds[200]
	ghostState := slotHoldArmed
	if state != nil {
		ghostState = state.state
	}
	scheduler.mu.Unlock()
	if ghostState != slotHoldArmed {
		t.Fatal("ghost must not seat on removed channel")
	}

	// 删除 B：candidates 空 → hold 整条消失
	scheduler.RemoveChannel(chB)
	scheduler.mu.Lock()
	_, exists := scheduler.holds[200]
	scheduler.mu.Unlock()
	if exists {
		t.Fatal("hold must be dropped when all candidates removed")
	}
}

// TestSlotScheduler_RemoveChannelReleasesHeld 验证 HELD hold 落在被删渠道时整条释放。
func TestSlotScheduler_RemoveChannelReleasesHeld(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	const ch = int64(1)

	block, _, _, _, ok := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault)
	if !ok {
		t.Fatal("pre-acquire failed")
	}
	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{{ID: ch, Name: "a", MaxConcurrency: 1}}, reqCtx, map[int64]string{ch: "key-a"})
	block()

	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost never seated")
	}
	scheduler.RemoveChannel(ch)
	scheduler.mu.Lock()
	_, exists := scheduler.holds[200]
	scheduler.mu.Unlock()
	if exists {
		t.Fatal("held hold on removed channel must be released")
	}
	if _, _, _, _, okAcq := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault); !okAcq {
		t.Fatal("slot must be free after release")
	}
}

// TestSlotScheduler_FanoutRoundTeardown 多渠道同时释放的拆除协议赛跑：
// 恰好处理一个授予、其余等待者全部 settle、无 granted waiter 残留队列。
func TestSlotScheduler_FanoutRoundTeardown(t *testing.T) {
	t.Parallel()

	limiter := newChannelConcurrencyLimiter()
	const chA, chB, chC = int64(1), int64(2), int64(3)

	releases := make([]func(), 0, 3)
	for _, ch := range []int64{chA, chB, chC} {
		r, _, _, _, ok := limiter.acquire(ch, "k", 1, model.AuthTokenPriorityDefault)
		if !ok {
			t.Fatalf("pre-acquire %d failed", ch)
		}
		releases = append(releases, r)
	}

	round := newSlotFanoutRound(
		context.Background(), limiter,
		[]*model.Config{{ID: chA}, {ID: chB}, {ID: chC}},
		200, false, 2*time.Second,
	)

	// 三渠道同时释放：三个授予并发到达
	var wg sync.WaitGroup
	for _, r := range releases {
		wg.Add(1)
		go func(r func()) { defer wg.Done(); r() }(r)
	}
	wg.Wait()

	ev := round.wait()
	if ev == nil {
		t.Fatal("no grant received")
	}
	round.close(ev)
	round.settleWinner(ev, false)

	// 队列必须清空：无 granted waiter 残留（会持续压制该渠道低优先级）
	deadline := time.Now().Add(2 * time.Second)
	for {
		limiter.mu.Lock()
		residual := 0
		for _, waiters := range limiter.channelWaiters {
			residual += len(waiters)
		}
		limiter.mu.Unlock()
		if residual == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d waiters still in queue after teardown", residual)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSlotScheduler_ArmExclusions 验证 arm 排除项：默认档位不 arm、
// CooldownFallback 候选不进 candidates。
func TestSlotScheduler_ArmExclusions(t *testing.T) {
	t.Parallel()

	scheduler, _ := newSchedulerForTest()

	// 默认档位：不 arm
	scheduler.armHold([]*model.Config{{ID: 1, Name: "a", MaxConcurrency: 1}},
		channelSlotTestRequestContext("normal", model.AuthTokenPriorityDefault))
	scheduler.mu.Lock()
	count := len(scheduler.holds)
	scheduler.mu.Unlock()
	if count != 0 {
		t.Fatalf("default priority must not arm, holds=%d", count)
	}

	// 高优但候选全是 CooldownFallback：不 arm
	scheduler.armHold([]*model.Config{{ID: 2, Name: "fb", MaxConcurrency: 1, CooldownFallback: true}},
		channelSlotTestRequestContext("prio", 200))
	scheduler.mu.Lock()
	count = len(scheduler.holds)
	scheduler.mu.Unlock()
	if count != 0 {
		t.Fatalf("fallback-only candidates must not arm, holds=%d", count)
	}
}

// ============================================================================
// 代理链路集成测试
// ============================================================================

// TestSlotScheduler_RequestFanoutLandsOnFreedChannel 验证请求级跨渠道并发排队：
// ch1 满载、ch8 稍后释放 → 请求不等 ch1 的完整预算，落 ch8。
func TestSlotScheduler_RequestFanoutLandsOnFreedChannel(t *testing.T) {
	t.Parallel()

	env, preferred, fallback, release := setupChannelSlotEnv(t)
	defer release()
	injectTokenPriorityForTest(env.server.authService, "prio-token", 200)
	env.server.preferredChannelKeySlotWait = 5 * time.Second
	fallback.MaxConcurrency = 1
	if _, err := env.store.UpdateConfig(context.Background(), fallback.ID, fallback); err != nil {
		t.Fatalf("UpdateConfig fallback: %v", err)
	}
	env.server.InvalidateChannelListCache()
	releaseFallback, _, _, _, ok := env.server.channelConcurrencyLimiter.acquire(fallback.ID, "sk-fallback", 1, model.AuthTokenPriorityDefault)
	if !ok {
		t.Fatal("pre-acquire fallback failed")
	}

	// 300ms 后释放 fallback：请求应从满载的 preferred 扇出并落到 fallback，
	// 而不是在 preferred 上耗完 5s 预算后才尝试 fallback
	time.AfterFunc(300*time.Millisecond, releaseFallback)

	start := time.Now()
	result, succeeded := env.server.runProxyAttemptLoopWithFailureBoundary(
		context.Background(),
		[]*model.Config{preferred, fallback},
		channelSlotTestRequestContext("prio-token", 200),
		newRecorder(),
		nil,
	)
	elapsed := time.Since(start)
	if !succeeded || result == nil || !result.succeeded {
		t.Fatalf("succeeded=%v result=%+v, want success via fanout", succeeded, result)
	}
	if result.channelID == nil || *result.channelID != fallback.ID {
		t.Fatalf("channelID=%v, want fallback %d (freed first)", result.channelID, fallback.ID)
	}
	if elapsed >= 2*time.Second {
		t.Fatalf("elapsed=%v, fanout must land on freed channel well before budget", elapsed)
	}
}

// TestSlotScheduler_BorrowViaKeyLoop 集成验证借用链路：
// hold 落座后，同档位请求经 tryChannelWithKeys 的 pin 直接借用成功。
func TestSlotScheduler_BorrowViaKeyLoop(t *testing.T) {
	t.Parallel()

	env, preferred, _, release := setupChannelSlotEnv(t)
	defer release()
	injectTokenPriorityForTest(env.server.authService, "prio-token", 200)
	scheduler := env.server.slotScheduler
	scheduler.window = 5 * time.Second

	// 手工 arm：幽灵在 preferred 排队（满载 Key 记录与真实链路等价）
	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{preferred}, reqCtx, map[int64]string{preferred.ID: "sk-preferred"})
	release()

	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost never seated")
	}

	// 同档位请求：借用（零排队）成功并消费 hold
	start := time.Now()
	result, err := env.server.tryChannelWithKeys(
		context.Background(), preferred,
		channelSlotTestRequestContext("prio-token", 200),
		newRecorder(),
	)
	if err != nil || result == nil || !result.succeeded {
		t.Fatalf("err=%v result=%+v, want borrowed success", err, result)
	}
	if elapsed := time.Since(start); elapsed >= 150*time.Millisecond {
		t.Fatalf("elapsed=%v, borrowed attempt must bypass queueing", elapsed)
	}
	scheduler.mu.Lock()
	_, exists := scheduler.holds[200]
	scheduler.mu.Unlock()
	if exists {
		t.Fatal("hold must be consumed by successful request")
	}
}

// TestSlotScheduler_PrioritizeHeldChannel 验证候选排序提前：
// hold 落座 fallback 后，同档位请求把 fallback 提到候选首位。
func TestSlotScheduler_PrioritizeHeldChannel(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	const chA, chB = int64(1), int64(2)

	blockA, _, _, _, okA := limiter.acquire(chA, "key-a", 1, model.AuthTokenPriorityDefault)
	if !okA {
		t.Fatal("pre-acquire A failed")
	}
	defer blockA()
	blockB, _, _, _, okB := limiter.acquire(chB, "key-b", 1, model.AuthTokenPriorityDefault)
	if !okB {
		t.Fatal("pre-acquire B failed")
	}
	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{
		{ID: chA, Name: "a", MaxConcurrency: 1},
		{ID: chB, Name: "b", MaxConcurrency: 1},
	}, reqCtx, map[int64]string{chA: "key-a", chB: "key-b"})
	blockB()

	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost never seated")
	}

	ordered := scheduler.prioritizeHeldChannel([]*model.Config{
		{ID: chA, Name: "a"}, {ID: chB, Name: "b"},
	}, 200)
	if len(ordered) != 2 || ordered[0].ID != chB {
		t.Fatalf("ordered=%v, want held channel B first", ordered)
	}
}

// TestSlotScheduler_BorrowedReleaseShortCircuit 验证 reserve 侧借用短路
//（含原生 Codex WS 路径共用的 reserveUpstreamRequest 层）。
func TestSlotScheduler_BorrowedReleaseShortCircuit(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	env := &proxyTestEnv{server: &Server{
		channelConcurrencyLimiter: limiter,
		slotScheduler:             scheduler,
	}}
	_ = env
	const ch = int64(7)

	block, _, _, _, ok := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault)
	if !ok {
		t.Fatal("pre-acquire failed")
	}
	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{{ID: ch, Name: "a", MaxConcurrency: 1}}, reqCtx, map[int64]string{ch: "key-a"})
	block()

	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost never seated")
	}
	token := scheduler.borrow(ch, 200, nil)
	if token == nil {
		t.Fatal("borrow failed")
	}

	s := env.server
	cfg := &model.Config{ID: ch, MaxConcurrency: 1, Name: "a"}
	ctx := context.WithValue(context.Background(), slotHoldBorrowContextKey{}, token)

	// 借用短路：不占新槽位，no-op release 不扰动 hold
	release, err := s.reserveUpstreamRequest(ctx, cfg, "key-a")
	if err != nil {
		t.Fatalf("borrowed reserve must succeed: %v", err)
	}
	release()
	if !scheduler.covers(token, ch) {
		t.Fatal("no-op release must not disturb the hold")
	}

	// 无凭据：正常 acquire——容量被 hold 占满，应失败
	if _, errPlain := s.reserveUpstreamRequest(context.Background(), cfg, "key-a"); errPlain == nil {
		t.Fatal("plain reserve must fail while hold occupies the slot")
	}

	// 错误渠道的凭据：不短路（渠道绑定防线）
	wrongCfg := &model.Config{ID: ch + 1, MaxConcurrency: 1, Name: "other"}
	if noop := scheduler.borrowedRelease(ctx, wrongCfg); noop != nil {
		t.Fatal("token must not short-circuit a different channel")
	}

	scheduler.settleBorrow(token, true, true)
}
