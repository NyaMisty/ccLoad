package app

import (
	"context"
	"net/http"
	"testing"
	"time"

	"ccLoad/internal/model"
	"ccLoad/internal/protocol"
)

// ============================================================================
// channelConcurrencyLimiter 渠道槽位优先队列单元测试
// ============================================================================

// TestChannelSlotQueue_GrantsHighestPriorityFirst 验证释放时把槽位授予
// 优先级最高的等待者（同优先级 FIFO），一次释放只授予一人；
// 被授予但未落座的等待者继续持有优先级占用（反偷窃窗口）。
func TestChannelSlotQueue_GrantsHighestPriorityFirst(t *testing.T) {
	t.Parallel()

	limiter := newChannelConcurrencyLimiter()
	release, _, _, _, ok := limiter.acquire(7, "key-a", 1, 100)
	if !ok {
		t.Fatal("pre-acquire failed")
	}
	defer release()

	grantedHigh := make(chan *channelSlotWaiter, 1)
	grantedLow := make(chan *channelSlotWaiter, 1)
	// 低优先级先入队，高优先级后入队
	go func() {
		grantedLow <- limiter.enqueueChannelSlotWait(context.Background(), 7, 100)
	}()
	time.Sleep(20 * time.Millisecond)
	go func() {
		grantedHigh <- limiter.enqueueChannelSlotWait(context.Background(), 7, 200)
	}()
	time.Sleep(20 * time.Millisecond) // 两个等待者都已入队

	go func() {
		time.Sleep(20 * time.Millisecond)
		release() // 第一次释放：高优先级（200）被授予
	}()

	high := <-grantedHigh
	if high == nil {
		t.Fatal("higher-priority waiter should be granted first")
	}

	// 授予未落座期间：同级/更低的新到 acquire 必须被反偷窃拦下（容量虽空闲）
	if _, _, _, preempted, ok2 := limiter.acquire(7, "key-b", 1, 100); ok2 || !preempted {
		t.Fatalf("acquire during grant window ok=%v preempted=%v, want rejected+preempted", ok2, preempted)
	}

	// 高优等待者落座：以其优先级 acquire 成功，settle 解除占用
	seated, _, _, _, ok3 := limiter.acquire(7, "key-a", 1, 200)
	if !ok3 {
		t.Fatal("granted waiter must be able to acquire with its own priority")
	}
	limiter.settleChannelSlotWaiter(high, true)

	// 落座后队列只剩低优先级等待者：新到默认优先级不再被 200 挡（同级竞速允许）
	if _, _, _, preempted, ok4 := limiter.acquire(7, "key-b", 1, 100); !ok4 || preempted {
		t.Fatalf("acquire after settle ok=%v preempted=%v, want allowed (same-level race)", ok4, preempted)
	}

	// 高优落座者释放 → 低优先级等待者被授予
	go func() {
		time.Sleep(10 * time.Millisecond)
		seated()
	}()
	low := <-grantedLow
	if low == nil {
		t.Fatal("lower-priority waiter should be granted after second release")
	}
	limiter.settleChannelSlotWaiter(low, false)
}

// TestChannelSlotQueue_CancelPrunesWaiter 验证 ctx 取消的等待者不残留：
// 后续授予跳过它并落到下一个有效等待者。
func TestChannelSlotQueue_CancelPrunesWaiter(t *testing.T) {
	t.Parallel()

	limiter := newChannelConcurrencyLimiter()
	release, _, _, _, ok := limiter.acquire(7, "key-a", 1, 100)
	if !ok {
		t.Fatal("pre-acquire failed")
	}
	defer release()

	// 低优先级等待者 50ms 后取消
	cancelCtx, cancel := context.WithCancel(context.Background())
	go func() { limiter.enqueueChannelSlotWait(cancelCtx, 7, 100) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond) // 取消清理完成

	granted := make(chan struct{})
	go func() {
		limiter.enqueueChannelSlotWait(context.Background(), 7, 200)
		close(granted)
	}()
	time.Sleep(20 * time.Millisecond)

	go func() {
		time.Sleep(20 * time.Millisecond)
		release()
	}()
	select {
	case <-granted:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled waiter should not block granting the next one")
	}
}

// TestChannelSlotQueue_AntiStealing 验证反偷窃：容量空闲但存在严格更高优先级
// 等待者时 acquire 被拒且 preempted=true；同级等待者不构成阻挡。
func TestChannelSlotQueue_AntiStealing(t *testing.T) {
	t.Parallel()

	limiter := newChannelConcurrencyLimiter()
	// 占满再释放，制造「容量空闲 + 队列有人」的局面
	release, _, _, _, ok := limiter.acquire(7, "key-a", 1, 100)
	if !ok {
		t.Fatal("pre-acquire failed")
	}

	highInQueue := make(chan struct{})
	go func() {
		limiter.enqueueChannelSlotWait(context.Background(), 7, 200)
		close(highInQueue)
	}()
	time.Sleep(20 * time.Millisecond) // 200 已入队

	release() // 容量空闲，但槽位刚被授予给 200（其 goroutine 即将重试 acquire）

	// 高优等待者尚未重新 acquire：默认优先级的新到请求必须被反偷窃拦下
	_, _, _, preempted, ok2 := limiter.acquire(7, "key-b", 1, 100)
	if ok2 || !preempted {
		t.Fatalf("acquire ok=%v preempted=%v, want rejected+preempted (higher waiter queued)", ok2, preempted)
	}

	// 同级等待者不挡：先等 200 拿到槽位
	<-highInQueue // 200 被授予（ready 关闭）
	time.Sleep(50 * time.Millisecond)

	// 现在构造同级场景：200 持有槽位、队列里没有更高者 → key-b 同级获取应成功
	// （200 的重试会先拿到 key-a 的槽位）
	deadline := time.Now().Add(2 * time.Second)
	for {
		releaseB, _, _, preemptedB, okB := limiter.acquire(7, "key-b", 1, 200)
		if okB {
			releaseB()
			break
		}
		if !preemptedB {
			t.Fatalf("same-priority acquire rejected without preemption flag: ok=%v", okB)
		}
		if time.Now().After(deadline) {
			t.Fatal("same-priority acquire kept being preempted; same-level waiters must not block")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestChannelSlotQueue_ReleaseWithoutWaitersNoPanic 验证无等待者时释放不 panic。
func TestChannelSlotQueue_ReleaseWithoutWaitersNoPanic(t *testing.T) {
	t.Parallel()

	limiter := newChannelConcurrencyLimiter()
	release, _, _, _, ok := limiter.acquire(9, "key-a", 1, 100)
	if !ok {
		t.Fatal("acquire failed")
	}
	release() // 不应 panic
}

// ============================================================================
// 代理链路集成测试
// ============================================================================

// setupChannelSlotEnv 构造双渠道环境：preferred（高优先级、单 Key 并发 1，槽位被预占）
// 与 fallback（低优先级）。releasePreferred 控制槽位释放时机。
func setupChannelSlotEnv(t testing.TB) (env *proxyTestEnv, preferred, fallback *model.Config, releasePreferred func()) {
	t.Helper()

	upstream := newTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chat-1","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	t.Cleanup(upstream.Close)

	env = setupProxyTestEnv(t, []testChannel{
		{name: "preferred", upstreamProtocol: "openai", models: "gpt-test", apiKey: "sk-preferred", priority: 59},
		{name: "fallback", upstreamProtocol: "openai", models: "gpt-test", apiKey: "sk-fallback", priority: 10},
	}, map[int]string{0: upstream.URL, 1: upstream.URL})

	ctx := context.Background()
	cfgs, err := env.store.ListConfigs(ctx)
	if err != nil {
		t.Fatalf("ListConfigs: %v", err)
	}
	for _, cfg := range cfgs {
		switch cfg.Name {
		case "preferred":
			preferred = cfg
		case "fallback":
			fallback = cfg
		}
	}
	if preferred == nil || fallback == nil {
		t.Fatalf("missing channels: preferred=%v fallback=%v", preferred, fallback)
	}
	preferred.MaxConcurrency = 1
	if _, err := env.store.UpdateConfig(ctx, preferred.ID, preferred); err != nil {
		t.Fatalf("UpdateConfig preferred: %v", err)
	}
	env.server.InvalidateChannelListCache()

	release, _, _, _, ok := env.server.channelConcurrencyLimiter.acquire(preferred.ID, "sk-preferred", 1, model.AuthTokenPriorityDefault)
	if !ok {
		t.Fatal("pre-acquire preferred key failed")
	}
	return env, preferred, fallback, release
}

func injectTokenPriorityForTest(svc *AuthService, token string, priority int) {
	tokenHash := model.HashToken(token)
	svc.authTokensMux.Lock()
	svc.authTokenPriorities[tokenHash] = priority
	svc.authTokensMux.Unlock()
}

func channelSlotTestRequestContext(token string, priority int) *proxyRequestContext {
	return &proxyRequestContext{
		originalModel:  "gpt-test",
		clientProtocol: protocol.OpenAI,
		requestMethod:  http.MethodPost,
		requestPath:    "/v1/chat/completions",
		body:           []byte(`{"model":"gpt-test","messages":[{"role":"user","content":"hi"}]}`),
		header:         make(http.Header),
		tokenHash:      model.HashToken(token),
		tokenPriority:  priority,
	}
}

// TestChannelSlotQueue_HighPriorityWaitsAndSucceeds 验证高优先级令牌在首选渠道
// Key 并发满载时排队等待授予，成功后仍落在首选渠道。
func TestChannelSlotQueue_HighPriorityWaitsAndSucceeds(t *testing.T) {
	t.Parallel()

	env, preferred, fallback, release := setupChannelSlotEnv(t)
	defer release()
	injectTokenPriorityForTest(env.server.authService, "prio-token", 200)
	env.server.preferredChannelKeySlotWait = 5 * time.Second

	time.AfterFunc(150*time.Millisecond, release) // 释放触发授予

	start := time.Now()
	result, succeeded := env.server.runProxyAttemptLoopWithFailureBoundary(
		context.Background(),
		[]*model.Config{preferred, fallback},
		channelSlotTestRequestContext("prio-token", 200),
		newRecorder(),
		nil,
	)
	if !succeeded || result == nil || !result.succeeded {
		t.Fatalf("succeeded=%v result=%+v, want success on preferred channel", succeeded, result)
	}
	if result.channelID == nil || *result.channelID != preferred.ID {
		got := int64(-1)
		if result.channelID != nil {
			got = *result.channelID
		}
		t.Fatalf("channelID=%d, want preferred channel %d", got, preferred.ID)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("elapsed=%v, want ≥100ms (waited for slot grant)", elapsed)
	}
}

// TestChannelSlotQueue_DefaultPriorityAlsoWaits 验证默认优先级令牌同样排队等待
//（区别于 v1 的立即溢出）：槽位释放后落在首选渠道。
func TestChannelSlotQueue_DefaultPriorityAlsoWaits(t *testing.T) {
	t.Parallel()

	env, preferred, fallback, release := setupChannelSlotEnv(t)
	defer release()
	// 不注入优先级：默认 100
	env.server.preferredChannelKeySlotWait = 5 * time.Second

	time.AfterFunc(150*time.Millisecond, release)

	result, succeeded := env.server.runProxyAttemptLoopWithFailureBoundary(
		context.Background(),
		[]*model.Config{preferred, fallback},
		channelSlotTestRequestContext("normal-token", model.AuthTokenPriorityDefault),
		newRecorder(),
		nil,
	)
	if !succeeded || result == nil || !result.succeeded {
		t.Fatalf("succeeded=%v result=%+v, want success on preferred channel", succeeded, result)
	}
	if result.channelID == nil || *result.channelID != preferred.ID {
		t.Fatalf("channelID=%v, want preferred channel %d (default priority must also queue)", result.channelID, preferred.ID)
	}
}

// TestChannelSlotQueue_TimeoutSpillsToNextChannel 验证预算耗尽后按
// ErrKeyConcurrencyExceeded 正常溢出到下一渠道。
func TestChannelSlotQueue_TimeoutSpillsToNextChannel(t *testing.T) {
	t.Parallel()

	env, preferred, fallback, release := setupChannelSlotEnv(t)
	defer release()
	injectTokenPriorityForTest(env.server.authService, "prio-token", 200)
	env.server.preferredChannelKeySlotWait = 300 * time.Millisecond

	result, succeeded := env.server.runProxyAttemptLoopWithFailureBoundary(
		context.Background(),
		[]*model.Config{preferred, fallback},
		channelSlotTestRequestContext("prio-token", 200),
		newRecorder(),
		nil,
	)
	if !succeeded || result == nil || !result.succeeded {
		t.Fatalf("succeeded=%v result=%+v, want success via fallback after timeout", succeeded, result)
	}
	if result.channelID == nil || *result.channelID != fallback.ID {
		t.Fatalf("channelID=%v, want fallback channel %d", result.channelID, fallback.ID)
	}
}

// TestChannelSlotQueue_BudgetSharedAcrossChannels 验证等待预算是请求级共享的：
// 两个渠道都满载时，总等待时间不超过单份预算（而非 2×）。
func TestChannelSlotQueue_BudgetSharedAcrossChannels(t *testing.T) {
	t.Parallel()

	env, preferred, fallback, release := setupChannelSlotEnv(t)
	defer release()
	injectTokenPriorityForTest(env.server.authService, "prio-token", 200)
	env.server.preferredChannelKeySlotWait = 300 * time.Millisecond
	fallback.MaxConcurrency = 1
	if _, err := env.store.UpdateConfig(context.Background(), fallback.ID, fallback); err != nil {
		t.Fatalf("UpdateConfig fallback: %v", err)
	}
	env.server.InvalidateChannelListCache()
	releaseFallback, _, _, _, ok := env.server.channelConcurrencyLimiter.acquire(fallback.ID, "sk-fallback", 1, model.AuthTokenPriorityDefault)
	if !ok {
		t.Fatal("pre-acquire fallback key failed")
	}
	defer releaseFallback()

	start := time.Now()
	_, succeeded := env.server.runProxyAttemptLoopWithFailureBoundary(
		context.Background(),
		[]*model.Config{preferred, fallback},
		channelSlotTestRequestContext("prio-token", 200),
		newRecorder(),
		nil,
	)
	elapsed := time.Since(start)
	if succeeded {
		t.Fatal("both channels saturated without release; must not succeed")
	}
	if elapsed >= 600*time.Millisecond {
		t.Fatalf("elapsed=%v, budget must be shared across channels (<2×300ms)", elapsed)
	}
}

// TestChannelSlotQueue_Exclusions 验证全冷却兜底探测与亲和探测不排队。
func TestChannelSlotQueue_Exclusions(t *testing.T) {
	t.Parallel()

	env, preferred, _, release := setupChannelSlotEnv(t)
	defer release()

	fallbackCfg := model.Config{ID: 999, Name: "cooldown-fallback", CooldownFallback: true}
	if env.server.shouldWaitForKeySlot(&fallbackCfg, channelSlotTestRequestContext("any", model.AuthTokenPriorityDefault)) {
		t.Fatal("cooldown fallback candidate must not queue")
	}

	probeCtx := channelSlotTestRequestContext("any", model.AuthTokenPriorityDefault)
	probeCtx.claudeAffinityProbe = true
	if env.server.shouldWaitForKeySlot(preferred, probeCtx) {
		t.Fatal("affinity probe must not queue")
	}

	if !env.server.shouldWaitForKeySlot(preferred, channelSlotTestRequestContext("any", model.AuthTokenPriorityDefault)) {
		t.Fatal("ordinary attempt with fresh budget should queue")
	}

	expiredCtx := channelSlotTestRequestContext("any", model.AuthTokenPriorityDefault)
	expiredCtx.slotWaitDeadline = time.Now().Add(-time.Second)
	if env.server.shouldWaitForKeySlot(preferred, expiredCtx) {
		t.Fatal("expired budget must not queue")
	}
}

// TestChannelSlotQueue_ArrivalDoesNotStealGrantedSlot 集成级验证反偷窃：
// 高优先级令牌排队等待期间，新到的默认优先级请求不能抢占该渠道的空闲槽位。
func TestChannelSlotQueue_ArrivalDoesNotStealGrantedSlot(t *testing.T) {
	t.Parallel()

	env, preferred, _, release := setupChannelSlotEnv(t)
	defer release()
	injectTokenPriorityForTest(env.server.authService, "prio-token", 200)
	env.server.preferredChannelKeySlotWait = 3 * time.Second

	// 高优请求先入场排队
	type outcome struct {
		channelID int64
		succeeded bool
	}
	done := make(chan outcome, 1)
	go func() {
		result, succeeded := env.server.runProxyAttemptLoopWithFailureBoundary(
			context.Background(),
			[]*model.Config{preferred},
			channelSlotTestRequestContext("prio-token", 200),
			newRecorder(),
			nil,
		)
		oc := outcome{succeeded: succeeded}
		if result != nil && result.channelID != nil {
			oc.channelID = *result.channelID
		}
		done <- oc
	}()
	time.Sleep(100 * time.Millisecond) // 高优已进入等待队列

	// 阶段1：高优在队、槽位被预占——新到默认优先级请求被拒
	if _, _, _, _, okAcq := env.server.channelConcurrencyLimiter.acquire(preferred.ID, "sk-preferred", 1, model.AuthTokenPriorityDefault); okAcq {
		t.Fatal("acquire must fail while the slot is held and a high-priority waiter is queued")
	}

	// 阶段2：确定性验证授予窗口的反偷窃——绕开代理路径直接驱动 limiter：
	// 构造「容量空闲 + 200 已授予未落座」，断言默认优先级 acquire 被拒且 preempted=true。
	limiter := env.server.channelConcurrencyLimiter
	waiter := make(chan *channelSlotWaiter, 1)
	go func() { waiter <- limiter.enqueueChannelSlotWait(context.Background(), preferred.ID, 200) }()
	time.Sleep(30 * time.Millisecond) // 入队

	// 释放被预占的槽位 → 授予给 200（其 goroutine 阻塞在 ready，尚未落座）
	releaseTriggered := make(chan struct{})
	go func() {
		release()
		close(releaseTriggered)
	}()
	<-releaseTriggered
	time.Sleep(30 * time.Millisecond) // 授予已发生（可能授予给最早入队的后台 200，它随即落座）

	// 反偷窃核心断言：仍存在 granted 未落座或已入队未授予的 200 时，
	// 默认优先级获取被拒且 preempted=true。若后台 200 已完成全链路，
	// 队列清空则本断言退化为容量检查——由 done 通道的成功结果兜底验证。
	if releaseProbe, _, _, preempted, okP := limiter.acquire(preferred.ID, "sk-other", 1, model.AuthTokenPriorityDefault); okP {
		releaseProbe()
		t.Fatalf("default-priority acquire succeeded during grant window (preempted=%v)", preempted)
	} else if !preempted {
		t.Fatal("rejection during grant window must be marked preempted")
	}

	select {
	case oc := <-done:
		if !oc.succeeded || oc.channelID != preferred.ID {
			t.Fatalf("high-priority outcome=%+v, want success on preferred channel", oc)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("high-priority waiter never got served")
	}
}
