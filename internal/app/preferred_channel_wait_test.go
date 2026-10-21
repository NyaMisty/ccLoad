package app

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"ccLoad/internal/model"
	"ccLoad/internal/protocol"
)

// setupPreferredChannelEnv 构造双渠道环境：preferred（高优先级）与 fallback（低优先级）。
// preferred 渠道单 Key 并发上限 1，Key 的槽位被预占，由调用方控制释放时机。
func setupPreferredChannelEnv(t testing.TB) (env *proxyTestEnv, preferred, fallback *model.Config, releasePreferred func()) {
	t.Helper()

	// 两个渠道指向同一上游：返回体一致，路由归属由 result.channelID 断言
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
	if len(cfgs) != 2 {
		t.Fatalf("configs=%d, want 2", len(cfgs))
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
		t.Fatalf("missing test channels: preferred=%v fallback=%v", preferred, fallback)
	}
	preferred.MaxConcurrency = 1
	if _, err := env.store.UpdateConfig(ctx, preferred.ID, preferred); err != nil {
		t.Fatalf("UpdateConfig preferred: %v", err)
	}
	env.server.InvalidateChannelListCache()

	// 预占 preferred 渠道唯一 Key 的全部并发槽位
	release, _, _, ok := env.server.channelConcurrencyLimiter.acquire(preferred.ID, "sk-preferred", 1)
	if !ok {
		t.Fatal("pre-acquire preferred key failed")
	}
	return env, preferred, fallback, release
}

// injectTokenPriority 直接写入优先级映射，绕过 admin CRUD。
func injectTokenPriority(svc *AuthService, token string, priority int) {
	tokenHash := model.HashToken(token)
	svc.authTokensMux.Lock()
	svc.authTokenPriorities[tokenHash] = priority
	svc.authTokensMux.Unlock()
}

func preferredWaitTestRequestContext(token string) *proxyRequestContext {
	return &proxyRequestContext{
		originalModel:  "gpt-test",
		clientProtocol: protocol.OpenAI,
		requestMethod:  http.MethodPost,
		requestPath:    "/v1/chat/completions",
		body:           []byte(`{"model":"gpt-test","messages":[{"role":"user","content":"hi"}]}`),
		header:         make(http.Header),
		tokenHash:      model.HashToken(token),
	}
}

// TestPreferredChannelWait_HighPriorityWaitsAndSucceeds 验证高优先级令牌在首选渠道
// Key 并发满载时等待槽位释放，成功后仍落在首选渠道而不是溢出。
func TestPreferredChannelWait_HighPriorityWaitsAndSucceeds(t *testing.T) {
	t.Parallel()

	env, preferred, fallback, release := setupPreferredChannelEnv(t)
	defer release()
	injectTokenPriority(env.server.authService, "prio-token", 200)
	env.server.preferredChannelKeySlotWait = 5 * time.Second

	// 150ms 后释放槽位，等待中的请求应在预算内拿到并成功
	time.AfterFunc(150*time.Millisecond, release)

	start := time.Now()
	result, succeeded := env.server.runProxyAttemptLoopWithFailureBoundary(
		context.Background(),
		[]*model.Config{preferred, fallback},
		preferredWaitTestRequestContext("prio-token"),
		newRecorder(),
		nil,
	)
	if !succeeded || result == nil || !result.succeeded {
		t.Fatalf("succeeded=%v result=%+v, want success on preferred channel", succeeded, result)
	}
	if result.channelID == nil || *result.channelID != preferred.ID {
		t.Fatalf("channelID=%v, want preferred channel %d", result.channelID, preferred.ID)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("elapsed=%v, want ≥100ms (waited for slot release)", elapsed)
	}
}

// TestPreferredChannelWait_DefaultPrioritySpillsImmediately 验证默认优先级令牌不等待，
// 立即溢出到低优先级渠道。
func TestPreferredChannelWait_DefaultPrioritySpillsImmediately(t *testing.T) {
	t.Parallel()

	env, preferred, fallback, release := setupPreferredChannelEnv(t)
	defer release()
	// 不注入优先级：默认 100 → 不等待

	start := time.Now()
	result, succeeded := env.server.runProxyAttemptLoopWithFailureBoundary(
		context.Background(),
		[]*model.Config{preferred, fallback},
		preferredWaitTestRequestContext("normal-token"),
		newRecorder(),
		nil,
	)
	if !succeeded || result == nil || !result.succeeded {
		t.Fatalf("succeeded=%v result=%+v, want success via fallback", succeeded, result)
	}
	if result.channelID == nil || *result.channelID != fallback.ID {
		t.Fatalf("channelID=%v, want fallback channel %d", result.channelID, fallback.ID)
	}
	if elapsed := time.Since(start); elapsed >= preferredChannelKeySlotWaitBudget {
		t.Fatalf("elapsed=%v, default-priority request must not wait the full budget", elapsed)
	}
}

// TestPreferredChannelWait_TimeoutSpillsToNextChannel 验证预算耗尽后按
// ErrKeyConcurrencyExceeded 正常溢出到下一渠道。
func TestPreferredChannelWait_TimeoutSpillsToNextChannel(t *testing.T) {
	t.Parallel()

	env, preferred, fallback, release := setupPreferredChannelEnv(t)
	defer release()
	injectTokenPriority(env.server.authService, "prio-token", 200)
	env.server.preferredChannelKeySlotWait = 300 * time.Millisecond

	result, succeeded := env.server.runProxyAttemptLoopWithFailureBoundary(
		context.Background(),
		[]*model.Config{preferred, fallback},
		preferredWaitTestRequestContext("prio-token"),
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

// TestPreferredChannelWait_Exclusions 验证兜底探测、亲和探测与非首选位置不等待。
func TestPreferredChannelWait_Exclusions(t *testing.T) {
	t.Parallel()

	env, preferred, _, release := setupPreferredChannelEnv(t)
	defer release()
	injectTokenPriority(env.server.authService, "prio-token", 200)

	fallbackCfg := model.Config{ID: 999, Name: "cooldown-fallback", CooldownFallback: true}
	if env.server.shouldWaitForPreferredChannel(&fallbackCfg, preferredWaitTestRequestContext("prio-token"), 0) {
		t.Fatal("cooldown fallback candidate must not wait")
	}

	probeCtx := preferredWaitTestRequestContext("prio-token")
	probeCtx.claudeAffinityProbe = true
	if env.server.shouldWaitForPreferredChannel(preferred, probeCtx, 0) {
		t.Fatal("affinity probe must not wait")
	}

	if env.server.shouldWaitForPreferredChannel(preferred, preferredWaitTestRequestContext("prio-token"), 1) {
		t.Fatal("non-first candidate must not wait")
	}
	if !env.server.shouldWaitForPreferredChannel(preferred, preferredWaitTestRequestContext("prio-token"), 0) {
		t.Fatal("first candidate with high priority should wait")
	}
}

// TestChannelConcurrencyLimiter_ChannelReleaseBroadcast 验证渠道级释放广播：
// 任一 Key 释放唤醒全部等待者，ctx 取消立即返回。
func TestChannelConcurrencyLimiter_ChannelReleaseBroadcast(t *testing.T) {
	t.Parallel()

	limiter := newChannelConcurrencyLimiter()

	// 占满两个 Key 的槽位：由 key-a 释放事件唤醒等待者
	releaseA, _, _, ok := limiter.acquire(7, "key-a", 1)
	if !ok {
		t.Fatal("pre-acquire key-a failed")
	}
	releaseB, _, _, ok := limiter.acquire(7, "key-b", 1)
	if !ok {
		t.Fatal("pre-acquire key-b failed")
	}

	var wg sync.WaitGroup
	woken := make(chan struct{}, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := limiter.waitForChannelRelease(context.Background(), 7); err != nil {
				t.Errorf("waitForChannelRelease: %v", err)
			}
			woken <- struct{}{}
		}()
	}

	// ctx 取消路径。释放广播与取消竞争时任一分支获胜都合法，只验证必然返回。
	cancelCtx, cancel := context.WithCancel(context.Background())
	goCancel := make(chan struct{})
	go func() {
		//nolint:errcheck // 返回 nil（广播先到）或 context.Canceled（取消先到）都正确
		_ = limiter.waitForChannelRelease(cancelCtx, 7)
		close(goCancel)
	}()

	time.Sleep(50 * time.Millisecond) // 等待全部等待者注册
	releaseA()                        // 任一 Key 释放 → 渠道广播

	for i := 0; i < 2; i++ {
		select {
		case <-woken:
		case <-time.After(2 * time.Second):
			t.Fatal("channel release broadcast did not wake waiters")
		}
	}

	// 独立验证 ctx 取消：无释放事件干扰时必须以 context.Canceled 返回
	cancelOnlyCtx, cancelOnly := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- limiter.waitForChannelRelease(cancelOnlyCtx, 8) }()
	time.Sleep(20 * time.Millisecond)
	cancelOnly()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled-only wait error=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled wait did not return")
	}

	cancel()
	select {
	case <-goCancel:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled wait did not return")
	}
	releaseB()
	wg.Wait()

	// 无等待者时释放不应 panic（广播 map 为空路径）
	releaseC, _, _, ok := limiter.acquire(7, "key-c", 1)
	if !ok {
		t.Fatal("acquire key-c failed")
	}
	releaseC()
}
