package app

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"ccLoad/internal/model"
	"ccLoad/internal/protocol"
)

func TestCooldownFallbackProber_ThrottlesPerChannel(t *testing.T) {
	t.Parallel()

	fakeNow := time.Date(2026, 9, 10, 18, 0, 0, 0, time.Local)
	prober := &cooldownFallbackProber{
		interval: time.Minute,
		now:      func() time.Time { return fakeNow },
	}

	if !prober.allowProbe(1) {
		t.Fatal("首次探测应放行")
	}
	if prober.allowProbe(1) {
		t.Fatal("窗口期内同渠道重复探测应被拒")
	}
	if !prober.allowProbe(2) {
		t.Fatal("限频按渠道隔离，渠道 2 首次探测应放行")
	}

	fakeNow = fakeNow.Add(time.Minute)
	if !prober.allowProbe(1) {
		t.Fatal("窗口结束后应重新放行探测")
	}

	prober.RemoveChannel(1)
	if !prober.allowProbe(1) {
		t.Fatal("RemoveChannel 后应视为从未探测")
	}

	var nilProber *cooldownFallbackProber
	if !nilProber.allowProbe(1) {
		t.Fatal("nil prober 必须放行（降级策略）")
	}
}

// 全冷却场景下：第一次选路返回兜底渠道，窗口期内的后续选路快速失败（空候选），
// 避免每个入站请求都真实请求冷却中的上游。
func TestSelectRouteCandidates_AllCooled_FallbackProbeThrottled(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	server := &Server{store: store, channelBalancer: NewSmoothWeightedRR()}
	fakeNow := time.Now()
	server.cooldownFallbackProbe.interval = time.Minute
	server.cooldownFallbackProbe.now = func() time.Time { return fakeNow }

	ctx := context.Background()
	cfg, err := store.CreateConfig(ctx, &model.Config{
		Name:         "fallback-throttle",
		URLs:         model.ChannelURLs{{URL: "https://api1.com"}},
		Priority:     100,
		Enabled:      true,
		ModelEntries: []model.ModelEntry{{Model: "test-model"}},
	})
	if err != nil {
		t.Fatalf("创建测试渠道失败: %v", err)
	}
	if err := store.SetChannelCooldown(ctx, cfg.ID, time.Now().Add(30*time.Minute)); err != nil {
		t.Fatalf("设置渠道冷却失败: %v", err)
	}

	candidates, err := server.selectCandidatesByModelAndClientProtocol(ctx, "test-model", "")
	if err != nil {
		t.Fatalf("selectCandidates失败: %v", err)
	}
	if len(candidates) != 1 || !candidates[0].CooldownFallback {
		t.Fatalf("首次应返回兜底渠道，实际 %+v", candidates)
	}

	candidates, err = server.selectCandidatesByModelAndClientProtocol(ctx, "test-model", "")
	if err != nil {
		t.Fatalf("selectCandidates失败: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("窗口期内重复兜底应返回空候选（快速失败），实际 %+v", candidates)
	}

	fakeNow = fakeNow.Add(time.Minute)
	candidates, err = server.selectCandidatesByModelAndClientProtocol(ctx, "test-model", "")
	if err != nil {
		t.Fatalf("selectCandidates失败: %v", err)
	}
	if len(candidates) != 1 || !candidates[0].CooldownFallback {
		t.Fatalf("窗口结束后应重新允许兜底探测，实际 %+v", candidates)
	}
}

// 兜底渠道单请求只允许 1 次真实上游探测，不随 maxKeyRetries/Key 数量扇出。
func TestTryChannelWithKeys_CooldownFallbackTriesSingleKey(t *testing.T) {
	t.Parallel()

	var upstreamHits atomic.Int32
	upstream := newTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHits.Add(1)
		resetAt := time.Now().Add(time.Hour).Format("2006-01-02 15:04:05")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"type":"error","error":{"type":"rate_limit_error","code":"1308","message":"[1308][Usage limit reached for 5 hour. Your limit will reset at ` + resetAt + `]"}}`))
	}))
	defer upstream.Close()

	env := setupProxyTestEnv(t, []testChannel{
		{name: "fallback-single-key", models: "gpt-4", apiKey: "sk-first"},
	}, map[int]string{0: upstream.URL})

	ctx := context.Background()
	cfgs, err := env.store.ListConfigs(ctx)
	if err != nil || len(cfgs) != 1 {
		t.Fatalf("ListConfigs: configs=%d err=%v", len(cfgs), err)
	}
	cfg := cfgs[0]
	if err := env.store.CreateAPIKeysBatch(ctx, []*model.APIKey{
		{ChannelID: cfg.ID, KeyIndex: 1, APIKey: "sk-second"},
		{ChannelID: cfg.ID, KeyIndex: 2, APIKey: "sk-third"},
	}); err != nil {
		t.Fatalf("CreateAPIKeysBatch failed: %v", err)
	}

	// 全部 Key 冷却：模拟“所有 Key 都触达上游配额”的全冷却状态
	for keyIndex := range 3 {
		if err := env.store.SetKeyCooldown(ctx, cfg.ID, keyIndex, time.Now().Add(time.Hour)); err != nil {
			t.Fatalf("SetKeyCooldown failed: %v", err)
		}
	}
	env.server.InvalidateAPIKeysCache(cfg.ID)
	env.server.maxKeyRetries = 10

	cfg.CooldownFallback = true
	body := []byte(`{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}`)
	result, err := env.server.tryChannelWithKeys(ctx, cfg, &proxyRequestContext{
		originalModel:  "gpt-4",
		clientProtocol: protocol.OpenAI,
		requestMethod:  http.MethodPost,
		requestPath:    "/v1/chat/completions",
		body:           body,
		header:         make(http.Header),
	}, newRecorder())
	if err != nil {
		t.Fatalf("兜底探测应返回最后一次失败而非错误: %v", err)
	}
	if result == nil || result.status != http.StatusTooManyRequests {
		t.Fatalf("兜底探测结果应为上游 429，实际 %+v", result)
	}
	if got := upstreamHits.Load(); got != 1 {
		t.Fatalf("兜底探测只允许 1 次上游请求，实际 %d 次", got)
	}
}
