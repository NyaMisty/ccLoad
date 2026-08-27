package app

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"ccLoad/internal/model"
	"ccLoad/internal/protocol"
)

func TestKeyConcurrencyLimiterIsolatesKeys(t *testing.T) {
	t.Parallel()

	limiter := newChannelConcurrencyLimiter()

	releaseA, active, limit, ok := limiter.acquire(7, "key-a", 1)
	if !ok || active != 1 || limit != 1 {
		t.Fatalf("first acquire got active=%d limit=%d ok=%v, want 1,1,true", active, limit, ok)
	}

	_, active, limit, ok = limiter.acquire(7, "key-a", 1)
	if ok || active != 1 || limit != 1 {
		t.Fatalf("second acquire for key-a got active=%d limit=%d ok=%v, want 1,1,false", active, limit, ok)
	}

	releaseB, active, limit, ok := limiter.acquire(7, "key-b", 1)
	if !ok || active != 1 || limit != 1 {
		t.Fatalf("first acquire for key-b got active=%d limit=%d ok=%v, want 1,1,true", active, limit, ok)
	}

	releaseA()

	releaseA, active, limit, ok = limiter.acquire(7, "key-a", 1)
	if !ok || active != 1 || limit != 1 {
		t.Fatalf("key-a after release got active=%d limit=%d ok=%v, want 1,1,true", active, limit, ok)
	}
	releaseA()
	releaseB()
}

func TestDoUpstreamRequestHoldsPerKeyConcurrencyUntilBodyClosed(t *testing.T) {
	t.Parallel()

	unblock := make(chan struct{})
	upstream := newTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-unblock
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	s := &Server{
		client:                    newTestHTTPClient(),
		channelConcurrencyLimiter: newChannelConcurrencyLimiter(),
	}
	cfg := &model.Config{ID: 42, MaxConcurrency: 1}

	firstReq, err := http.NewRequest(http.MethodGet, upstream.URL, nil)
	if err != nil {
		t.Fatalf("new first request: %v", err)
	}
	firstResp, err := s.doUpstreamRequest(cfg, "key-a", firstReq)
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}

	secondReq, err := http.NewRequest(http.MethodGet, upstream.URL, nil)
	if err != nil {
		t.Fatalf("new second request: %v", err)
	}
	secondResp, err := s.doUpstreamRequest(cfg, "key-a", secondReq)
	if secondResp != nil {
		_ = secondResp.Body.Close()
	}
	if !errors.Is(err, ErrKeyConcurrencyExceeded) {
		t.Fatalf("second request error=%v, want ErrKeyConcurrencyExceeded", err)
	}

	otherKeyReq, err := http.NewRequest(http.MethodGet, upstream.URL, nil)
	if err != nil {
		t.Fatalf("new other-key request: %v", err)
	}
	otherKeyResp, err := s.doUpstreamRequest(cfg, "key-b", otherKeyReq)
	if err != nil {
		t.Fatalf("other key should have an independent slot: %v", err)
	}

	close(unblock)
	if _, err := io.ReadAll(firstResp.Body); err != nil {
		t.Fatalf("read first response: %v", err)
	}
	if err := firstResp.Body.Close(); err != nil {
		t.Fatalf("close first response: %v", err)
	}
	if _, err := io.ReadAll(otherKeyResp.Body); err != nil {
		t.Fatalf("read other-key response: %v", err)
	}
	if err := otherKeyResp.Body.Close(); err != nil {
		t.Fatalf("close other-key response: %v", err)
	}

	thirdReq, err := http.NewRequest(http.MethodGet, upstream.URL, nil)
	if err != nil {
		t.Fatalf("new third request: %v", err)
	}
	thirdResp, err := s.doUpstreamRequest(cfg, "key-a", thirdReq)
	if err != nil {
		t.Fatalf("third request after release failed: %v", err)
	}
	_ = thirdResp.Body.Close()
}

func TestKeyConcurrencySkipReasonIncludesScanCounts(t *testing.T) {
	t.Parallel()

	err := &keyConcurrencyExhaustedError{
		cause:               &keyConcurrencyExceededError{active: 2, limit: 2},
		checkedKeys:         3,
		totalKeys:           4,
		concurrencyLimited:  3,
		upstreamAttempts:    1,
		maxUpstreamAttempts: 2,
		perKeyLimit:         2,
	}

	if !errors.Is(err, ErrKeyConcurrencyExceeded) {
		t.Fatalf("error should wrap ErrKeyConcurrencyExceeded: %v", err)
	}

	const want = "Key 检查=3/4，并发满载=3，上游尝试=1/2，单 Key 上限=2"
	if got := err.Error(); got != want {
		t.Fatalf("error=%q, want %q", got, want)
	}
}

func TestTryChannelWithKeysReportsConcurrencyScanCounts(t *testing.T) {
	t.Parallel()

	upstream := newTestHTTPServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("upstream should not be called when every key is concurrency limited")
	}))
	defer upstream.Close()

	env := setupProxyTestEnv(t, []testChannel{
		{name: "limited", models: "gpt-4", apiKey: "sk-first"},
	}, map[int]string{0: upstream.URL})

	ctx := context.Background()
	cfgs, err := env.store.ListConfigs(ctx)
	if err != nil {
		t.Fatalf("ListConfigs failed: %v", err)
	}
	if len(cfgs) != 1 {
		t.Fatalf("configs=%d, want 1", len(cfgs))
	}
	cfg := cfgs[0]
	cfg.MaxConcurrency = 1
	if _, err := env.store.UpdateConfig(ctx, cfg.ID, cfg); err != nil {
		t.Fatalf("UpdateConfig failed: %v", err)
	}
	if err := env.store.CreateAPIKeysBatch(ctx, []*model.APIKey{
		{ChannelID: cfg.ID, KeyIndex: 1, APIKey: "sk-second"},
	}); err != nil {
		t.Fatalf("CreateAPIKeysBatch failed: %v", err)
	}
	env.server.InvalidateAPIKeysCache(cfg.ID)
	env.server.maxKeyRetries = 1

	for _, apiKey := range []string{"sk-first", "sk-second"} {
		release, _, _, ok := env.server.channelConcurrencyLimiter.acquire(cfg.ID, apiKey, 1)
		if !ok {
			t.Fatalf("pre-acquire key %q failed", apiKey)
		}
		defer release()
	}

	body := []byte(`{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}`)
	result, err := env.server.tryChannelWithKeys(ctx, cfg, &proxyRequestContext{
		originalModel:  "gpt-4",
		clientProtocol: protocol.OpenAI,
		requestMethod:  http.MethodPost,
		requestPath:    "/v1/chat/completions",
		body:           body,
		header:         make(http.Header),
	}, newRecorder())
	if result != nil {
		t.Fatalf("result=%+v, want nil", result)
	}
	if !errors.Is(err, ErrKeyConcurrencyExceeded) {
		t.Fatalf("error=%v, want ErrKeyConcurrencyExceeded", err)
	}

	var exhaustedErr *keyConcurrencyExhaustedError
	if !errors.As(err, &exhaustedErr) {
		t.Fatalf("error type=%T, want *keyConcurrencyExhaustedError", err)
	}
	if exhaustedErr.checkedKeys != 2 || exhaustedErr.totalKeys != 2 {
		t.Fatalf("Key check=%d/%d, want 2/2", exhaustedErr.checkedKeys, exhaustedErr.totalKeys)
	}
	if exhaustedErr.concurrencyLimited != 2 {
		t.Fatalf("concurrency limited=%d, want 2", exhaustedErr.concurrencyLimited)
	}
	if exhaustedErr.upstreamAttempts != 0 || exhaustedErr.maxUpstreamAttempts != 1 {
		t.Fatalf("upstream attempts=%d/%d, want 0/1", exhaustedErr.upstreamAttempts, exhaustedErr.maxUpstreamAttempts)
	}
	if exhaustedErr.perKeyLimit != 1 {
		t.Fatalf("per-key limit=%d, want 1", exhaustedErr.perKeyLimit)
	}
}
