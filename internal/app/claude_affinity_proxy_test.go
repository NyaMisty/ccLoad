package app

import (
	"context"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"ccLoad/internal/model"
)

const claudeAffinityTestResponse = `{"id":"msg_affinity","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"model":"claude-sonnet-4-6","stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`

type claudeAffinityAttempt struct {
	channel string
	apiKey  string
}

type claudeAffinityAttemptLog struct {
	mu       sync.Mutex
	attempts []claudeAffinityAttempt
}

func (log *claudeAffinityAttemptLog) add(channel, apiKey string) {
	log.mu.Lock()
	log.attempts = append(log.attempts, claudeAffinityAttempt{
		channel: channel,
		apiKey:  apiKey,
	})
	log.mu.Unlock()
}

func (log *claudeAffinityAttemptLog) reset() {
	log.mu.Lock()
	log.attempts = nil
	log.mu.Unlock()
}

func (log *claudeAffinityAttemptLog) snapshot() []claudeAffinityAttempt {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]claudeAffinityAttempt(nil), log.attempts...)
}

func TestProxy_NativeAnthropicSoftAffinityAcrossKeysAndChannels(t *testing.T) {
	const (
		primaryKeyA = "sk-claude-affinity-a"
		primaryKeyB = "sk-claude-affinity-b"
		fallbackKey = "sk-claude-affinity-fallback"
		sessionA    = "11111111-2222-4333-8444-555555555555"
		sessionB    = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	)

	var failureMode atomic.Int32
	attemptLog := &claudeAffinityAttemptLog{}
	var preferredMu sync.RWMutex
	preferredKey := ""

	primary := newTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := headerValueFold(r.Header, "x-api-key")
		attemptLog.add("primary", apiKey)

		preferredMu.RLock()
		currentPreferred := preferredKey
		preferredMu.RUnlock()
		mode := failureMode.Load()
		if mode == 1 && apiKey == currentPreferred {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, `{"error":{"type":"api_error","message":"provider unavailable"}}`)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, claudeAffinityTestResponse)
	}))
	t.Cleanup(primary.Close)

	fallback := newTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptLog.add("fallback", headerValueFold(r.Header, "x-api-key"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, claudeAffinityTestResponse)
	}))
	t.Cleanup(fallback.Close)

	env := setupProxyTestEnv(t, []testChannel{
		{
			name: "claude-affinity-primary", upstreamProtocol: "anthropic",
			protocolTransformMode: model.ProtocolTransformModeUpstream,
			models:                "claude-sonnet-4-6", apiKey: primaryKeyA, priority: 100,
			retryOtherKeysOnFailure: true,
		},
		{
			name: "claude-affinity-fallback", upstreamProtocol: "anthropic",
			protocolTransformMode: model.ProtocolTransformModeUpstream,
			models:                "claude-sonnet-4-6", apiKey: fallbackKey, priority: 50,
		},
	}, map[int]string{0: primary.URL, 1: fallback.URL})

	configs, err := env.store.ListConfigs(context.Background())
	if err != nil || len(configs) != 2 {
		t.Fatalf("ListConfigs: configs=%d err=%v", len(configs), err)
	}
	var primaryCfg, fallbackCfg *model.Config
	for _, cfg := range configs {
		switch cfg.Name {
		case "claude-affinity-primary":
			primaryCfg = cfg
		case "claude-affinity-fallback":
			fallbackCfg = cfg
		}
	}
	if primaryCfg == nil || fallbackCfg == nil {
		t.Fatalf("missing affinity test channels: %+v", configs)
	}
	if err := env.store.CreateAPIKeysBatch(context.Background(), []*model.APIKey{{
		ChannelID: primaryCfg.ID, KeyIndex: 1, APIKey: primaryKeyB,
		KeyStrategy: model.KeyStrategyRoundRobin,
	}}); err != nil {
		t.Fatalf("create second primary Key: %v", err)
	}
	if err := env.store.UpdateAPIKeysStrategy(
		context.Background(), primaryCfg.ID, model.KeyStrategyRoundRobin,
	); err != nil {
		t.Fatalf("enable primary Key round robin: %v", err)
	}
	env.server.InvalidateAPIKeysCache(primaryCfg.ID)

	send := func(sessionID string) {
		t.Helper()
		response := doProxyRequest(t, env.engine, "/v1/messages", map[string]any{
			"model":      "claude-sonnet-4-6",
			"max_tokens": 64,
			"messages": []any{
				map[string]any{"role": "user", "content": "hello"},
			},
		}, map[string]string{
			"Anthropic-Version":        "2023-06-01",
			"X-Claude-Code-Session-Id": sessionID,
		})
		if response.Code != http.StatusOK {
			t.Fatalf("session %s status=%d body=%s", sessionID, response.Code, response.Body.String())
		}
	}
	assertAttempts := func(want ...claudeAffinityAttempt) {
		t.Helper()
		got := attemptLog.snapshot()
		if len(got) != len(want) {
			t.Fatalf("attempts=%+v, want %+v", got, want)
		}
		for index := range want {
			if got[index] != want[index] {
				t.Fatalf("attempts=%+v, want %+v", got, want)
			}
		}
	}

	send(sessionA)
	first := attemptLog.snapshot()
	if len(first) != 1 || first[0].channel != "primary" {
		t.Fatalf("initial attempts=%+v, want one primary attempt", first)
	}
	preferredMu.Lock()
	preferredKey = first[0].apiKey
	preferredMu.Unlock()
	otherKey := primaryKeyA
	if preferredKey == primaryKeyA {
		otherKey = primaryKeyB
	}
	originalKey := preferredKey
	keys, err := env.store.GetAPIKeys(context.Background(), primaryCfg.ID)
	if err != nil {
		t.Fatalf("get primary Keys: %v", err)
	}
	keyIndexes := make(map[string]int, len(keys))
	for _, key := range keys {
		keyIndexes[key.APIKey] = key.KeyIndex
	}

	attemptLog.reset()
	send(sessionA)
	assertAttempts(claudeAffinityAttempt{channel: "primary", apiKey: preferredKey})

	attemptLog.reset()
	send(sessionB)
	assertAttempts(claudeAffinityAttempt{channel: "primary", apiKey: otherKey})

	// Disabling the bound Key makes normal routing select the other Key. Once
	// that Key succeeds, re-enabling the old Key must not pull the session back.
	if err := env.store.SetAPIKeyDisabled(
		context.Background(), primaryCfg.ID, keyIndexes[originalKey], true,
	); err != nil {
		t.Fatalf("disable original affinity Key: %v", err)
	}
	env.server.InvalidateAPIKeysCache(primaryCfg.ID)

	attemptLog.reset()
	send(sessionA)
	assertAttempts(claudeAffinityAttempt{channel: "primary", apiKey: otherKey})

	if err := env.store.SetAPIKeyDisabled(
		context.Background(), primaryCfg.ID, keyIndexes[originalKey], false,
	); err != nil {
		t.Fatalf("re-enable original affinity Key: %v", err)
	}
	env.server.InvalidateAPIKeysCache(primaryCfg.ID)

	attemptLog.reset()
	send(sessionA)
	assertAttempts(claudeAffinityAttempt{channel: "primary", apiKey: otherKey})
	preferredMu.Lock()
	preferredKey = otherKey
	preferredMu.Unlock()

	// Make the fallback channel globally preferable. The established session
	// must still try its newly bound healthy channel and Key first.
	fallbackCfg.Priority = 200
	if _, err := env.store.UpdateConfig(context.Background(), fallbackCfg.ID, fallbackCfg); err != nil {
		t.Fatalf("raise fallback priority: %v", err)
	}
	env.server.InvalidateChannelListCache()

	attemptLog.reset()
	send(sessionA)
	assertAttempts(claudeAffinityAttempt{channel: "primary", apiKey: preferredKey})

	// A failed preferred Key resumes the untouched channel order. The first
	// fallback that succeeds becomes the session's new affinity target.
	failureMode.Store(1)
	attemptLog.reset()
	send(sessionA)
	assertAttempts(
		claudeAffinityAttempt{channel: "primary", apiKey: preferredKey},
		claudeAffinityAttempt{channel: "fallback", apiKey: fallbackKey},
	)

	if err := env.store.ResetKeyCooldown(
		context.Background(), primaryCfg.ID, keyIndexes[preferredKey],
	); err != nil {
		t.Fatalf("recover preferred Key: %v", err)
	}
	env.server.invalidateChannelRelatedCache(primaryCfg.ID)
	failureMode.Store(0)

	attemptLog.reset()
	send(sessionA)
	assertAttempts(claudeAffinityAttempt{channel: "fallback", apiKey: fallbackKey})
}

func TestProxy_TranslatedAnthropicDoesNotCreateSoftAffinity(t *testing.T) {
	const sessionID = "89abcdef-0123-4567-89ab-cdef01234567"

	upstream := newTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"id":"chatcmpl-affinity","object":"chat.completion","model":"claude-sonnet-4-6",
			"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}
		}`)
	}))
	t.Cleanup(upstream.Close)

	env := setupProxyTestEnv(t, []testChannel{{
		name: "translated-anthropic-no-affinity", upstreamProtocol: "openai",
		protocolTransformMode: model.ProtocolTransformModeLocal,
		models:                "claude-sonnet-4-6", priority: 100,
	}}, map[int]string{0: upstream.URL})
	response := doProxyRequest(t, env.engine, "/v1/messages", map[string]any{
		"model":      "claude-sonnet-4-6",
		"max_tokens": 64,
		"messages": []any{
			map[string]any{"role": "user", "content": "hello"},
		},
	}, map[string]string{
		"Anthropic-Version":        "2023-06-01",
		"X-Claude-Code-Session-Id": sessionID,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}

	affinity := newClaudeSessionAffinity(
		env.store,
		model.HashToken("test-api-key"),
		http.Header{"X-Claude-Code-Session-Id": {sessionID}},
		nil,
	)
	if affinity == nil {
		t.Fatal("valid Claude session did not produce an affinity identity")
	}
	if err := affinity.load(context.Background()); err != nil {
		t.Fatalf("load affinity: %v", err)
	}
	if target, ok := affinity.targetSnapshot(); ok {
		t.Fatalf("translated OpenAI success created Claude affinity: %+v", target)
	}
}
