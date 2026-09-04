package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"ccLoad/internal/model"
	"ccLoad/internal/zaiauth"
)

type zaiProxyAttempt struct {
	channel       string
	apiKey        string
	deviceID      string
	bodySession   string
	headerSession string
	requestID     string
	traceID       string
	headers       http.Header
	body          []byte
}

type zaiProxyAttemptLog struct {
	mu       sync.Mutex
	attempts []zaiProxyAttempt
}

func (log *zaiProxyAttemptLog) capture(channel string, request *http.Request) {
	body, _ := io.ReadAll(request.Body)
	var identity zaiRequestIdentity
	var payload map[string]any
	if json.Unmarshal(body, &payload) == nil {
		metadata, _ := payload["metadata"].(map[string]any)
		rawIdentity, _ := metadata["user_id"].(string)
		_ = json.Unmarshal([]byte(rawIdentity), &identity)
	}
	log.mu.Lock()
	log.attempts = append(log.attempts, zaiProxyAttempt{
		channel:       channel,
		apiKey:        anthropicHeaderValue(request.Header, "x-api-key"),
		deviceID:      identity.DeviceID,
		bodySession:   identity.SessionID,
		headerSession: anthropicHeaderValue(request.Header, "x-session-id"),
		requestID:     anthropicHeaderValue(request.Header, "x-request-id"),
		traceID:       anthropicHeaderValue(request.Header, "x-zcode-trace-id"),
		headers:       request.Header.Clone(),
		body:          append([]byte(nil), body...),
	})
	log.mu.Unlock()
}

func (log *zaiProxyAttemptLog) snapshot() []zaiProxyAttempt {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]zaiProxyAttempt(nil), log.attempts...)
}

// End-to-end proxy coverage for Z.ai Coding Plan channels: a downstream
// Anthropic request must reach the upstream carrying ZCode's identity, ZCode's
// authentication header and ZCode's device fingerprint.
func TestProxy_ZAICodingPlanReplicatesZCodeWire(t *testing.T) {
	t.Parallel()

	var upstreamHeaders http.Header
	var upstreamBody []byte
	upstream := newTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHeaders = r.Header.Clone()
		upstreamBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"msg_1","type":"message","role":"assistant","model":"glm-4.7","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2}}`)
	}))
	defer upstream.Close()

	credential := &zaiauth.Credential{APIKey: "key-id.secret", Email: "user@example.com", UserID: "u-1"}
	credentialJSON, err := credential.JSON()
	if err != nil {
		t.Fatalf("encode z.ai credential: %v", err)
	}
	env := setupProxyTestEnv(t, []testChannel{{
		name: "zai-coding-plan", upstreamProtocol: "anthropic", models: "glm-4.7",
		authType: model.AuthTypeZAIOAuth, oauthCredential: credentialJSON,
	}}, map[int]string{0: upstream.URL})

	response := doProxyRequest(t, env.engine, "/v1/messages", map[string]any{
		"model":      "glm-4.7",
		"max_tokens": 16,
		"messages":   []any{map[string]any{"role": "user", "content": "hello"}},
	}, map[string]string{"anthropic-version": "2023-06-01"})
	if response.Code != http.StatusOK {
		t.Fatalf("proxy status = %d body = %s", response.Code, response.Body.String())
	}

	if got := anthropicHeaderValue(upstreamHeaders, "x-api-key"); got != "key-id.secret" {
		t.Fatalf("upstream x-api-key = %q", got)
	}
	if got := anthropicHeaderValue(upstreamHeaders, "Authorization"); got != "" {
		t.Fatalf("upstream Authorization = %q, ZCode sends x-api-key only", got)
	}
	for _, entry := range zaiauth.SourceHeaders() {
		if got := anthropicHeaderValue(upstreamHeaders, entry[0]); got != entry[1] {
			t.Fatalf("upstream %s = %q, want %q", entry[0], got, entry[1])
		}
	}

	var request map[string]any
	if err := json.Unmarshal(upstreamBody, &request); err != nil {
		t.Fatalf("decode upstream body: %v", err)
	}
	metadata, _ := request["metadata"].(map[string]any)
	rawIdentity, _ := metadata["user_id"].(string)
	var identity zaiRequestIdentity
	if err := json.Unmarshal([]byte(rawIdentity), &identity); err != nil {
		t.Fatalf("decode metadata.user_id %q: %v", rawIdentity, err)
	}
	if identity.DeviceID != credential.DeviceID || identity.DeviceID == "" {
		t.Fatalf("device id = %q, want %q", identity.DeviceID, credential.DeviceID)
	}
	if identity.SessionID == "" || identity.AccountUUID != "" {
		t.Fatalf("identity = %+v", identity)
	}
	if !strings.HasPrefix(rawIdentity, `{"device_id":`) {
		t.Fatalf("metadata.user_id shape = %s", rawIdentity)
	}
}

// A Coding Plan channel keeps serving OpenAI-shaped clients: ccLoad translates
// them locally to the Anthropic wire the ZCode endpoint expects.
func TestProxy_ZAICodingPlanTranslatesOpenAIClients(t *testing.T) {
	t.Parallel()

	var upstreamPath string
	var upstreamBody []byte
	upstream := newTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamPath = r.URL.Path
		upstreamBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"msg_1","type":"message","role":"assistant","model":"glm-4.7","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2}}`)
	}))
	defer upstream.Close()

	credentialJSON, err := (&zaiauth.Credential{APIKey: "key-id.secret"}).JSON()
	if err != nil {
		t.Fatalf("encode z.ai credential: %v", err)
	}
	env := setupProxyTestEnv(t, []testChannel{{
		name: "zai-openai-client", upstreamProtocol: "anthropic", models: "glm-4.7",
		authType: model.AuthTypeZAIOAuth, oauthCredential: credentialJSON,
	}}, map[int]string{0: upstream.URL})

	response := doProxyRequest(t, env.engine, "/v1/chat/completions", map[string]any{
		"model":    "glm-4.7",
		"messages": []any{map[string]any{"role": "user", "content": "hello"}},
	}, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("proxy status = %d body = %s", response.Code, response.Body.String())
	}
	if !strings.HasSuffix(upstreamPath, "/v1/messages") {
		t.Fatalf("upstream path = %q", upstreamPath)
	}
	var request map[string]any
	if err := json.Unmarshal(upstreamBody, &request); err != nil {
		t.Fatalf("decode upstream body: %v", err)
	}
	metadata, _ := request["metadata"].(map[string]any)
	if _, ok := metadata["user_id"].(string); !ok {
		t.Fatalf("translated request is missing the ZCode fingerprint: %s", upstreamBody)
	}
}

func TestProxy_ZAICodingPlanSoftAffinityFollowsActualKey(t *testing.T) {
	t.Parallel()

	const (
		modelName    = "glm-zai-affinity"
		keyA         = "zai-88.secret-a"
		keyB         = "zai-89.secret-b"
		deviceA      = "11111111-2222-4333-8444-555555555555"
		deviceB      = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
		sessionID    = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"
		otherSession = "9f2504e0-4f89-41d3-9a0c-0305e82c3302"
	)
	attemptLog := &zaiProxyAttemptLog{}
	responseBody := `{"id":"msg_zai_affinity","type":"message","role":"assistant","model":"` +
		modelName + `","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn",` +
		`"usage":{"input_tokens":3,"output_tokens":2}}`
	newUpstream := func(channel string) *testHTTPServer {
		t.Helper()
		return newTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attemptLog.capture(channel, r)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, responseBody)
		}))
	}
	upstreamA := newUpstream("a")
	t.Cleanup(upstreamA.Close)
	upstreamB := newUpstream("b")
	t.Cleanup(upstreamB.Close)

	credentialJSON := func(apiKey, deviceID, userID string) string {
		t.Helper()
		payload, err := (&zaiauth.Credential{
			APIKey: apiKey, DeviceID: deviceID, UserID: userID,
		}).JSON()
		if err != nil {
			t.Fatalf("encode ZCode credential: %v", err)
		}
		return payload
	}
	credentialA := credentialJSON(keyA, deviceA, "account-a")
	credentialB := credentialJSON(keyB, deviceB, "account-b")
	env := setupProxyTestEnv(t, []testChannel{
		{
			name: "zai-affinity-a", upstreamProtocol: "anthropic", models: modelName,
			authType: model.AuthTypeZAIOAuth, oauthCredential: credentialA, priority: 200,
		},
		{
			name: "zai-affinity-b", upstreamProtocol: "anthropic", models: modelName,
			authType: model.AuthTypeZAIOAuth, oauthCredential: credentialB, priority: 100,
		},
	}, map[int]string{0: upstreamA.URL, 1: upstreamB.URL})

	configs, err := env.store.ListConfigs(context.Background())
	if err != nil || len(configs) != 2 {
		t.Fatalf("list ZCode channels: configs=%d err=%v", len(configs), err)
	}
	var cfgA, cfgB *model.Config
	for _, cfg := range configs {
		switch cfg.Name {
		case "zai-affinity-a":
			cfgA = cfg
		case "zai-affinity-b":
			cfgB = cfg
		}
	}
	if cfgA == nil || cfgB == nil {
		t.Fatalf("missing ZCode channels: %+v", configs)
	}

	send := func(sourceSession string) zaiProxyAttempt {
		t.Helper()
		before := len(attemptLog.snapshot())
		foreignIdentity, marshalErr := json.Marshal(zaiRequestIdentity{
			DeviceID: "foreign-device", SessionID: sourceSession,
		})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		response := doProxyRequest(t, env.engine, "/v1/messages", map[string]any{
			"model":      modelName,
			"max_tokens": 32,
			"metadata":   map[string]any{"user_id": string(foreignIdentity)},
			"system": []any{map[string]any{
				"type": "text",
				"text": "x-anthropic-billing-header: cc_version=9.9.9.abc; cc_entrypoint=cli; cch=abcde;",
			}},
			"messages": []any{map[string]any{"role": "user", "content": "hello"}},
		}, map[string]string{
			"Anthropic-Version":        "2023-06-01",
			"Anthropic-Beta":           "claude-code-20250219",
			"X-Claude-Code-Session-Id": sourceSession,
		})
		if response.Code != http.StatusOK {
			t.Fatalf("session %s status=%d body=%s", sourceSession, response.Code, response.Body.String())
		}
		attempts := attemptLog.snapshot()
		if len(attempts) != before+1 {
			t.Fatalf("session %s attempts=%+v, want one new attempt", sourceSession, attempts[before:])
		}
		attempt := attempts[before]
		if attempt.headerSession != attempt.bodySession {
			t.Fatalf("ZCode body/header session mismatch: %+v", attempt)
		}
		if attempt.bodySession != zaiUpstreamSessionID(attempt.apiKey, sourceSession) {
			t.Fatalf("wire session=%q key=%q source=%q", attempt.bodySession, attempt.apiKey, sourceSession)
		}
		if attempt.requestID == "" || attempt.traceID == "" {
			t.Fatalf("ZCode request identifiers missing: %+v", attempt)
		}
		if anthropicHeaderValue(attempt.headers, "Authorization") != "" ||
			anthropicHeaderValue(attempt.headers, "Anthropic-Beta") != "" ||
			anthropicHeaderValue(attempt.headers, "X-Claude-Code-Session-Id") != "" {
			t.Fatalf("Claude identity leaked into ZCode headers: %v", attempt.headers)
		}
		if strings.Contains(string(attempt.body), "x-anthropic-billing-header:") ||
			strings.Contains(string(attempt.body), "cch=") {
			t.Fatalf("Claude billing/CCH leaked into ZCode body: %s", attempt.body)
		}
		return attempt
	}

	first := send(sessionID)
	if first.channel != "a" || first.apiKey != keyA || first.deviceID != deviceA {
		t.Fatalf("initial target=%+v", first)
	}

	cfgB.Priority = 300
	if _, err = env.store.UpdateConfig(context.Background(), cfgB.ID, cfgB); err != nil {
		t.Fatalf("raise channel B priority: %v", err)
	}
	env.server.InvalidateChannelListCache()
	sticky := send(sessionID)
	if sticky.channel != "a" || sticky.apiKey != keyA || sticky.bodySession != first.bodySession {
		t.Fatalf("established ZCode affinity moved: first=%+v next=%+v", first, sticky)
	}

	if _, err = env.store.UpdateChannelEnabled(context.Background(), cfgA.ID, false); err != nil {
		t.Fatalf("disable channel A: %v", err)
	}
	env.server.InvalidateChannelListCache()
	fallback := send(sessionID)
	if fallback.channel != "b" || fallback.apiKey != keyB || fallback.deviceID != deviceB ||
		fallback.bodySession == first.bodySession {
		t.Fatalf("ZCode fallback did not migrate by key: first=%+v fallback=%+v", first, fallback)
	}

	if _, err = env.store.UpdateChannelEnabled(context.Background(), cfgA.ID, true); err != nil {
		t.Fatalf("re-enable channel A: %v", err)
	}
	cfgA.Enabled = true
	cfgA.Priority = 400
	if _, err = env.store.UpdateConfig(context.Background(), cfgA.ID, cfgA); err != nil {
		t.Fatalf("restore channel A priority: %v", err)
	}
	env.server.InvalidateChannelListCache()
	migrated := send(sessionID)
	if migrated.channel != "b" || migrated.apiKey != keyB ||
		migrated.bodySession != fallback.bodySession {
		t.Fatalf("latest successful key was not retained: fallback=%+v next=%+v", fallback, migrated)
	}

	normal := send(otherSession)
	if normal.channel != "a" || normal.apiKey != keyA || normal.deviceID != deviceA {
		t.Fatalf("new session did not follow normal routing: %+v", normal)
	}

	// Move both complete credentials between channels. The affinity row still
	// points at channel B, but key B now lives on lower-priority channel A.
	currentA, err := env.store.GetConfig(context.Background(), cfgA.ID)
	if err != nil {
		t.Fatalf("reload channel A before moving credentials: %v", err)
	}
	currentB, err := env.store.GetConfig(context.Background(), cfgB.ID)
	if err != nil {
		t.Fatalf("reload channel B before moving credentials: %v", err)
	}
	swapped, err := env.store.CompareAndSwapOAuthCredential(
		context.Background(),
		cfgA.ID,
		model.AuthTypeZAIOAuth,
		currentA.OAuthCredential,
		credentialB,
	)
	if err != nil || !swapped {
		t.Fatalf("move key B to channel A: swapped=%t err=%v", swapped, err)
	}
	swapped, err = env.store.CompareAndSwapOAuthCredential(
		context.Background(),
		cfgB.ID,
		model.AuthTypeZAIOAuth,
		currentB.OAuthCredential,
		credentialA,
	)
	if err != nil || !swapped {
		t.Fatalf("move key A to channel B: swapped=%t err=%v", swapped, err)
	}
	cfgA.OAuthCredential = credentialB
	cfgA.Priority = 50
	cfgB.OAuthCredential = credentialA
	cfgB.Priority = 500
	if _, err = env.store.UpdateConfig(context.Background(), cfgA.ID, cfgA); err != nil {
		t.Fatalf("lower channel A priority: %v", err)
	}
	if _, err = env.store.UpdateConfig(context.Background(), cfgB.ID, cfgB); err != nil {
		t.Fatalf("raise channel B priority: %v", err)
	}
	env.server.zaiCredentials.invalidate(cfgA.ID)
	env.server.zaiCredentials.invalidate(cfgB.ID)
	env.server.InvalidateChannelListCache()
	moved := send(sessionID)
	if moved.channel != "a" || moved.apiKey != keyB || moved.deviceID != deviceB {
		t.Fatalf("affinity followed channel hint instead of actual key: %+v", moved)
	}
}

func TestProxy_ZAIRejectedKeyRederivesSessionAffinityAndPersistsEveryAttempt(t *testing.T) {
	t.Parallel()

	const (
		modelName       = "glm-zai-refresh"
		oldKey          = "zai-old.secret"
		newKey          = "zai-new.secret"
		accessToken     = "zai-account-access"
		deviceID        = "11111111-2222-4333-8444-555555555555"
		sourceSessionID = "3f2504e0-4f89-41d3-9a0c-0305e82c3301"
	)
	attemptLog := &zaiProxyAttemptLog{}
	upstream := newTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptLog.capture("refresh", r)
		w.Header().Set("Content-Type", "application/json")
		if anthropicHeaderValue(r.Header, "x-api-key") == oldKey {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"type":"authentication_error","message":"rejected"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"id":"msg_zai_refresh","type":"message","role":"assistant",`+
			`"model":"`+modelName+`","content":[{"type":"text","text":"ok"}],`+
			`"stop_reason":"end_turn","usage":{"input_tokens":3,"output_tokens":2}}`)
	}))
	t.Cleanup(upstream.Close)

	rawCredential, err := (&zaiauth.Credential{
		APIKey: oldKey, AccessToken: accessToken, DeviceID: deviceID,
		UserID: "user-1", Email: "user@example.com",
	}).JSON()
	if err != nil {
		t.Fatal(err)
	}
	env := setupProxyTestEnv(t, []testChannel{{
		name: "zai-refresh", upstreamProtocol: "anthropic", models: modelName,
		authType: model.AuthTypeZAIOAuth, oauthCredential: rawCredential,
	}}, map[int]string{0: upstream.URL})

	refreshClient := &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		var payload string
		switch {
		case req.URL.Path == "/api/auth/z/login":
			payload = `{"code":0,"data":{"access_token":"business-token"}}`
		case req.URL.Path == "/api/biz/customer/getCustomerInfo":
			payload = `{"code":0,"data":{"userId":"user-1","email":"user@example.com",` +
				`"organizations":[{"organizationId":"org-1","organizationName":"默认机构",` +
				`"projects":[{"projectId":"project-1","projectName":"默认项目"}]}]}}`
		case req.URL.Path == "/api/biz/v1/organization/org-1/projects/project-1/api_keys" &&
			req.Method == http.MethodGet:
			payload = `{"code":0,"data":[{"name":"zcode-api-key","apiKey":"zai-new"}]}`
		case strings.HasSuffix(req.URL.Path, "/api_keys/copy/zai-new"):
			payload = `{"code":0,"data":{"secretKey":"secret"}}`
		default:
			t.Fatalf("unexpected ZCode key re-resolution request: %s %s", req.Method, req.URL)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(payload)),
			Request:    req,
		}, nil
	})}
	env.server.zaiCredentials = newZAICredentialManager(
		env.store,
		func(*model.Config) *http.Client { return refreshClient },
		func(int64) { env.server.InvalidateChannelListCache() },
	)

	response := doProxyRequest(t, env.engine, "/v1/messages", map[string]any{
		"model":      modelName,
		"max_tokens": 32,
		"messages":   []any{map[string]any{"role": "user", "content": "hello"}},
	}, map[string]string{
		"Anthropic-Version":        "2023-06-01",
		"X-Claude-Code-Session-Id": sourceSessionID,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	attempts := attemptLog.snapshot()
	if len(attempts) != 2 || attempts[0].apiKey != oldKey || attempts[1].apiKey != newKey {
		t.Fatalf("ZCode key attempts=%+v", attempts)
	}
	for _, attempt := range attempts {
		wantSession := zaiUpstreamSessionID(attempt.apiKey, sourceSessionID)
		if attempt.bodySession != wantSession || attempt.headerSession != wantSession ||
			attempt.deviceID != deviceID {
			t.Fatalf("attempt identity=%+v, want session=%q device=%q", attempt, wantSession, deviceID)
		}
	}
	if attempts[0].bodySession == attempts[1].bodySession {
		t.Fatal("old and replacement Coding Plan keys shared one upstream session")
	}
	if attempts[0].requestID == attempts[1].requestID || attempts[0].traceID == attempts[1].traceID {
		t.Fatalf("request/trace IDs were reused across retries: %+v", attempts)
	}

	configs, err := env.store.ListConfigs(context.Background())
	if err != nil || len(configs) != 1 {
		t.Fatalf("list refreshed ZCode channel: configs=%d err=%v", len(configs), err)
	}
	persistedCredential, err := zaiauth.ParseCredential([]byte(configs[0].OAuthCredential))
	if err != nil || persistedCredential.APIKey != newKey || persistedCredential.DeviceID != deviceID {
		t.Fatalf("persisted replacement credential=%+v err=%v", persistedCredential, err)
	}
	affinity := newClaudeSessionAffinity(
		env.store,
		model.HashToken("test-api-key"),
		http.Header{"X-Claude-Code-Session-Id": {sourceSessionID}},
		nil,
	)
	if affinity == nil {
		t.Fatal("missing Claude affinity identity")
	}
	if err = affinity.load(context.Background()); err != nil {
		t.Fatalf("load replacement affinity: %v", err)
	}
	target, ok := affinity.targetSnapshot()
	if !ok || target.targetKind != model.ClaudeAffinityTargetZAICodingPlan ||
		target.apiKeyHash != claudeAffinityAPIKeyHash(newKey) {
		t.Fatalf("replacement affinity=%+v found=%t", target, ok)
	}

	deadline := time.Now().Add(2 * time.Second)
	var logs []*model.LogEntry
	for time.Now().Before(deadline) {
		logs, err = env.store.ListLogs(
			context.Background(),
			time.Now().Add(-time.Minute),
			10,
			0,
			&model.LogFilter{LogSource: model.LogSourceProxy},
		)
		if err != nil {
			t.Fatalf("list ZCode attempt logs: %v", err)
		}
		var matches int
		for _, entry := range logs {
			if entry.Model == modelName {
				matches++
			}
		}
		if matches >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	loggedKeys := make(map[string]bool, 2)
	loggedRequests := 0
	for _, entry := range logs {
		if entry.Model != modelName {
			continue
		}
		requests, requestErr := env.store.GetLogUpstreamRequests(context.Background(), entry.ID)
		if requestErr != nil {
			t.Fatalf("get ZCode upstream request log: %v", requestErr)
		}
		for _, request := range requests {
			var headers http.Header
			if unmarshalErr := json.Unmarshal([]byte(request.Headers), &headers); unmarshalErr != nil {
				t.Fatalf("decode persisted ZCode headers: %v", unmarshalErr)
			}
			key := anthropicHeaderValue(headers, "x-api-key")
			if key != oldKey && key != newKey {
				t.Fatalf("persisted ZCode key=%q headers=%s", key, request.Headers)
			}
			loggedKeys[key] = true
			loggedRequests++
			identity := decodeZAIRequestIdentity(t, request.Body)
			if identity.DeviceID != deviceID ||
				identity.SessionID != zaiUpstreamSessionID(key, sourceSessionID) {
				t.Fatalf("persisted ZCode identity=%+v key=%q", identity, key)
			}
		}
	}
	if loggedRequests != 2 || !loggedKeys[oldKey] || !loggedKeys[newKey] {
		t.Fatalf("persisted attempts=%d keys=%v logs=%+v", loggedRequests, loggedKeys, logs)
	}
}
