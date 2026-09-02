package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"ccLoad/internal/model"

	"github.com/tidwall/gjson"
)

func TestCaptureRequestLogEntryPreservesRawRequest(t *testing.T) {
	const target = "https://example.com/v1/messages?key=secret-query&trace=visible&key=second%2Bsecret"
	body := []byte(`{"prompt":"keep exactly"}`)
	headers := http.Header{
		"Authorization": {"Bearer inbound-secret", "ApiKey secondary-secret"},
		"X-Trace":       {"one", "two"},
	}
	wantHeaders := headers.Clone()
	entry := captureRequestLogEntry(
		http.MethodPost,
		target,
		headers,
		body,
		model.RequestTransportHTTP,
	)
	body[0] = 'X'
	headers["Authorization"][0] = "changed after capture"
	headers["X-Trace"] = []string{"changed after capture"}

	if string(entry.Body) != `{"prompt":"keep exactly"}` {
		t.Fatalf("captured body changed: %q", entry.Body)
	}
	if entry.URL != target {
		t.Fatalf("captured URL=%q, want exact target %q", entry.URL, target)
	}
	var gotHeaders http.Header
	if err := json.Unmarshal([]byte(entry.Headers), &gotHeaders); err != nil {
		t.Fatalf("decode captured headers: %v", err)
	}
	if !reflect.DeepEqual(gotHeaders, wantHeaders) {
		t.Fatalf("captured headers=%v, want exact values %v", gotHeaders, wantHeaders)
	}
}

func TestProxyPersistsRequestSnapshotsWhenDebugIsDisabled(t *testing.T) {
	var sentBody []byte
	var sentAuthorization string
	upstream := newTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sentBody, _ = io.ReadAll(r.Body)
		sentAuthorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
			"id":"chatcmpl-request-log","object":"chat.completion","model":"gpt-request-log",
			"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}
		}`)
	}))
	t.Cleanup(upstream.Close)

	const upstreamKey = "sk-upstream-request-secret"
	env := setupProxyTestEnv(t, []testChannel{{
		name: "request-log", upstreamProtocol: "openai",
		models: "gpt-request-log", apiKey: upstreamKey,
	}}, map[int]string{0: upstream.URL})

	const inboundTarget = "/v1/chat/completions?trace=client&api_key=inbound-query-secret"
	response := doProxyRequest(t, env.engine, inboundTarget, map[string]any{
		"model":    "gpt-request-log",
		"messages": []any{map[string]any{"role": "user", "content": "persist me"}},
	}, map[string]string{"X-Client-Trace": "trace-value"})
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}

	logEntry := waitForProxyLog(t, env, "gpt-request-log")
	inbound, err := env.store.GetLogInboundRequest(context.Background(), logEntry.ID)
	if err != nil {
		t.Fatalf("get inbound request: %v", err)
	}
	if inbound == nil || inbound.URL != inboundTarget ||
		gjson.GetBytes(inbound.Body, "messages.0.content").String() != "persist me" ||
		gjson.Get(inbound.Headers, "X-Client-Trace.0").String() != "trace-value" {
		t.Fatalf("inbound request=%+v", inbound)
	}
	if got := gjson.Get(inbound.Headers, "Authorization.0").String(); got != "Bearer test-api-key" {
		t.Fatalf("persisted inbound authorization=%q, want original value", got)
	}

	requests, err := env.store.GetLogUpstreamRequests(context.Background(), logEntry.ID)
	if err != nil {
		t.Fatalf("get upstream requests: %v", err)
	}
	if len(requests) != 1 {
		t.Fatalf("upstream requests=%+v, want one", requests)
	}
	if requests[0].Method != http.MethodPost ||
		!strings.HasPrefix(requests[0].URL, upstream.URL) ||
		!bytes.Equal(requests[0].Body, sentBody) {
		t.Fatalf("upstream request=%+v sent_body=%s", requests[0], sentBody)
	}
	if !strings.Contains(requests[0].Headers, upstreamKey) {
		t.Fatalf("persisted upstream headers omitted API key: %s", requests[0].Headers)
	}
	if sentAuthorization != "Bearer "+upstreamKey {
		t.Fatalf("wire authorization=%q, want upstream API key", sentAuthorization)
	}
	if got := gjson.Get(requests[0].Headers, "Authorization.0").String(); got != sentAuthorization {
		t.Fatalf("persisted upstream authorization=%q, want wire value %q", got, sentAuthorization)
	}

	debugLog, err := env.store.GetDebugLogByLogID(context.Background(), logEntry.ID)
	if err != nil {
		t.Fatalf("get disabled debug log: %v", err)
	}
	if debugLog != nil {
		t.Fatalf("request snapshots unexpectedly enabled debug response logging: %+v", debugLog)
	}
}
