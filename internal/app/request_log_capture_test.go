package app

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"ccLoad/internal/model"

	"github.com/tidwall/gjson"
)

func TestCaptureRequestLogEntryPreservesBodyAndRedactsCredentials(t *testing.T) {
	body := []byte(`{"prompt":"keep exactly"}`)
	entry := captureRequestLogEntry(
		http.MethodPost,
		"https://example.com/v1/messages?key=secret-query&trace=visible",
		http.Header{
			"Authorization": {"Bearer inbound-secret"},
			"X-Trace":       {"one", "two"},
		},
		body,
		model.RequestTransportHTTP,
	)
	body[0] = 'X'

	if string(entry.Body) != `{"prompt":"keep exactly"}` {
		t.Fatalf("captured body changed: %q", entry.Body)
	}
	if strings.Contains(entry.URL, "secret-query") ||
		!strings.Contains(entry.URL, "trace=visible") {
		t.Fatalf("captured URL was not selectively redacted: %q", entry.URL)
	}
	if strings.Contains(entry.Headers, "inbound-secret") {
		t.Fatalf("captured headers leaked credential: %s", entry.Headers)
	}
	if got := gjson.Get(entry.Headers, "X-Trace.#").Int(); got != 2 {
		t.Fatalf("multi-value header count=%d, headers=%s", got, entry.Headers)
	}
}

func TestProxyPersistsRequestSnapshotsWhenDebugIsDisabled(t *testing.T) {
	var sentBody []byte
	upstream := newTestHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sentBody, _ = io.ReadAll(r.Body)
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

	response := doProxyRequest(t, env.engine, "/v1/chat/completions?trace=client", map[string]any{
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
	if inbound == nil || inbound.URL != "/v1/chat/completions?trace=client" ||
		gjson.GetBytes(inbound.Body, "messages.0.content").String() != "persist me" ||
		gjson.Get(inbound.Headers, "X-Client-Trace.0").String() != "trace-value" {
		t.Fatalf("inbound request=%+v", inbound)
	}
	if strings.Contains(inbound.Headers, "test-api-key") {
		t.Fatalf("inbound headers leaked API token: %s", inbound.Headers)
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
	if strings.Contains(requests[0].Headers, upstreamKey) {
		t.Fatalf("upstream headers leaked API key: %s", requests[0].Headers)
	}

	debugLog, err := env.store.GetDebugLogByLogID(context.Background(), logEntry.ID)
	if err != nil {
		t.Fatalf("get disabled debug log: %v", err)
	}
	if debugLog != nil {
		t.Fatalf("request snapshots unexpectedly enabled debug response logging: %+v", debugLog)
	}
}
