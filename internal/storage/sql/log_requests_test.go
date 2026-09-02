package sql_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"ccLoad/internal/model"
)

func TestLogRequestDetailsPersistWithPlainAndDebugLogs(t *testing.T) {
	t.Parallel()

	store := newTestStore(t, "log-requests.db")
	ctx := context.Background()
	channelID := createTestChannel(t, ctx, store, "log-request-channel")
	now := time.Now().UTC().Round(0)

	plain := &model.LogEntry{
		Time:       newJSONTime(now),
		Model:      "gpt-4",
		ChannelID:  channelID,
		StatusCode: http.StatusOK,
		Message:    "ok",
		InboundRequest: &model.RequestLogEntry{
			CapturedAt: newJSONTime(now),
			Transport:  model.RequestTransportHTTP,
			Method:     http.MethodPost,
			URL:        "/v1/chat/completions?trace=one",
			Headers:    `{"Authorization":["Bear******oken"],"X-Trace":["one","two"]}`,
			Body:       []byte(`{"model":"gpt-4","messages":[]}`),
		},
		UpstreamRequests: []*model.RequestLogEntry{
			{
				CapturedAt: newJSONTime(now.Add(time.Millisecond)),
				Transport:  model.RequestTransportHTTP,
				Method:     http.MethodPost,
				URL:        "https://first.example.com/v1/chat/completions",
				Headers:    `{"Authorization":["Bear******ream"]}`,
				Body:       []byte(`{"model":"gpt-4","retry":0}`),
			},
			{
				CapturedAt: newJSONTime(now.Add(2 * time.Millisecond)),
				Transport:  model.RequestTransportWebsocket,
				Method:     "WEBSOCKET",
				URL:        "wss://second.example.com/v1/responses",
				Headers:    `{"Authorization":["Bear******ream"]}`,
				Body:       []byte(`{"type":"response.create"}`),
			},
		},
	}
	withDebug := &model.LogEntry{
		Time:       newJSONTime(now.Add(time.Second)),
		Model:      "gpt-4",
		ChannelID:  channelID,
		StatusCode: http.StatusBadGateway,
		Message:    "failed",
		DebugData: &model.DebugLogEntry{
			CreatedAt:  now.Unix(),
			ReqMethod:  http.MethodPost,
			ReqURL:     "https://debug.example.com",
			ReqHeaders: `{}`,
			ReqBody:    []byte(`{"debug":true}`),
		},
		InboundRequest: &model.RequestLogEntry{
			CapturedAt: newJSONTime(now.Add(time.Second)),
			Transport:  model.RequestTransportHTTP,
			Method:     http.MethodPost,
			URL:        "/v1/chat/completions",
			Headers:    `{}`,
			Body:       []byte(`{"debug":true}`),
		},
		UpstreamRequests: []*model.RequestLogEntry{{
			CapturedAt: newJSONTime(now.Add(time.Second)),
			Transport:  model.RequestTransportHTTP,
			Method:     http.MethodPost,
			URL:        "https://debug.example.com",
			Headers:    `{}`,
			Body:       []byte(`{"debug":true}`),
		}},
	}

	if err := store.BatchAddLogs(ctx, []*model.LogEntry{plain, withDebug}); err != nil {
		t.Fatalf("batch add logs with requests: %v", err)
	}
	if plain.ID <= 0 || withDebug.ID <= 0 || plain.ID == withDebug.ID {
		t.Fatalf("log IDs were not assigned: plain=%d debug=%d", plain.ID, withDebug.ID)
	}

	inbound, err := store.GetLogInboundRequest(ctx, plain.ID)
	if err != nil {
		t.Fatalf("get plain inbound request: %v", err)
	}
	if inbound == nil || inbound.LogID != plain.ID ||
		!bytes.Equal(inbound.Body, plain.InboundRequest.Body) ||
		inbound.Headers != plain.InboundRequest.Headers ||
		inbound.CapturedAt.UnixMilli() != plain.InboundRequest.CapturedAt.UnixMilli() {
		t.Fatalf("plain inbound request=%+v", inbound)
	}
	upstream, err := store.GetLogUpstreamRequests(ctx, plain.ID)
	if err != nil {
		t.Fatalf("get plain upstream requests: %v", err)
	}
	if len(upstream) != 2 || upstream[0].Sequence != 1 || upstream[1].Sequence != 2 ||
		upstream[0].Transport != model.RequestTransportHTTP ||
		upstream[1].Transport != model.RequestTransportWebsocket ||
		!bytes.Equal(upstream[1].Body, plain.UpstreamRequests[1].Body) {
		t.Fatalf("plain upstream requests=%+v", upstream)
	}

	debugInbound, err := store.GetLogInboundRequest(ctx, withDebug.ID)
	if err != nil || debugInbound == nil {
		t.Fatalf("get debug inbound request: request=%+v err=%v", debugInbound, err)
	}
	debugUpstream, err := store.GetLogUpstreamRequests(ctx, withDebug.ID)
	if err != nil || len(debugUpstream) != 1 {
		t.Fatalf("get debug upstream requests: requests=%+v err=%v", debugUpstream, err)
	}
	debugLog, err := store.GetDebugLogByLogID(ctx, withDebug.ID)
	if err != nil || debugLog == nil {
		t.Fatalf("get debug log: log=%+v err=%v", debugLog, err)
	}

	if err := store.CleanupLogsBefore(ctx, now.Add(2*time.Hour)); err != nil {
		t.Fatalf("cleanup parent logs: %v", err)
	}
	inbound, err = store.GetLogInboundRequest(ctx, plain.ID)
	if err != nil {
		t.Fatalf("get inbound after cleanup: %v", err)
	}
	upstream, err = store.GetLogUpstreamRequests(ctx, plain.ID)
	if err != nil {
		t.Fatalf("get upstream after cleanup: %v", err)
	}
	if inbound != nil || len(upstream) != 0 {
		t.Fatalf("request details survived parent cleanup: inbound=%+v upstream=%+v", inbound, upstream)
	}
}
