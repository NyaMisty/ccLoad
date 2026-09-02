package storage

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"ccLoad/internal/model"
)

func TestHybridStoreLogRequestDetailsConvergeToPrimary(t *testing.T) {
	ctx := context.Background()
	sqlite := createTestSQLiteStore(t)
	primary := createTestSQLiteStore(t)
	releaseInitialization := make(chan struct{})
	hybrid := newHybridStore(sqlite, primary, func(initCtx context.Context) error {
		select {
		case <-releaseInitialization:
			return nil
		case <-initCtx.Done():
			return initCtx.Err()
		}
	})
	t.Cleanup(func() {
		select {
		case <-releaseInitialization:
		default:
			close(releaseInitialization)
		}
		_ = hybrid.Close()
	})

	inboundBody := []byte(`{"model":"gpt-request-log","prompt":"original"}`)
	upstreamBody := []byte(`{"model":"gpt-request-log","prompt":"translated"}`)
	expectedInboundBody := bytes.Clone(inboundBody)
	expectedUpstreamBody := bytes.Clone(upstreamBody)
	entry := &model.LogEntry{
		Time:       model.JSONTime{Time: time.Now()},
		Model:      "gpt-request-log",
		StatusCode: http.StatusOK,
		Message:    "ok",
		InboundRequest: &model.RequestLogEntry{
			CapturedAt: model.JSONTime{Time: time.Now()},
			Transport:  model.RequestTransportHTTP,
			Method:     http.MethodPost,
			URL:        "/v1/chat/completions",
			Headers:    `{"X-Trace":["inbound"]}`,
			Body:       inboundBody,
		},
		UpstreamRequests: []*model.RequestLogEntry{{
			CapturedAt: model.JSONTime{Time: time.Now()},
			Transport:  model.RequestTransportHTTP,
			Method:     http.MethodPost,
			URL:        "https://upstream.example/v1/chat/completions",
			Headers:    `{"X-Trace":["upstream"]}`,
			Body:       upstreamBody,
		}},
	}
	if err := hybrid.AddLog(ctx, entry); err != nil {
		t.Fatalf("add hybrid log: %v", err)
	}

	// The primary worker is deliberately blocked, so these mutations verify
	// that asynchronous replication owns deep copies of request bodies.
	entry.InboundRequest.Body[0] = 'X'
	entry.UpstreamRequests[0].Body[0] = 'X'
	close(releaseInitialization)

	var primaryLogID int64
	waitForCondition(t, 3*time.Second, func() bool {
		logs, err := primary.ListLogs(ctx, time.Time{}, 10, 0, nil)
		if err != nil || len(logs) != 1 || hybrid.RuntimeMetrics().PrimarySyncPending != 0 {
			return false
		}
		primaryLogID = logs[0].ID
		return true
	})

	inbound, err := primary.GetLogInboundRequest(ctx, primaryLogID)
	if err != nil {
		t.Fatalf("get primary inbound request: %v", err)
	}
	if inbound == nil || !bytes.Equal(inbound.Body, expectedInboundBody) {
		t.Fatalf("primary inbound request=%+v", inbound)
	}
	upstream, err := primary.GetLogUpstreamRequests(ctx, primaryLogID)
	if err != nil {
		t.Fatalf("get primary upstream requests: %v", err)
	}
	if len(upstream) != 1 || !bytes.Equal(upstream[0].Body, expectedUpstreamBody) {
		t.Fatalf("primary upstream requests=%+v", upstream)
	}
}
