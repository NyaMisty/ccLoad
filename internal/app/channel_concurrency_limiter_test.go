package app

import (
	"errors"
	"io"
	"net/http"
	"testing"

	"ccLoad/internal/model"
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
