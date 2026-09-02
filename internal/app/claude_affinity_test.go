package app

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"ccLoad/internal/model"
	"ccLoad/internal/storage"
)

func TestClaudeSessionAffinityRequestIdentity(t *testing.T) {
	const (
		headerSession   = "11111111-2222-4333-8444-555555555555"
		metadataSession = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	)
	identity, err := json.Marshal(map[string]string{
		"device_id":  "device",
		"session_id": metadataSession,
	})
	if err != nil {
		t.Fatalf("marshal Claude identity: %v", err)
	}
	body, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"user_id": string(identity)},
	})
	if err != nil {
		t.Fatalf("marshal Claude request: %v", err)
	}
	store, err := storage.CreateSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("create affinity store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	fromHeader := newClaudeSessionAffinity(
		store,
		"token-a",
		http.Header{"X-Claude-Code-Session-Id": {headerSession}},
		body,
	)
	sameHeader := newClaudeSessionAffinity(
		store,
		"token-a",
		http.Header{"X-Claude-Code-Session-Id": {"11111111222243338444555555555555"}},
		nil,
	)
	fromMetadata := newClaudeSessionAffinity(store, "token-a", nil, body)
	fromInvalidHeaderFallback := newClaudeSessionAffinity(
		store,
		"token-a",
		http.Header{"X-Claude-Code-Session-Id": {"not-a-uuid"}},
		body,
	)
	otherSubject := newClaudeSessionAffinity(
		store,
		"token-b",
		http.Header{"X-Claude-Code-Session-Id": {headerSession}},
		body,
	)
	if fromHeader == nil || sameHeader == nil || fromMetadata == nil ||
		fromInvalidHeaderFallback == nil || otherSubject == nil {
		t.Fatal("valid Claude session identity did not create affinity")
	}
	if fromHeader.subjectSessionHash != sameHeader.subjectSessionHash {
		t.Fatal("equivalent UUID spellings produced different affinity identities")
	}
	if fromHeader.subjectSessionHash == fromMetadata.subjectSessionHash {
		t.Fatal("metadata session was not isolated from the preferred header session")
	}
	if fromMetadata.subjectSessionHash != fromInvalidHeaderFallback.subjectSessionHash {
		t.Fatal("invalid session header did not fall back to valid metadata session")
	}
	if fromHeader.subjectSessionHash == otherSubject.subjectSessionHash {
		t.Fatal("different inbound tokens shared Claude affinity")
	}
	if newClaudeSessionAffinity(store, "token-a", http.Header{
		"X-Claude-Code-Session-Id": {"not-a-uuid"},
	}, []byte(`{"metadata":{}}`)) != nil {
		t.Fatal("invalid session identity must not create affinity")
	}
	if newClaudeSessionAffinity(store, "", http.Header{
		"X-Claude-Code-Session-Id": {headerSession},
	}, nil) != nil {
		t.Fatal("missing authenticated subject must not create affinity")
	}
}

func TestClaudeSessionAffinityFollowsLatestSuccessfulKey(t *testing.T) {
	ctx := context.Background()
	store, err := storage.CreateSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("create affinity store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	createChannel := func(name, apiKey string) (*model.Config, *model.APIKey) {
		t.Helper()
		cfg, createErr := store.CreateConfig(ctx, &model.Config{
			Name:         name,
			URLs:         model.ChannelURLs{{URL: "https://" + name + ".example.com"}},
			Enabled:      true,
			ModelEntries: []model.ModelEntry{{Model: "claude-sonnet-4-6"}},
		})
		if createErr != nil {
			t.Fatalf("create channel %s: %v", name, createErr)
		}
		if createErr = store.CreateAPIKeysBatch(ctx, []*model.APIKey{{
			ChannelID: cfg.ID, KeyIndex: 0, APIKey: apiKey,
		}}); createErr != nil {
			t.Fatalf("create key for %s: %v", name, createErr)
		}
		keys, getErr := store.GetAPIKeys(ctx, cfg.ID)
		if getErr != nil || len(keys) != 1 {
			t.Fatalf("get key for %s: keys=%d err=%v", name, len(keys), getErr)
		}
		return cfg, keys[0]
	}
	firstCfg, firstKey := createChannel("affinity-first", "sk-first")
	fallbackCfg, fallbackKey := createChannel("affinity-fallback", "sk-fallback")

	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	newAffinity := func() *claudeSessionAffinity {
		t.Helper()
		affinity := newClaudeSessionAffinity(
			store,
			"token-a",
			http.Header{"X-Claude-Code-Session-Id": {"11111111-2222-4333-8444-555555555555"}},
			nil,
		)
		affinity.now = func() time.Time { return now }
		return affinity
	}
	first, ok := newClaudeAffinityTarget(firstCfg, firstKey.ID, firstKey.KeyIndex, firstKey.APIKey)
	if !ok {
		t.Fatal("first API key did not create an affinity target")
	}
	fallback, ok := newClaudeAffinityTarget(
		fallbackCfg,
		fallbackKey.ID,
		fallbackKey.KeyIndex,
		fallbackKey.APIKey,
	)
	if !ok {
		t.Fatal("fallback API key did not create an affinity target")
	}

	affinity := newAffinity()
	if err = affinity.load(ctx); err != nil {
		t.Fatalf("load empty affinity: %v", err)
	}
	if err = affinity.remember(ctx, first); err != nil {
		t.Fatalf("remember first key: %v", err)
	}
	if err = affinity.remember(ctx, fallback); err != nil {
		t.Fatalf("remember fallback key: %v", err)
	}
	if got, found := affinity.targetSnapshot(); !found || got.apiKeyHash != fallback.apiKeyHash {
		t.Fatalf("in-memory affinity did not follow fallback: got=%+v found=%t", got, found)
	}

	reloaded := newAffinity()
	if err = reloaded.load(ctx); err != nil {
		t.Fatalf("reload affinity: %v", err)
	}
	if got, found := reloaded.targetSnapshot(); !found || got.apiKeyHash != fallback.apiKeyHash {
		t.Fatalf("persisted affinity did not follow fallback: got=%+v found=%t", got, found)
	}

	now = now.Add(30 * time.Minute)
	if err = reloaded.remember(ctx, first); err != nil {
		t.Fatalf("move affinity back to first key: %v", err)
	}
	movedAgain := newAffinity()
	if err = movedAgain.load(ctx); err != nil {
		t.Fatalf("load affinity moved back to first key: %v", err)
	}
	if got, found := movedAgain.targetSnapshot(); !found ||
		got.apiKeyHash != first.apiKeyHash ||
		!got.expiresAt.Equal(now.Add(claudeSessionAffinityTTL)) {
		t.Fatalf("latest successful key was not persisted: got=%+v found=%t", got, found)
	}

	now = now.Add(claudeSessionAffinityTTL + time.Second)
	expired := newAffinity()
	if err = expired.load(ctx); err != nil {
		t.Fatalf("load expired affinity: %v", err)
	}
	if _, found := expired.targetSnapshot(); found {
		t.Fatal("expired affinity remained available")
	}
}

func TestClaudeSessionAffinitySelectsOnlyHealthyBoundKey(t *testing.T) {
	now := time.Now()
	preferred := &model.Config{ID: 2}
	affinity := &claudeSessionAffinity{
		target: claudeAffinityTarget{
			apiKeyID:   22,
			channelID:  preferred.ID,
			keyIndex:   99,
			apiKeyHash: claudeAffinityAPIKeyHash("sk-preferred"),
			expiresAt:  now.Add(time.Hour),
		},
		hasTarget: true,
		now:       func() time.Time { return now },
	}

	apiKeys := []*model.APIKey{
		{ID: 11, KeyIndex: 0, APIKey: "sk-other"},
		{ID: 22, KeyIndex: 1, APIKey: "sk-preferred"},
	}
	keyIndex, apiKey, pinned := selectClaudeAffinityKey(preferred, apiKeys, nil, affinity)
	if !pinned || keyIndex != 1 || apiKey != "sk-preferred" {
		t.Fatalf("preferred key selection index=%d key=%q pinned=%t", keyIndex, apiKey, pinned)
	}
	apiKeys[1].CooldownUntil = now.Add(time.Minute).Unix()
	if _, _, pinned = selectClaudeAffinityKey(preferred, apiKeys, nil, affinity); pinned {
		t.Fatal("cooled preferred key must not override normal fallback selection")
	}
	apiKeys[1].CooldownUntil = 0
	apiKeys[1].Disabled = true
	if _, _, pinned = selectClaudeAffinityKey(preferred, apiKeys, nil, affinity); pinned {
		t.Fatal("disabled preferred key must not override normal fallback selection")
	}
}
