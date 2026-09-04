package sql_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"ccLoad/internal/model"
	"ccLoad/internal/storage"
)

func TestClaudeSessionAffinitySurvivesSQLiteReopen(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "claude-affinity.db")

	firstStore, err := storage.CreateSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("create first store: %v", err)
	}
	cfg, err := firstStore.CreateConfig(ctx, &model.Config{
		Name:         "persisted-affinity",
		URLs:         model.ChannelURLs{{URL: "https://api.example.com"}},
		Enabled:      true,
		ModelEntries: []model.ModelEntry{{Model: "claude-sonnet-4-6"}},
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err = firstStore.CreateAPIKeysBatch(ctx, []*model.APIKey{{
		ChannelID: cfg.ID,
		KeyIndex:  3,
		APIKey:    "sk-persisted",
	}}); err != nil {
		t.Fatalf("create API key: %v", err)
	}
	keys, err := firstStore.GetAPIKeys(ctx, cfg.ID)
	if err != nil || len(keys) != 1 {
		t.Fatalf("get API key: keys=%d err=%v", len(keys), err)
	}

	now := time.Date(2030, 2, 3, 4, 5, 6, 0, time.UTC)
	const subjectHash = "1111111111111111111111111111111111111111111111111111111111111111"
	const keyHash = "2222222222222222222222222222222222222222222222222222222222222222"
	if err = firstStore.RememberClaudeSessionAffinity(ctx, &model.ClaudeSessionAffinity{
		SubjectSessionHash: subjectHash,
		TargetKind:         model.ClaudeAffinityTargetAPIKey,
		APIKeyID:           keys[0].ID,
		ChannelID:          cfg.ID,
		APIKeyHash:         keyHash,
		ExpiresAt:          now.Add(time.Hour),
		UpdatedAt:          now,
	}, now); err != nil {
		t.Fatalf("remember affinity: %v", err)
	}
	if err = firstStore.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	secondStore, err := storage.CreateSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = secondStore.Close() })

	got, err := secondStore.GetClaudeSessionAffinity(ctx, subjectHash, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("get reopened affinity: %v", err)
	}
	if got == nil || got.TargetKind != model.ClaudeAffinityTargetAPIKey ||
		got.APIKeyID != keys[0].ID || got.ChannelID != cfg.ID ||
		got.APIKeyHash != keyHash || !got.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("reopened affinity=%+v", got)
	}

	if err = secondStore.DeleteAPIKey(ctx, cfg.ID, 3); err != nil {
		t.Fatalf("delete bound API key: %v", err)
	}
	got, err = secondStore.GetClaudeSessionAffinity(ctx, subjectHash, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("get affinity after key delete: %v", err)
	}
	if got == nil || got.APIKeyHash != keyHash {
		t.Fatalf("key-authoritative affinity was lost with its locator row: %+v", got)
	}

	const zaiKeyHash = "3333333333333333333333333333333333333333333333333333333333333333"
	if err = secondStore.RememberClaudeSessionAffinity(ctx, &model.ClaudeSessionAffinity{
		SubjectSessionHash: subjectHash,
		TargetKind:         model.ClaudeAffinityTargetZAICodingPlan,
		ChannelID:          cfg.ID,
		APIKeyHash:         zaiKeyHash,
		ExpiresAt:          now.Add(2 * time.Hour),
		UpdatedAt:          now,
	}, now); err != nil {
		t.Fatalf("remember ZCode affinity: %v", err)
	}
	got, err = secondStore.GetClaudeSessionAffinity(ctx, subjectHash, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("get ZCode affinity: %v", err)
	}
	if got == nil || got.TargetKind != model.ClaudeAffinityTargetZAICodingPlan ||
		got.APIKeyID != 0 || got.ChannelID != cfg.ID || got.APIKeyHash != zaiKeyHash {
		t.Fatalf("ZCode affinity=%+v", got)
	}
}
