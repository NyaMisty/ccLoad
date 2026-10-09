package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"ccLoad/internal/model"
)

// TestSelectCandidates_TokenRestrictionBindsCooldownFallback 验证令牌渠道限制
// 是硬路由约束：全冷却兜底挑「最早恢复」渠道时，只能在令牌允许的渠道里挑，
// 绝不能把白名单外的渠道放进来（修复前：兜底先选渠道、限制后过滤，
// 白名单外渠道被选中导致 403，表现为「白名单不生效」）。
func TestSelectCandidates_TokenRestrictionBindsCooldownFallback(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	server := &Server{store: store, channelBalancer: NewSmoothWeightedRR(), authService: newTestAuthService(t)}
	ctx := context.Background()
	now := time.Now()

	channels := []*model.Config{
		{Name: "outside-allowlist", URLs: model.ChannelURLs{{URL: "https://api1.com"}}, Priority: 100, ModelEntries: []model.ModelEntry{{Model: "test-model"}}, Enabled: true},
		{Name: "in-allowlist", URLs: model.ChannelURLs{{URL: "https://api2.com"}}, Priority: 90, ModelEntries: []model.ModelEntry{{Model: "test-model"}}, Enabled: true},
	}
	var ids []int64
	var allowlistedID int64
	for _, cfg := range channels {
		created, err := store.CreateConfig(ctx, cfg)
		if err != nil {
			t.Fatalf("CreateConfig(%s): %v", cfg.Name, err)
		}
		ids = append(ids, created.ID)
		if cfg.Name == "in-allowlist" {
			allowlistedID = created.ID
		}
	}

	// 全冷却：白名单外渠道最早恢复（30s），白名单内渠道最晚（5min）。
	// 修复前兜底会选中 outside-allowlist；修复后必须选中白名单内的 in-allowlist。
	if err := store.SetChannelCooldown(ctx, ids[0], now.Add(30*time.Second)); err != nil {
		t.Fatalf("SetChannelCooldown(id0): %v", err)
	}
	if err := store.SetChannelCooldown(ctx, ids[1], now.Add(5*time.Minute)); err != nil {
		t.Fatalf("SetChannelCooldown(id1): %v", err)
	}

	restriction, err := model.NewChannelRestriction(model.ChannelRestrictionModeAllow, []int64{allowlistedID})
	if err != nil {
		t.Fatalf("NewChannelRestriction: %v", err)
	}
	server.authService.authTokensMux.Lock()
	server.authService.authTokenChannels["restricted-token"] = restriction
	server.authService.authTokensMux.Unlock()

	candidates, err := server.selectCandidatesByModelAndClientProtocol(ctx, "test-model", "", "restricted-token")
	if err != nil {
		t.Fatalf("selectCandidates: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("期望兜底只在白名单内挑1个渠道，实际%d个", len(candidates))
	}
	if candidates[0].Name != "in-allowlist" {
		t.Fatalf("兜底必须选白名单内渠道 in-allowlist，实际选中 %s（白名单被绕过）", candidates[0].Name)
	}
}

// TestSelectCandidates_TokenRestrictionEmptyPoolReturnsSentinel 验证限制排除全部候选时
// 返回哨兵错误（上层映射 403），而不是退回无限制全量池继续兜底。
func TestSelectCandidates_TokenRestrictionEmptyPoolReturnsSentinel(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	server := &Server{store: store, channelBalancer: NewSmoothWeightedRR(), authService: newTestAuthService(t)}
	ctx := context.Background()

	created, err := store.CreateConfig(ctx, &model.Config{
		Name: "only-channel", URLs: model.ChannelURLs{{URL: "https://api1.com"}}, Priority: 100,
		ModelEntries: []model.ModelEntry{{Model: "test-model"}}, Enabled: true,
	})
	if err != nil {
		t.Fatalf("CreateConfig: %v", err)
	}

	// allow 白名单指向一个不存在的渠道 ID：所有真实候选都被排除。
	restriction, err := model.NewChannelRestriction(model.ChannelRestrictionModeAllow, []int64{created.ID + 999})
	if err != nil {
		t.Fatalf("NewChannelRestriction: %v", err)
	}
	server.authService.authTokensMux.Lock()
	server.authService.authTokenChannels["phantom-token"] = restriction
	server.authService.authTokensMux.Unlock()

	_, err = server.selectCandidatesByModelAndClientProtocol(ctx, "test-model", "", "phantom-token")
	if !errors.Is(err, ErrNoAllowedChannelForToken) {
		t.Fatalf("期望 ErrNoAllowedChannelForToken，实际: %v", err)
	}
}

// TestSelectCandidates_TokenDenyListStillWorks 验证 deny 模式在限制前置后行为不变：
// 黑名单渠道被排除，其余渠道正常参与（含全冷却兜底）。
func TestSelectCandidates_TokenDenyListStillWorks(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	server := &Server{store: store, channelBalancer: NewSmoothWeightedRR(), authService: newTestAuthService(t)}
	ctx := context.Background()
	now := time.Now()

	var deniedID int64
	for _, cfg := range []*model.Config{
		{Name: "denied-earliest", URLs: model.ChannelURLs{{URL: "https://api1.com"}}, Priority: 100, ModelEntries: []model.ModelEntry{{Model: "test-model"}}, Enabled: true},
		{Name: "allowed-later", URLs: model.ChannelURLs{{URL: "https://api2.com"}}, Priority: 90, ModelEntries: []model.ModelEntry{{Model: "test-model"}}, Enabled: true},
	} {
		created, err := store.CreateConfig(ctx, cfg)
		if err != nil {
			t.Fatalf("CreateConfig(%s): %v", cfg.Name, err)
		}
		if cfg.Name == "denied-earliest" {
			deniedID = created.ID
		}
	}

	// 全冷却：被 deny 的渠道最早恢复，兜底必须跳过它选 allowed-later。
	if err := store.SetChannelCooldown(ctx, deniedID, now.Add(30*time.Second)); err != nil {
		t.Fatalf("SetChannelCooldown: %v", err)
	}
	if err := store.SetChannelCooldown(ctx, deniedID+1, now.Add(5*time.Minute)); err != nil {
		t.Fatalf("SetChannelCooldown: %v", err)
	}

	restriction, err := model.NewChannelRestriction(model.ChannelRestrictionModeDeny, []int64{deniedID})
	if err != nil {
		t.Fatalf("NewChannelRestriction: %v", err)
	}
	server.authService.authTokensMux.Lock()
	server.authService.authTokenChannels["deny-token"] = restriction
	server.authService.authTokensMux.Unlock()

	candidates, err := server.selectCandidatesByModelAndClientProtocol(ctx, "test-model", "", "deny-token")
	if err != nil {
		t.Fatalf("selectCandidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Name != "allowed-later" {
		t.Fatalf("期望 deny 模式兜底选 allowed-later，实际: %+v", candidates)
	}
}
