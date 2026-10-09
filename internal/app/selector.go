package app

import (
	"context"
	"errors"
	"net/url"
	"strings"

	modelpkg "ccLoad/internal/model"
	"ccLoad/internal/protocol"
	"ccLoad/internal/util"
)

func normalizeOptionalProtocol(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return util.NormalizeProtocol(value)
}

// ErrNoAllowedChannelForToken 表示令牌的渠道限制把所有候选（含全冷却兜底候选）
// 全部排除。调用方必须直接拒绝请求，绝不能绕过限制去选路。
var ErrNoAllowedChannelForToken = errors.New("no allowed upstream channel for this token")

// filterChannelsByTokenRestriction 按令牌渠道限制压缩候选列表。
// restriction 为 nil 表示无限制（绝大多数请求），原样返回。
// 第二返回值报告本次是否实际执行了限制过滤。
// 注意：入参是共享快照的外层 slice，本函数只做压缩、不重排，调用方使用返回值。
func filterChannelsByTokenRestriction(channels []*modelpkg.Config, restriction interface {
	Allows(channelID int64) bool
}) (filtered []*modelpkg.Config, restricted bool) {
	if restriction == nil || len(channels) == 0 {
		return channels, false
	}
	kept := make([]*modelpkg.Config, 0, len(channels))
	for _, cfg := range channels {
		if cfg != nil && restriction.Allows(cfg.ID) {
			kept = append(kept, cfg)
		}
	}
	return kept, true
}

// tokenChannelRestriction 读取令牌的渠道限制策略；无限制或服务不可用时返回 nil。
func (s *Server) tokenChannelRestriction(tokenHash string) interface {
	Allows(channelID int64) bool
} {
	if s.authService == nil || tokenHash == "" {
		return nil
	}
	restriction, hasRestriction := s.authService.getChannelRestriction(tokenHash)
	if !hasRestriction {
		return nil
	}
	return restriction
}

// restrictionAllowsChannel 报告限制策略是否放行渠道；restriction 为 nil 时恒放行。
func restrictionAllowsChannel(restriction interface {
	Allows(channelID int64) bool
}, channelID int64) bool {
	if restriction == nil {
		return true
	}
	return restriction.Allows(channelID)
}

// selectCandidatesByClientProtocol 返回所有启用渠道；clientProtocol 仅用于计算客户端协议对应的模型冷却键。
// 可选的 tokenHash 启用令牌渠道限制过滤（硬路由约束，先于冷却/兜底生效）。
func (s *Server) selectCandidatesByClientProtocol(ctx context.Context, clientProtocol string, tokenHash ...string) ([]*modelpkg.Config, error) {
	channels, err := s.getEnabledChannelsSnapshotByModel(ctx, "*")
	if err != nil {
		return nil, err
	}
	// 令牌渠道限制是硬路由约束，必须先于冷却过滤/全冷却兜底生效，
	// 否则兜底挑出的「最早恢复」渠道可能根本不在令牌的允许范围内。
	if restricted, restrictedByToken := filterChannelsByTokenRestriction(channels, s.tokenChannelRestriction(firstOrEmpty(tokenHash))); restrictedByToken {
		if len(restricted) == 0 {
			return nil, ErrNoAllowedChannelForToken
		}
		channels = restricted
	}
	return s.filterCooldownChannels(ctx, channels, "*", clientProtocol)
}

func firstOrEmpty(values []string) string {
	if len(values) > 0 {
		return values[0]
	}
	return ""
}

// alphaSearchUpstreamURLs removes exact URLs for other Codex endpoints and
// endpoints that recently proved they do not implement alpha/search.
// Normal base URLs remain eligible because the request path is appended later.
func (s *Server) alphaSearchUpstreamURLs(cfg *modelpkg.Config) []string {
	urls := cfg.GetURLs()
	compatible := make([]string, 0, len(urls))
	for _, rawURL := range urls {
		key := protocolCapabilityKey{
			channelID: cfg.ID, baseURL: rawURL,
			clientProtocol: protocol.Codex, requestFamily: protocol.RequestFamilyAlphaSearch,
		}
		if cached, known := s.protocolCapabilities.get(key); known && cached == protocolUnsupported {
			continue
		}
		if !modelpkg.HasExactUpstreamURLMarker(rawURL) {
			compatible = append(compatible, rawURL)
			continue
		}

		parsed, err := url.Parse(modelpkg.StripExactUpstreamURLMarker(rawURL))
		if err == nil && protocol.DetectRequestFamily(parsed.Path) == protocol.RequestFamilyAlphaSearch {
			compatible = append(compatible, rawURL)
		}
	}
	return compatible
}

func (s *Server) selectAlphaSearchCandidates(ctx context.Context, modelName string, tokenHash ...string) ([]*modelpkg.Config, error) {
	routeModel := modelName
	if routeModel == "" {
		routeModel = "*"
	}
	channels, err := s.getEnabledChannelsSnapshotByModel(ctx, routeModel)
	if err != nil {
		return nil, err
	}
	// 硬路由约束：先于 URL 兼容过滤与冷却兜底。
	if restricted, restrictedByToken := filterChannelsByTokenRestriction(channels, s.tokenChannelRestriction(firstOrEmpty(tokenHash))); restrictedByToken {
		if len(restricted) == 0 {
			return nil, ErrNoAllowedChannelForToken
		}
		channels = restricted
	}

	compatible := make([]*modelpkg.Config, 0, len(channels))
	for _, cfg := range channels {
		if cfg == nil {
			continue
		}

		urls := s.alphaSearchUpstreamURLs(cfg)
		if len(urls) == 0 {
			continue
		}
		if len(urls) != len(cfg.GetURLs()) {
			configuredURLs := cfg.URLs
			cfg = cfg.Clone()
			allowed := make(map[string]struct{}, len(urls))
			for _, runtimeURL := range urls {
				allowed[runtimeURL] = struct{}{}
			}
			cfg.URLs = cfg.URLs[:0]
			for _, entry := range configuredURLs {
				if _, ok := allowed[entry.RuntimeURL()]; ok {
					cfg.URLs = append(cfg.URLs, entry)
				}
			}
		}
		compatible = append(compatible, cfg)
	}

	return s.filterCooldownChannels(ctx, compatible, routeModel, string(protocol.Codex))
}

// selectCandidatesByModelAndClientProtocol 按模型选择候选渠道；clientProtocol 仅表示客户端协议，不过滤上游主协议。
// 可选的 tokenHash 启用令牌渠道限制过滤——限制是硬路由约束，先于冷却过滤与
// 全冷却兜底生效：兜底挑「最早恢复」也只能在令牌允许的渠道里挑。
func (s *Server) selectCandidatesByModelAndClientProtocol(ctx context.Context, model string, clientProtocol string, tokenHash ...string) ([]*modelpkg.Config, error) {
	normalizedType := normalizeOptionalProtocol(clientProtocol)
	restriction := s.tokenChannelRestriction(firstOrEmpty(tokenHash))

	channels, err := s.getEnabledChannelsSnapshotByModel(ctx, model)
	if err != nil {
		return nil, err
	}
	if restricted, restrictedByToken := filterChannelsByTokenRestriction(channels, restriction); restrictedByToken {
		if len(restricted) == 0 {
			return nil, ErrNoAllowedChannelForToken
		}
		channels = restricted
	}

	// 先做冷却/成本过滤，但不触发“全冷却兜底”，以便后续还能继续做模糊匹配回退。
	filtered, err := s.filterCooldownChannelsStrict(ctx, channels, model, normalizedType)
	if err != nil {
		return nil, err
	}
	if len(filtered) > 0 {
		return filtered, nil
	}

	// 兜底：全量查询（用于“模糊匹配回退”以及最终“全冷却兜底”场景）
	// 注意：此处不能以 len(channels)==0 作为是否回退的条件。
	// 精确候选可能存在但全部在冷却/成本限额下不可用，这时仍需尝试模糊匹配补充候选。
	var allCandidates []*modelpkg.Config
	if model != "*" {
		source, err := s.getEnabledChannelsSnapshotByModel(ctx, "*")
		if err != nil {
			return nil, err
		}
		if len(source) == 0 {
			source, err = s.store.ListConfigs(ctx)
			if err != nil {
				return nil, err
			}
		}

		allCandidates = make([]*modelpkg.Config, 0, len(source))
		modelMatched := 0
		for _, cfg := range source {
			if cfg == nil || !cfg.Enabled {
				continue
			}
			if !s.configSupportsModelWithFuzzyMatch(cfg, model) {
				continue
			}
			modelMatched++
			if !restrictionAllowsChannel(restriction, cfg.ID) {
				continue
			}
			allCandidates = append(allCandidates, cfg)
		}
		// 有渠道支持该模型但全被限制排除：403 哨兵。
		// 「限制排除一切」与「模型谁都不支持」必须区分，后者保持 503 语义。
		if restriction != nil && modelMatched > 0 && len(allCandidates) == 0 {
			return nil, ErrNoAllowedChannelForToken
		}
	}

	// 再次过滤，但仍不触发“全冷却兜底”：先把可用的候选尽可能找出来。
	filtered, err = s.filterCooldownChannelsStrict(ctx, allCandidates, model, normalizedType)
	if err != nil {
		return nil, err
	}
	if len(filtered) > 0 {
		return filtered, nil
	}

	// 最终兜底：如果候选存在但全部在冷却中，让全冷却兜底逻辑选择“最早恢复”的渠道。
	// allCandidates 已经过限制过滤，兜底自然只在令牌允许的渠道里挑。
	return s.filterCooldownChannels(ctx, allCandidates, model, normalizedType)
}
