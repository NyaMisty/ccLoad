package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"ccLoad/internal/model"
	"ccLoad/internal/storage"

	"github.com/google/uuid"
)

const (
	claudeSessionAffinityTTL      = time.Hour
	claudeAffinityConcurrencyWait = 5 * time.Second
)

type claudeAffinityConcurrencyWaitContextKey struct{}

var claudeAffinityLoadFailureCount atomic.Uint64
var claudeAffinityWriteFailureCount atomic.Uint64
var claudeAffinityCleanupFailureCount atomic.Uint64
var claudeAffinityProbeFailureCount atomic.Uint64

var errClaudeAffinityTargetAlreadyTried = errors.New("claude session affinity target already tried")

// claudeAffinityTarget identifies one upstream key. Row and channel IDs are
// lookup hints; targetKind plus apiKeyHash are authoritative.
type claudeAffinityTarget struct {
	targetKind string
	apiKeyID   int64
	channelID  int64
	apiKeyHash string
	expiresAt  time.Time
}

func newClaudeAffinityTarget(
	cfg *model.Config,
	apiKeyID int64,
	keyIndex int,
	apiKey string,
) (claudeAffinityTarget, bool) {
	apiKey = strings.TrimSpace(apiKey)
	if cfg == nil || cfg.ID <= 0 || apiKey == "" {
		return claudeAffinityTarget{}, false
	}
	target := claudeAffinityTarget{
		targetKind: claudeAffinityTargetKind(cfg),
		channelID:  cfg.ID,
		apiKeyHash: claudeAffinityAPIKeyHash(apiKey),
	}
	switch target.targetKind {
	case model.ClaudeAffinityTargetZAICodingPlan:
	case model.ClaudeAffinityTargetAPIKey:
		if apiKeyID <= 0 || keyIndex < 0 {
			return claudeAffinityTarget{}, false
		}
		target.apiKeyID = apiKeyID
	default:
		return claudeAffinityTarget{}, false
	}
	return target, true
}

func claudeAffinityTargetKind(cfg *model.Config) string {
	switch {
	case cfg == nil:
		return ""
	case cfg.UsesZAIOAuth():
		return model.ClaudeAffinityTargetZAICodingPlan
	case !cfg.UsesOAuth():
		return model.ClaudeAffinityTargetAPIKey
	default:
		return ""
	}
}

type claudeSessionAffinity struct {
	store              storage.Store
	subjectSessionHash string
	target             claudeAffinityTarget
	hasTarget          bool
	now                func() time.Time
}

func newClaudeSessionAffinity(
	store storage.Store,
	subject string,
	headers http.Header,
	body []byte,
) *claudeSessionAffinity {
	if store == nil {
		return nil
	}
	subject = strings.TrimSpace(subject)
	sessionID := claudeSessionAffinityID(headers, body)
	if subject == "" || sessionID == "" {
		return nil
	}
	return &claudeSessionAffinity{
		store: store,
		subjectSessionHash: claudeAffinityDigest(
			"ccload:claude:affinity\x00" + subject + "\x00" + sessionID,
		),
		now: time.Now,
	}
}

func claudeSessionAffinityID(headers http.Header, body []byte) string {
	if sessionID := canonicalClaudeSessionID(anthropicHeaderValue(headers, "X-Claude-Code-Session-Id")); sessionID != "" {
		return sessionID
	}
	request, err := decodeAnthropicRequest(body)
	if err != nil {
		return ""
	}
	return canonicalClaudeSessionID(anthropicSessionIDFromRequest(request))
}

func canonicalClaudeSessionID(sessionID string) string {
	parsed, err := uuid.Parse(strings.TrimSpace(sessionID))
	if err != nil {
		return ""
	}
	return parsed.String()
}

func claudeAffinityDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func claudeAffinityAPIKeyHash(apiKey string) string {
	return claudeAffinityDigest(
		"ccload:claude:affinity:key\x00" + strings.TrimSpace(apiKey),
	)
}

func (affinity *claudeSessionAffinity) load(ctx context.Context) error {
	if affinity == nil || affinity.store == nil {
		return nil
	}
	now := affinity.now()
	entry, err := affinity.store.GetClaudeSessionAffinity(
		ctx,
		affinity.subjectSessionHash,
		now,
	)
	if err != nil {
		return err
	}
	if entry == nil {
		affinity.target = claudeAffinityTarget{}
		affinity.hasTarget = false
		return nil
	}
	affinity.target = claudeAffinityTarget{
		targetKind: entry.TargetKind,
		apiKeyID:   entry.APIKeyID,
		channelID:  entry.ChannelID,
		apiKeyHash: entry.APIKeyHash,
		expiresAt:  entry.ExpiresAt,
	}
	affinity.hasTarget = true
	return nil
}

func (affinity *claudeSessionAffinity) targetSnapshot() (claudeAffinityTarget, bool) {
	if affinity == nil || !affinity.hasTarget {
		return claudeAffinityTarget{}, false
	}
	if !affinity.target.expiresAt.After(affinity.now()) {
		return claudeAffinityTarget{}, false
	}
	return affinity.target, true
}

func (affinity *claudeSessionAffinity) remember(
	ctx context.Context,
	target claudeAffinityTarget,
) error {
	if affinity == nil || affinity.store == nil ||
		!validClaudeAffinityTarget(target) ||
		target.channelID <= 0 || target.apiKeyHash == "" {
		return nil
	}
	now := affinity.now()
	target.expiresAt = now.Add(claudeSessionAffinityTTL)
	entry := &model.ClaudeSessionAffinity{
		SubjectSessionHash: affinity.subjectSessionHash,
		TargetKind:         target.targetKind,
		APIKeyID:           target.apiKeyID,
		ChannelID:          target.channelID,
		APIKeyHash:         target.apiKeyHash,
		ExpiresAt:          target.expiresAt,
		UpdatedAt:          now,
	}
	if err := affinity.store.RememberClaudeSessionAffinity(ctx, entry, now); err != nil {
		return err
	}

	affinity.target = target
	affinity.hasTarget = true
	return nil
}

func validClaudeAffinityTarget(target claudeAffinityTarget) bool {
	switch target.targetKind {
	case model.ClaudeAffinityTargetAPIKey:
		return target.apiKeyID > 0
	case model.ClaudeAffinityTargetZAICodingPlan:
		return target.apiKeyID == 0
	default:
		return false
	}
}

func selectClaudeAffinityKey(
	cfg *model.Config,
	apiKeys []*model.APIKey,
	triedKeys map[int]bool,
	affinity *claudeSessionAffinity,
) (int, string, bool) {
	target, ok := affinity.targetSnapshot()
	if !ok || cfg == nil ||
		target.targetKind != model.ClaudeAffinityTargetAPIKey ||
		target.apiKeyHash == "" {
		return 0, "", false
	}
	now := affinity.now()
	for _, apiKey := range apiKeys {
		if apiKey == nil || apiKey.Disabled || apiKey.IsCoolingDown(now) || triedKeys[apiKey.KeyIndex] {
			continue
		}
		if claudeAffinityAPIKeyHash(apiKey.APIKey) == target.apiKeyHash {
			return apiKey.KeyIndex, apiKey.APIKey, true
		}
	}
	return 0, "", false
}

type claudeAffinityAttemptKey struct {
	targetKind string
	apiKeyHash string
}

func (reqCtx *proxyRequestContext) recordClaudeAffinityProbeKey(cfg *model.Config, apiKey string) {
	if reqCtx == nil || !reqCtx.claudeAffinityProbe {
		return
	}
	targetKind := claudeAffinityTargetKind(cfg)
	apiKey = strings.TrimSpace(apiKey)
	if targetKind == "" || apiKey == "" {
		return
	}
	if reqCtx.claudeAffinityTriedKeys == nil {
		reqCtx.claudeAffinityTriedKeys = make(map[claudeAffinityAttemptKey]struct{}, 1)
	}
	reqCtx.claudeAffinityTriedKeys[claudeAffinityAttemptKey{
		targetKind: targetKind,
		apiKeyHash: claudeAffinityAPIKeyHash(apiKey),
	}] = struct{}{}
}

func (reqCtx *proxyRequestContext) claudeAffinityKeyWasTried(cfg *model.Config, apiKey string) bool {
	if reqCtx == nil || len(reqCtx.claudeAffinityTriedKeys) == 0 {
		return false
	}
	targetKind := claudeAffinityTargetKind(cfg)
	apiKey = strings.TrimSpace(apiKey)
	if targetKind == "" || apiKey == "" {
		return false
	}
	_, tried := reqCtx.claudeAffinityTriedKeys[claudeAffinityAttemptKey{
		targetKind: targetKind,
		apiKeyHash: claudeAffinityAPIKeyHash(apiKey),
	}]
	return tried
}

func (s *Server) claudeAffinityTargetMatches(
	ctx context.Context,
	cfg *model.Config,
	affinity *claudeSessionAffinity,
	target claudeAffinityTarget,
) (bool, error) {
	if cfg == nil {
		return false, nil
	}
	switch target.targetKind {
	case model.ClaudeAffinityTargetAPIKey:
		if cfg.UsesOAuth() {
			return false, nil
		}
		apiKeys, err := s.getAPIKeys(ctx, cfg.ID)
		if err != nil {
			return false, err
		}
		_, _, available := selectClaudeAffinityKey(cfg, apiKeys, nil, affinity)
		return available, nil
	case model.ClaudeAffinityTargetZAICodingPlan:
		if !cfg.UsesZAIOAuth() || s.zaiCredentials == nil {
			return false, nil
		}
		credential, err := s.zaiCredentials.credential(ctx, cfg, false)
		if err != nil {
			return false, err
		}
		return claudeAffinityAPIKeyHash(credential.APIKey) == target.apiKeyHash, nil
	default:
		return false, nil
	}
}

func (s *Server) findClaudeAffinityTargetChannel(
	ctx context.Context,
	candidates []*model.Config,
	affinity *claudeSessionAffinity,
	target claudeAffinityTarget,
) *model.Config {
	for pass := 0; pass < 2; pass++ {
		for _, candidate := range candidates {
			isHint := candidate != nil && candidate.ID == target.channelID
			if pass == 0 && !isHint || pass == 1 && isHint {
				continue
			}
			matches, err := s.claudeAffinityTargetMatches(ctx, candidate, affinity, target)
			if err != nil {
				count := claudeAffinityProbeFailureCount.Add(1)
				if count%100 == 1 {
					log.Printf("[WARN] 检查 Claude session 亲和 Key 失败 (累计: %d): %v", count, err)
				}
				continue
			}
			if matches {
				return candidate
			}
		}
	}
	return nil
}

// tryClaudeAffinityKey makes one key-scoped attempt before ordinary channel
// routing. If the key is unavailable or the attempt fails, the caller resumes
// the untouched candidate order, so affinity cannot turn into channel affinity.
func (s *Server) tryClaudeAffinityKey(
	ctx context.Context,
	candidates []*model.Config,
	reqCtx *proxyRequestContext,
	w http.ResponseWriter,
) (*proxyResult, bool) {
	if reqCtx == nil || reqCtx.claudeAffinity == nil {
		return nil, false
	}
	target, ok := reqCtx.claudeAffinity.targetSnapshot()
	if !ok {
		return nil, false
	}

	targetChannel := s.findClaudeAffinityTargetChannel(
		ctx,
		candidates,
		reqCtx.claudeAffinity,
		target,
	)
	if targetChannel == nil {
		return nil, false
	}

	reqCtx.claudeAffinityProbe = true
	affinityCtx := context.WithValue(
		ctx,
		claudeAffinityConcurrencyWaitContextKey{},
		claudeAffinityConcurrencyWait,
	)
	result, err := s.tryChannelWithKeys(affinityCtx, targetChannel, reqCtx, w)
	reqCtx.claudeAffinityProbe = false
	if err != nil {
		if !errors.Is(err, ErrAllKeysUnavailable) &&
			!errors.Is(err, ErrAllKeysExhausted) &&
			!errors.Is(err, ErrChannelRPMExceeded) &&
			!errors.Is(err, ErrKeyConcurrencyExceeded) {
			count := claudeAffinityProbeFailureCount.Add(1)
			if count%100 == 1 {
				log.Printf("[WARN] Claude session 亲和 Key 尝试失败 (累计: %d): %v", count, err)
			}
		}
		return nil, true
	}
	return result, true
}
