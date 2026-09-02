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

const claudeSessionAffinityTTL = time.Hour

var claudeAffinityLoadFailureCount atomic.Uint64
var claudeAffinityWriteFailureCount atomic.Uint64
var claudeAffinityCleanupFailureCount atomic.Uint64
var claudeAffinityProbeFailureCount atomic.Uint64

// claudeAffinityTarget identifies one API key row. ChannelID and KeyIndex are
// runtime locators; APIKeyHash must also match before the key is preferred.
type claudeAffinityTarget struct {
	apiKeyID   int64
	channelID  int64
	keyIndex   int
	apiKeyHash string
	expiresAt  time.Time
}

func newClaudeAffinityTarget(
	cfg *model.Config,
	apiKeyID int64,
	keyIndex int,
	apiKey string,
) (claudeAffinityTarget, bool) {
	if cfg == nil || cfg.UsesOAuth() || apiKeyID <= 0 ||
		keyIndex < 0 || strings.TrimSpace(apiKey) == "" {
		return claudeAffinityTarget{}, false
	}
	return claudeAffinityTarget{
		apiKeyID:   apiKeyID,
		channelID:  cfg.ID,
		keyIndex:   keyIndex,
		apiKeyHash: claudeAffinityAPIKeyHash(apiKey),
	}, true
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
		apiKeyID:   entry.APIKeyID,
		channelID:  entry.ChannelID,
		keyIndex:   entry.KeyIndex,
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
		target.apiKeyID <= 0 || target.channelID <= 0 ||
		target.keyIndex < 0 || target.apiKeyHash == "" {
		return nil
	}
	now := affinity.now()
	target.expiresAt = now.Add(claudeSessionAffinityTTL)
	entry := &model.ClaudeSessionAffinity{
		SubjectSessionHash: affinity.subjectSessionHash,
		APIKeyID:           target.apiKeyID,
		ChannelID:          target.channelID,
		KeyIndex:           target.keyIndex,
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

func selectClaudeAffinityKey(
	cfg *model.Config,
	apiKeys []*model.APIKey,
	triedKeys map[int]bool,
	affinity *claudeSessionAffinity,
) (int, string, bool) {
	target, ok := affinity.targetSnapshot()
	if !ok || cfg == nil || target.channelID != cfg.ID || target.apiKeyHash == "" {
		return 0, "", false
	}
	now := affinity.now()
	for _, apiKey := range apiKeys {
		if apiKey == nil || apiKey.Disabled || apiKey.IsCoolingDown(now) || triedKeys[apiKey.KeyIndex] {
			continue
		}
		if apiKey.ID == target.apiKeyID &&
			claudeAffinityAPIKeyHash(apiKey.APIKey) == target.apiKeyHash {
			return apiKey.KeyIndex, apiKey.APIKey, true
		}
	}
	return 0, "", false
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

	var targetChannel *model.Config
	for _, candidate := range candidates {
		if candidate != nil && candidate.ID == target.channelID {
			targetChannel = candidate
			break
		}
	}
	if targetChannel == nil || targetChannel.UsesOAuth() {
		return nil, false
	}

	apiKeys, err := s.getAPIKeys(ctx, targetChannel.ID)
	if err != nil {
		count := claudeAffinityProbeFailureCount.Add(1)
		if count%100 == 1 {
			log.Printf("[WARN] 检查 Claude session 亲和 Key 失败 (累计: %d): %v", count, err)
		}
		return nil, false
	}
	keyIndex, _, available := selectClaudeAffinityKey(
		targetChannel,
		apiKeys,
		nil,
		reqCtx.claudeAffinity,
	)
	if !available {
		return nil, false
	}

	reqCtx.claudeAffinityTriedChannel = targetChannel.ID
	reqCtx.claudeAffinityTriedKey = keyIndex
	reqCtx.claudeAffinityProbe = true
	result, err := s.tryChannelWithKeys(ctx, targetChannel, reqCtx, w)
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
