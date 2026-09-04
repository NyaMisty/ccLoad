package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"ccLoad/internal/model"
)

// GetClaudeSessionAffinity returns an unexpired upstream-key affinity.
func (s *SQLStore) GetClaudeSessionAffinity(
	ctx context.Context,
	subjectSessionHash string,
	now time.Time,
) (*model.ClaudeSessionAffinity, error) {
	subjectSessionHash = strings.TrimSpace(subjectSessionHash)
	if subjectSessionHash == "" {
		return nil, nil
	}

	var affinity model.ClaudeSessionAffinity
	var apiKeyID sql.NullInt64
	var expiresAt, updatedAt int64
	err := s.QueryRowContext(ctx, `
		SELECT subject_session_hash, target_kind, api_key_id, channel_id,
		       api_key_hash, expires_at, updated_at
		FROM claude_session_affinities
		WHERE subject_session_hash = ? AND expires_at > ?
	`, subjectSessionHash, timeToUnix(now)).Scan(
		&affinity.SubjectSessionHash,
		&affinity.TargetKind,
		&apiKeyID,
		&affinity.ChannelID,
		&affinity.APIKeyHash,
		&expiresAt,
		&updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get Claude session affinity: %w", err)
	}
	if apiKeyID.Valid {
		affinity.APIKeyID = apiKeyID.Int64
	}
	affinity.ExpiresAt = unixToTime(expiresAt)
	affinity.UpdatedAt = unixToTime(updatedAt)
	return &affinity, nil
}

// RememberClaudeSessionAffinity makes the latest successful upstream key the
// session's preferred key and renews the binding TTL.
func (s *SQLStore) RememberClaudeSessionAffinity(
	ctx context.Context,
	affinity *model.ClaudeSessionAffinity,
	now time.Time,
) error {
	if affinity == nil ||
		strings.TrimSpace(affinity.SubjectSessionHash) == "" ||
		!validClaudeAffinityTarget(affinity) ||
		affinity.ChannelID <= 0 ||
		strings.TrimSpace(affinity.APIKeyHash) == "" ||
		!affinity.ExpiresAt.After(now) {
		return errors.New("invalid Claude session affinity")
	}

	var apiKeyID any
	if affinity.APIKeyID > 0 {
		apiKeyID = affinity.APIKeyID
	}
	query := `
		INSERT INTO claude_session_affinities
			(subject_session_hash, target_kind, api_key_id, channel_id,
			 api_key_hash, expires_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(subject_session_hash) DO UPDATE SET
			target_kind = excluded.target_kind,
			api_key_id = excluded.api_key_id,
			channel_id = excluded.channel_id,
			api_key_hash = excluded.api_key_hash,
			expires_at = excluded.expires_at,
			updated_at = excluded.updated_at`
	args := []any{
		strings.TrimSpace(affinity.SubjectSessionHash),
		affinity.TargetKind,
		apiKeyID,
		affinity.ChannelID,
		strings.TrimSpace(affinity.APIKeyHash),
		timeToUnix(affinity.ExpiresAt),
		timeToUnix(now),
	}
	if s.IsMySQL() {
		query = `
			INSERT INTO claude_session_affinities
				(subject_session_hash, target_kind, api_key_id, channel_id,
				 api_key_hash, expires_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				target_kind = VALUES(target_kind),
				api_key_id = VALUES(api_key_id),
				channel_id = VALUES(channel_id),
				api_key_hash = VALUES(api_key_hash),
				expires_at = VALUES(expires_at),
				updated_at = VALUES(updated_at)`
	}

	if _, err := s.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("remember Claude session affinity: %w", err)
	}
	return nil
}

func validClaudeAffinityTarget(affinity *model.ClaudeSessionAffinity) bool {
	switch affinity.TargetKind {
	case model.ClaudeAffinityTargetAPIKey:
		return affinity.APIKeyID > 0
	case model.ClaudeAffinityTargetZAICodingPlan:
		return affinity.APIKeyID == 0
	default:
		return false
	}
}

// CleanupClaudeSessionAffinities deletes expired bindings.
func (s *SQLStore) CleanupClaudeSessionAffinities(ctx context.Context, now time.Time) error {
	if _, err := s.ExecContext(
		ctx,
		`DELETE FROM claude_session_affinities WHERE expires_at <= ?`,
		timeToUnix(now),
	); err != nil {
		return fmt.Errorf("cleanup Claude session affinities: %w", err)
	}
	return nil
}
