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

// GetClaudeSessionAffinity returns an unexpired API-key affinity.
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
	var expiresAt, updatedAt int64
	err := s.QueryRowContext(ctx, `
		SELECT affinity.subject_session_hash, affinity.api_key_id,
		       key_row.channel_id, key_row.key_index, affinity.api_key_hash,
		       affinity.expires_at, affinity.updated_at
		FROM claude_session_affinities AS affinity
		INNER JOIN api_keys AS key_row ON key_row.id = affinity.api_key_id
		WHERE affinity.subject_session_hash = ? AND affinity.expires_at > ?
	`, subjectSessionHash, timeToUnix(now)).Scan(
		&affinity.SubjectSessionHash,
		&affinity.APIKeyID,
		&affinity.ChannelID,
		&affinity.KeyIndex,
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
	affinity.ExpiresAt = unixToTime(expiresAt)
	affinity.UpdatedAt = unixToTime(updatedAt)
	return &affinity, nil
}

// RememberClaudeSessionAffinity makes the latest successful API key the
// session's preferred key and renews the binding TTL.
func (s *SQLStore) RememberClaudeSessionAffinity(
	ctx context.Context,
	affinity *model.ClaudeSessionAffinity,
	now time.Time,
) error {
	if affinity == nil ||
		strings.TrimSpace(affinity.SubjectSessionHash) == "" ||
		affinity.APIKeyID <= 0 ||
		strings.TrimSpace(affinity.APIKeyHash) == "" ||
		!affinity.ExpiresAt.After(now) {
		return errors.New("invalid Claude session affinity")
	}

	query := `
		INSERT INTO claude_session_affinities
			(subject_session_hash, api_key_id, api_key_hash, expires_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(subject_session_hash) DO UPDATE SET
			api_key_id = excluded.api_key_id,
			api_key_hash = excluded.api_key_hash,
			expires_at = excluded.expires_at,
			updated_at = excluded.updated_at`
	args := []any{
		strings.TrimSpace(affinity.SubjectSessionHash),
		affinity.APIKeyID,
		strings.TrimSpace(affinity.APIKeyHash),
		timeToUnix(affinity.ExpiresAt),
		timeToUnix(now),
	}
	if s.IsMySQL() {
		query = `
			INSERT INTO claude_session_affinities
				(subject_session_hash, api_key_id, api_key_hash, expires_at, updated_at)
			VALUES (?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				api_key_id = VALUES(api_key_id),
				api_key_hash = VALUES(api_key_hash),
				expires_at = VALUES(expires_at),
				updated_at = VALUES(updated_at)`
	}

	if _, err := s.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("remember Claude session affinity: %w", err)
	}
	return nil
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
