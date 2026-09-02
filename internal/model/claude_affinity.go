package model

import "time"

// ClaudeSessionAffinity identifies the upstream API key preferred by one
// authenticated Claude session. ChannelID and KeyIndex are resolved from
// APIKeyID when reading; APIKeyHash protects against in-place key rotation.
type ClaudeSessionAffinity struct {
	SubjectSessionHash string
	APIKeyID           int64
	ChannelID          int64
	KeyIndex           int
	APIKeyHash         string
	ExpiresAt          time.Time
	UpdatedAt          time.Time
}
