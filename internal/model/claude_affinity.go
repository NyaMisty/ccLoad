package model

import "time"

// ClaudeAffinityTargetAPIKey and ClaudeAffinityTargetZAICodingPlan identify
// the two upstream key stores supported by persistent Claude soft affinity.
const (
	ClaudeAffinityTargetAPIKey        = "api_key"
	ClaudeAffinityTargetZAICodingPlan = "zai_coding_plan"
)

// ClaudeSessionAffinity identifies the upstream key preferred by one
// authenticated Claude session. APIKeyID and ChannelID are lookup hints;
// TargetKind plus APIKeyHash are the authoritative target identity.
type ClaudeSessionAffinity struct {
	SubjectSessionHash string
	TargetKind         string
	APIKeyID           int64
	ChannelID          int64
	APIKeyHash         string
	ExpiresAt          time.Time
	UpdatedAt          time.Time
}
