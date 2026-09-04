package app

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"ccLoad/internal/model"
	"ccLoad/internal/protocol"
	"ccLoad/internal/zaiauth"

	"github.com/google/uuid"
)

// Z.ai Coding Plan wire contract.
//
// ZCode never calls the public Coding Plan origin directly: it rewrites the
// endpoint through the routing table published by zcode.z.ai and stamps every
// request with its client identity plus a metadata.user_id device fingerprint.
// ccLoad replicates all three so Coding Plan traffic proxied here is the same
// traffic ZCode itself would have sent.

func isZAICodingPlanBaseURL(raw string) bool {
	return strings.EqualFold(strings.TrimRight(strings.TrimSpace(raw), "/"), zaiauth.CodingPlanProxyBaseURL)
}

// isZAICodingPlanRequest reports whether this attempt uses the ZCode Coding
// Plan wire contract. API-key channels opt in by using the dedicated endpoint.
func isZAICodingPlanRequest(cfg *model.Config, upstream protocol.Protocol, requestPath string) bool {
	if cfg == nil || !isAnthropicMessagesRequest(upstream, requestPath) {
		return false
	}
	if cfg.UsesZAIOAuth() {
		return true
	}
	return cfg.GetAuthType() == model.AuthTypeAPIKey &&
		len(cfg.URLs) == 1 && !cfg.URLs[0].Exact &&
		isZAICodingPlanBaseURL(cfg.URLs[0].URL)
}

// zaiRequestIdentity is ZCode's metadata.user_id payload. Field order matches
// the official client because the value travels as an opaque JSON string.
type zaiRequestIdentity struct {
	DeviceID    string `json:"device_id"`
	AccountUUID string `json:"account_uuid"`
	SessionID   string `json:"session_id"`
}

// finalizeZAICodingPlanBody replaces the caller's metadata.user_id with the
// channel's ZCode fingerprint and the already key-scoped upstream session. A
// foreign client fingerprint must never reach the Coding Plan upstream.
func finalizeZAICodingPlanBody(body []byte, cfg *model.Config, sessionID string) ([]byte, error) {
	var request map[string]any
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, errors.New("finalize z.ai Coding Plan request: invalid JSON body")
	}
	identity, err := json.Marshal(zaiRequestIdentity{
		DeviceID:  zaiDeviceID(cfg),
		SessionID: sessionID,
	})
	if err != nil {
		return nil, errors.New("finalize z.ai Coding Plan request: invalid identity")
	}
	metadata, _ := request["metadata"].(map[string]any)
	if metadata == nil {
		metadata = make(map[string]any, 1)
	}
	metadata["user_id"] = string(identity)
	request["metadata"] = metadata
	finalized, err := json.Marshal(request)
	if err != nil {
		return nil, errors.New("finalize z.ai Coding Plan request: encode failed")
	}
	return finalized, nil
}

// injectZAICodingPlanHeaders rebuilds the request headers as ZCode sends them.
// The proxy and admin-test paths mark this as a wire rebuild, then re-run
// applyHeaderRules so channel header rules still apply. Auth headers stay
// blocked by the blacklist; ZCode identity headers (UA, x-session-id, ...)
// can be overridden, matching the Claude Code CLI fingerprint contract.
func injectZAICodingPlanHeaders(req *http.Request, cfg *model.Config, apiKey string, body []byte, incoming http.Header) {
	if req == nil {
		return
	}
	accept := anthropicHeaderValue(incoming, "Accept")
	if accept == "" {
		accept = "application/json"
	}
	anthropicVersion := anthropicHeaderValue(incoming, "anthropic-version")
	if anthropicVersion == "" {
		anthropicVersion = "2023-06-01"
	}
	for name := range req.Header {
		delete(req.Header, name)
	}
	setRawHeader(req.Header, "Accept", accept)
	setRawHeader(req.Header, "Content-Type", "application/json")
	setRawHeader(req.Header, "anthropic-version", anthropicVersion)
	// ZCode's Anthropic provider authenticates with x-api-key only.
	setRawHeader(req.Header, "x-api-key", strings.TrimSpace(apiKey))
	for _, entry := range zaiauth.SourceHeaders() {
		setRawHeader(req.Header, entry[0], entry[1])
	}
	setRawHeader(req.Header, "x-request-id", uuid.NewString())
	setRawHeader(req.Header, "x-zcode-trace-id", uuid.NewString())
	setRawHeader(req.Header, "x-session-id", resolveFinalZAISessionID(body, cfg, apiKey, incoming))
}

// resolveZAISourceSessionID resolves the session before applying the selected
// Coding Plan key. sourceBody and headers always belong to the inbound request,
// so retries never derive from a preceding attempt's wire session.
func resolveZAISourceSessionID(sourceBody, anthropicBody []byte, cfg *model.Config, headers http.Header) string {
	if sessionID := anthropicSessionIDFromHeaders(headers); sessionID != "" {
		return sessionID
	}
	if sessionID := anthropicSessionIDFromBody(sourceBody); sessionID != "" {
		return sessionID
	}
	if sessionID := anthropicSessionIDFromBody(anthropicBody); sessionID != "" {
		return sessionID
	}
	if deviceID := zaiDeviceID(cfg); deviceID != "" {
		request, _ := decodeAnthropicRequest(anthropicBody)
		messages, _ := request["messages"].([]any)
		return anthropicStableSessionID(deviceID, anthropicFirstUserText(messages))
	}
	return uuid.NewString()
}

func zaiUpstreamSessionID(apiKey, sourceSessionID string) string {
	apiKey = strings.TrimSpace(apiKey)
	sourceSessionID = canonicalClaudeSessionID(sourceSessionID)
	if apiKey == "" || sourceSessionID == "" {
		return sourceSessionID
	}
	return uuid.NewSHA1(
		uuid.NameSpaceOID,
		[]byte("ccload:zai:session\x00"+apiKey+"\x00"+sourceSessionID),
	).String()
}

func resolveFinalZAISessionID(body []byte, cfg *model.Config, apiKey string, headers http.Header) string {
	if sessionID := anthropicSessionIDFromBody(body); sessionID != "" {
		return sessionID
	}
	return zaiUpstreamSessionID(
		apiKey,
		resolveZAISourceSessionID(nil, body, cfg, headers),
	)
}

func enforceZAISessionHeader(req *http.Request, body []byte) {
	if req == nil {
		return
	}
	if sessionID := anthropicSessionIDFromBody(body); sessionID != "" {
		setRawHeader(req.Header, "x-session-id", sessionID)
	}
}

func zaiDeviceID(cfg *model.Config) string {
	if cfg == nil {
		return ""
	}
	if deviceID := strings.TrimSpace(cfg.ZAIDeviceID); deviceID != "" {
		return deviceID
	}
	credential, err := zaiauth.ParseCredential([]byte(cfg.OAuthCredential))
	if err != nil {
		return ""
	}
	return credential.DeviceID
}

// zaiCredentialRejected reports whether an upstream response means the Coding
// Plan key itself was refused. Z.ai answers 401 with its own error envelope.
func zaiCredentialRejected(status int) bool {
	return status == http.StatusUnauthorized
}
