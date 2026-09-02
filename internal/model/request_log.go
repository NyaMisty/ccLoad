package model

// Request transport values persisted with raw request snapshots.
const (
	RequestTransportHTTP      = "http"
	RequestTransportWebsocket = "websocket"
)

// RequestLogEntry is an unredacted snapshot of an HTTP or WebSocket request.
// URL and body are preserved as captured; Headers contains every captured
// header name and value encoded as JSON.
type RequestLogEntry struct {
	LogID      int64    `json:"log_id"`
	Sequence   int      `json:"sequence,omitempty"`
	CapturedAt JSONTime `json:"captured_at"`
	Transport  string   `json:"transport"`
	Method     string   `json:"method"`
	URL        string   `json:"url"`
	Headers    string   `json:"headers"`
	Body       []byte   `json:"body"`
}

// Clone returns a deep copy suitable for an asynchronous log write.
func (entry *RequestLogEntry) Clone() *RequestLogEntry {
	if entry == nil {
		return nil
	}
	clone := *entry
	clone.Body = append([]byte(nil), entry.Body...)
	return &clone
}

// CloneRequestLogEntries returns deep copies of request snapshots.
func CloneRequestLogEntries(entries []*RequestLogEntry) []*RequestLogEntry {
	if len(entries) == 0 {
		return nil
	}
	clones := make([]*RequestLogEntry, 0, len(entries))
	for _, entry := range entries {
		if entry != nil {
			clones = append(clones, entry.Clone())
		}
	}
	return clones
}
