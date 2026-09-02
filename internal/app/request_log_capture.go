package app

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"ccLoad/internal/model"

	"github.com/bytedance/sonic"
)

func captureRequestLogEntry(
	method string,
	target string,
	header http.Header,
	body []byte,
	transport string,
) *model.RequestLogEntry {
	return &model.RequestLogEntry{
		CapturedAt: model.JSONTime{Time: time.Now()},
		Transport:  transport,
		Method:     method,
		URL:        redactRequestLogURL(target),
		Headers:    encodeRequestLogHeaders(header),
		Body:       append([]byte(nil), body...),
	}
}

func captureInboundRequestLog(req *http.Request, body []byte, transport string) *model.RequestLogEntry {
	if req == nil {
		return nil
	}
	target := ""
	if req.URL != nil {
		target = req.URL.RequestURI()
	}
	return captureRequestLogEntry(req.Method, target, req.Header, body, transport)
}

func captureUpstreamRequestLog(req *http.Request, body []byte, transport string) *model.RequestLogEntry {
	if req == nil {
		return nil
	}
	target := ""
	if req.URL != nil {
		target = req.URL.String()
	}
	return captureRequestLogEntry(req.Method, target, req.Header, body, transport)
}

func encodeRequestLogHeaders(header http.Header) string {
	headers := make(http.Header, len(header))
	for name, values := range header {
		copied := append([]string(nil), values...)
		if isSensitiveHeader(name) {
			for index := range copied {
				copied[index] = maskHeaderValue(copied[index])
			}
		}
		headers[name] = copied
	}
	encoded, _ := sonic.Marshal(headers)
	return string(encoded)
}

func redactRequestLogURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.RawQuery == "" {
		return rawURL
	}
	query := parsed.Query()
	changed := false
	for name, values := range query {
		if !isSensitiveRequestQuery(name) {
			continue
		}
		for index := range values {
			values[index] = maskHeaderValue(values[index])
		}
		query[name] = values
		changed = true
	}
	if !changed {
		return rawURL
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func isSensitiveRequestQuery(name string) bool {
	normalized := strings.NewReplacer("-", "", "_", "").Replace(strings.ToLower(strings.TrimSpace(name)))
	switch normalized {
	case "key", "apikey", "accesstoken", "authorization":
		return true
	default:
		return false
	}
}
