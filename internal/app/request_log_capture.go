package app

import (
	"net/http"
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
		URL:        target,
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
		headers[name] = append([]string(nil), values...)
	}
	encoded, _ := sonic.Marshal(headers)
	return string(encoded)
}
