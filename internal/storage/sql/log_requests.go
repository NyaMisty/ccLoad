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

func requestCapturedAtMillis(entry *model.RequestLogEntry) int64 {
	if entry == nil || entry.CapturedAt.IsZero() {
		return time.Now().UnixMilli()
	}
	return entry.CapturedAt.Round(0).UnixMilli()
}

func requestBodyForStorage(body []byte) []byte {
	if body == nil {
		return []byte{}
	}
	return body
}

func normalizeRequestTransport(transport string) string {
	switch strings.TrimSpace(transport) {
	case model.RequestTransportWebsocket:
		return model.RequestTransportWebsocket
	default:
		return model.RequestTransportHTTP
	}
}

func insertLogRequestDetails(
	ctx context.Context,
	store *SQLStore,
	tx *sql.Tx,
	logs []*model.LogEntry,
) error {
	var inboundStmt, upstreamStmt *normalizedStmt
	defer func() {
		if inboundStmt != nil {
			_ = inboundStmt.Close()
		}
		if upstreamStmt != nil {
			_ = upstreamStmt.Close()
		}
	}()

	for _, logEntry := range logs {
		if logEntry == nil ||
			(logEntry.InboundRequest == nil && len(logEntry.UpstreamRequests) == 0) {
			continue
		}
		if logEntry.ID <= 0 {
			return errors.New("persist request details: log ID was not assigned")
		}
		if inbound := logEntry.InboundRequest; inbound != nil {
			if inboundStmt == nil {
				var err error
				inboundStmt, err = store.prepareTx(ctx, tx, `
					INSERT INTO log_inbound_requests
						(log_id, captured_at, transport, method, url, headers, body)
					VALUES (?, ?, ?, ?, ?, ?, ?)`)
				if err != nil {
					return fmt.Errorf("prepare inbound request insert: %w", err)
				}
			}
			if _, err := inboundStmt.ExecContext(
				ctx,
				logEntry.ID,
				requestCapturedAtMillis(inbound),
				normalizeRequestTransport(inbound.Transport),
				inbound.Method,
				inbound.URL,
				inbound.Headers,
				requestBodyForStorage(inbound.Body),
			); err != nil {
				return fmt.Errorf("insert inbound request for log %d: %w", logEntry.ID, err)
			}
		}

		for index, upstream := range logEntry.UpstreamRequests {
			if upstream == nil {
				continue
			}
			if upstreamStmt == nil {
				var err error
				upstreamStmt, err = store.prepareTx(ctx, tx, `
					INSERT INTO log_upstream_requests
						(log_id, sequence, captured_at, transport, method, url, headers, body)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
				if err != nil {
					return fmt.Errorf("prepare upstream request insert: %w", err)
				}
			}
			if _, err := upstreamStmt.ExecContext(
				ctx,
				logEntry.ID,
				index+1,
				requestCapturedAtMillis(upstream),
				normalizeRequestTransport(upstream.Transport),
				upstream.Method,
				upstream.URL,
				upstream.Headers,
				requestBodyForStorage(upstream.Body),
			); err != nil {
				return fmt.Errorf(
					"insert upstream request %d for log %d: %w",
					index+1,
					logEntry.ID,
					err,
				)
			}
		}
	}
	return nil
}

// GetLogInboundRequest returns the original accepted client request for a log.
func (s *SQLStore) GetLogInboundRequest(
	ctx context.Context,
	logID int64,
) (*model.RequestLogEntry, error) {
	var entry model.RequestLogEntry
	var capturedAt int64
	err := s.QueryRowContext(ctx, `
		SELECT log_id, captured_at, transport, method, url, headers, body
		FROM log_inbound_requests
		WHERE log_id = ?
	`, logID).Scan(
		&entry.LogID,
		&capturedAt,
		&entry.Transport,
		&entry.Method,
		&entry.URL,
		&entry.Headers,
		&entry.Body,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get inbound request for log %d: %w", logID, err)
	}
	entry.CapturedAt = model.JSONTime{Time: time.UnixMilli(capturedAt)}
	return &entry, nil
}

// GetLogUpstreamRequests returns selected upstream wire requests in send order.
func (s *SQLStore) GetLogUpstreamRequests(
	ctx context.Context,
	logID int64,
) ([]*model.RequestLogEntry, error) {
	rows, err := s.QueryContext(ctx, `
		SELECT log_id, sequence, captured_at, transport, method, url, headers, body
		FROM log_upstream_requests
		WHERE log_id = ?
		ORDER BY sequence
	`, logID)
	if err != nil {
		return nil, fmt.Errorf("get upstream requests for log %d: %w", logID, err)
	}
	defer func() { _ = rows.Close() }()

	entries := make([]*model.RequestLogEntry, 0)
	for rows.Next() {
		entry := &model.RequestLogEntry{}
		var capturedAt int64
		if err := rows.Scan(
			&entry.LogID,
			&entry.Sequence,
			&capturedAt,
			&entry.Transport,
			&entry.Method,
			&entry.URL,
			&entry.Headers,
			&entry.Body,
		); err != nil {
			return nil, fmt.Errorf("scan upstream request for log %d: %w", logID, err)
		}
		entry.CapturedAt = model.JSONTime{Time: time.UnixMilli(capturedAt)}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate upstream requests for log %d: %w", logID, err)
	}
	return entries, nil
}
