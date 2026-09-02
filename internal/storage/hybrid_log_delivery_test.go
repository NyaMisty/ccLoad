package storage

import (
	"context"
	"testing"
	"time"

	"ccLoad/internal/model"
)

func TestHybridStoreRetainsEveryPendingPrimaryLogBatch(t *testing.T) {
	ctx := context.Background()
	sqlite := createTestSQLiteStore(t)
	primary := createTestSQLiteStore(t)
	releaseInitialization := make(chan struct{})
	hybrid := newHybridStore(sqlite, primary, func(initCtx context.Context) error {
		select {
		case <-releaseInitialization:
			return nil
		case <-initCtx.Done():
			return initCtx.Err()
		}
	})
	t.Cleanup(func() {
		select {
		case <-releaseInitialization:
		default:
			close(releaseInitialization)
		}
		_ = hybrid.Close()
	})

	for _, message := range []string{"first-batch", "second-batch"} {
		if err := hybrid.BatchAddLogs(ctx, []*model.LogEntry{{
			Time:       model.JSONTime{Time: time.Now()},
			StatusCode: 200,
			Message:    message,
		}}); err != nil {
			t.Fatalf("add %s: %v", message, err)
		}
	}

	close(releaseInitialization)
	waitForCondition(t, 3*time.Second, func() bool {
		logs, err := primary.ListLogs(ctx, time.Time{}, 10, 0, nil)
		return err == nil && len(logs) == 2 && hybrid.RuntimeMetrics().PrimarySyncPending == 0
	})

	logs, err := primary.ListLogs(ctx, time.Time{}, 10, 0, nil)
	if err != nil {
		t.Fatalf("list primary logs: %v", err)
	}
	messages := map[string]bool{}
	for _, entry := range logs {
		messages[entry.Message] = true
	}
	if !messages["first-batch"] || !messages["second-batch"] {
		t.Fatalf("primary logs=%+v", logs)
	}
	if dropped := hybrid.RuntimeMetrics().PrimarySyncDropped; dropped != 0 {
		t.Fatalf("primary sync dropped=%d, want 0", dropped)
	}
}
