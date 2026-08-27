package app

import "testing"

func TestAttemptIndexCacheUsesMaximumAttemptAsFallback(t *testing.T) {
	t.Parallel()

	cache := newAttemptIndexCache(8)
	cache.record(101, 100, 1)
	cache.record(102, 100, 2)
	cache.record(103, 100, 3)

	for logID, wantFinal := range map[int64]bool{101: false, 102: false, 103: true} {
		index, isFinal, ok := cache.lookup(logID)
		if !ok {
			t.Fatalf("lookup(%d) not found", logID)
		}
		if index != int32(logID-100) {
			t.Fatalf("lookup(%d) index=%d", logID, index)
		}
		if isFinal != wantFinal {
			t.Fatalf("lookup(%d) isFinal=%v, want %v", logID, isFinal, wantFinal)
		}
	}
}

func TestAttemptIndexCacheFinalOverrideReplacesMaximumAttempt(t *testing.T) {
	t.Parallel()

	cache := newAttemptIndexCache(8)
	cache.record(201, 200, 1)
	cache.record(202, 200, 2)
	cache.recordFinal(203, 200)

	if _, isFinal, ok := cache.lookup(203); !ok || !isFinal {
		t.Fatalf("final override lookup=(%v,%v), want found final", isFinal, ok)
	}
	for _, logID := range []int64{201, 202} {
		if _, isFinal, _ := cache.lookup(logID); isFinal {
			t.Fatalf("log %d must not remain final after override", logID)
		}
	}

	cache.recordFinal(204, 200)
	if _, isFinal, _ := cache.lookup(203); isFinal {
		t.Fatal("older final override must be replaced")
	}
	if _, isFinal, ok := cache.lookup(204); !ok || !isFinal {
		t.Fatal("latest final override must be final")
	}
}

func TestAttemptIndexCacheSupportsStandaloneFinalLog(t *testing.T) {
	t.Parallel()

	cache := newAttemptIndexCache(8)
	cache.recordFinal(301, 0)

	index, isFinal, ok := cache.lookup(301)
	if !ok || !isFinal || index != 0 {
		t.Fatalf("lookup=(%d,%v,%v), want (0,true,true)", index, isFinal, ok)
	}
}

func TestAttemptIndexCacheEvictsOldestLogs(t *testing.T) {
	t.Parallel()

	cache := newAttemptIndexCache(2)
	cache.record(1, 10, 1)
	cache.record(2, 10, 2)
	cache.record(3, 10, 3)

	if _, _, ok := cache.lookup(1); ok {
		t.Fatal("oldest log was not evicted")
	}
	if _, _, ok := cache.lookup(2); !ok {
		t.Fatal("newer log was unexpectedly evicted")
	}
}
