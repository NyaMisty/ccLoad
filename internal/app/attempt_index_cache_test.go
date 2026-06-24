package app

import "testing"

// TestAttemptIndexCache_MaxIdxFallback 无 override 时回退 maxIdx 派生：
// 成功响应 / 单渠道失败场景，同链 attempt_index 最大者才是终。
func TestAttemptIndexCache_MaxIdxFallback(t *testing.T) {
	c := newAttemptIndexCache(8)
	// reqID=100：上游尝试 attempt_index 1,2,3（如多 key/URL 重试）
	c.record(101, 100, 1)
	c.record(102, 100, 2)
	c.record(103, 100, 3)

	for id, wantFinal := range map[int64]bool{101: false, 102: false, 103: true} {
		_, isFinal, ok := c.lookup(id)
		if !ok {
			t.Fatalf("lookup(%d) 期望 ok=true", id)
		}
		if isFinal != wantFinal {
			t.Fatalf("lookup(%d) 期望 isFinal=%v, 实际=%v", id, wantFinal, isFinal)
		}
	}
}

// TestAttemptIndexCache_FinalOverride 多渠道全失败：汇总日志 override 后成为唯一终，
// 同时撤销 maxIdx 落在最后一条上游失败日志上的错标。
func TestAttemptIndexCache_FinalOverride(t *testing.T) {
	c := newAttemptIndexCache(8)
	// reqID=200：上游失败 attempt_index 1,2
	c.record(201, 200, 1)
	c.record(202, 200, 2)
	// override 前：maxIdx=2 落在 202，会被错标为终（建立基线）
	if _, isFinal, _ := c.lookup(202); !isFinal {
		t.Fatalf("override 前 202 应是 maxIdx 终（基线）")
	}
	// 汇总日志（真正返回 client）写入并标记 override
	c.recordFinal(203, 200)

	// 汇总日志 203 = 终
	if _, isFinal, ok := c.lookup(203); !ok || !isFinal {
		t.Fatalf("汇总日志 203 期望 isFinal=true ok=true, 实际 isFinal=%v ok=%v", isFinal, ok)
	}
	// 上游失败日志被撤销，不再是终
	for _, id := range []int64{201, 202} {
		if _, isFinal, _ := c.lookup(id); isFinal {
			t.Fatalf("override 后 %d 不应是终（撤销错标）", id)
		}
	}
}

// TestAttemptIndexCache_FinalOverrideNoUpstream no available upstream：reqID 从无上游日志，
// 直接写汇总并 override，查询时仍能正确判为终。
func TestAttemptIndexCache_FinalOverrideNoUpstream(t *testing.T) {
	c := newAttemptIndexCache(8)
	// reqID=300：无任何上游日志，直接 503 兜底
	c.recordFinal(301, 300)
	if _, isFinal, ok := c.lookup(301); !ok || !isFinal {
		t.Fatalf("no-upstream 汇总日志 301 期望 isFinal=true ok=true, 实际=%v %v", isFinal, ok)
	}
}
