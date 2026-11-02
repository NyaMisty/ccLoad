package app

import (
	"context"
	"net/http"
	"testing"
	"time"

	"ccLoad/internal/model"
)

// TestSlotSchedulerStats_ExposesHoldsAndKeys 验证 runtime-metrics 快照：
// HELD hold 以 slots_held=1 计入 held_slots；ARMED 不计；渠道 Key 的
// active/limit/waiters 如实反映 limiter 状态。
func TestSlotSchedulerStats_ExposesHoldsAndKeys(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	const ch = int64(1)

	block, _, _, _, ok := limiter.acquire(ch, "key-a", 2, model.AuthTokenPriorityDefault)
	if !ok {
		t.Fatal("pre-acquire failed")
	}
	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{{ID: ch, Name: "a", MaxConcurrency: 2}}, reqCtx,
		map[int64]string{ch: "key-a"})
	block()

	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost never seated")
	}

	stats := scheduler.stats()
	if len(stats.Holds) != 1 {
		t.Fatalf("holds=%d, want 1", len(stats.Holds))
	}
	hold := stats.Holds[0]
	if hold.Tier != 200 || hold.State != "held" || hold.ChannelID != ch {
		t.Fatalf("hold stats=%+v, want tier=200 state=held channel=%d", hold, ch)
	}
	if hold.SlotsHeld != 1 || stats.HeldSlots != 1 {
		t.Fatalf("slots_held=%d held_slots=%d, want 1/1", hold.SlotsHeld, stats.HeldSlots)
	}
	if hold.ExpiresInMs <= 0 || hold.ExpiresInMs > int64(scheduler.holdWindow().Milliseconds()) {
		t.Fatalf("expires_in_ms=%d, want (0, %d]", hold.ExpiresInMs, scheduler.holdWindow().Milliseconds())
	}

	// 渠道 Key 占用：hold 的 +1 与 keyLimits 观测值
	found := false
	for _, key := range stats.ChannelKeys {
		if key.ChannelID != ch {
			continue
		}
		found = true
		if key.Active < 1 || key.Limit != 2 {
			t.Fatalf("key stats=%+v, want active≥1 limit=2", key)
		}
		if len(key.Key) != 8 {
			t.Fatalf("key=%q, want 8-hex-char hash prefix", key.Key)
		}
	}
	if !found {
		t.Fatalf("channel %d missing from key stats: %+v", ch, stats.ChannelKeys)
	}
}

// TestSlotSchedulerStats_ArmedHoldsZeroSlots 验证 ARMED（幽灵排队中）不计槽位。
func TestSlotSchedulerStats_ArmedHoldsZeroSlots(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	const ch = int64(1)

	// 渠道保持占满：幽灵永远排队中（ARMED）
	block, _, _, _, ok := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault)
	if !ok {
		t.Fatal("pre-acquire failed")
	}
	defer block()
	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{{ID: ch, Name: "a", MaxConcurrency: 1}}, reqCtx,
		map[int64]string{ch: "key-a"})

	deadline := time.Now().Add(2 * time.Second)
	for {
		stats := scheduler.stats()
		if len(stats.Holds) == 1 && stats.Holds[0].State == "armed" && stats.TotalWaiters >= 1 {
			if stats.Holds[0].SlotsHeld != 0 || stats.HeldSlots != 0 {
				t.Fatalf("armed hold slots: %+v held_slots=%d", stats.Holds[0], stats.HeldSlots)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("hold never reached armed state with a queued waiter: %+v", scheduler.stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSlotSchedulerStats_BorrowedState 验证 BORROWED 状态字符串与槽位计数。
func TestSlotSchedulerStats_BorrowedState(t *testing.T) {
	t.Parallel()

	scheduler, limiter := newSchedulerForTest()
	const ch = int64(1)

	block, _, _, _, ok := limiter.acquire(ch, "key-a", 1, model.AuthTokenPriorityDefault)
	if !ok {
		t.Fatal("pre-acquire failed")
	}
	reqCtx := channelSlotTestRequestContext("prio-token", 200)
	armHoldSaturated(scheduler, []*model.Config{{ID: ch, Name: "a", MaxConcurrency: 1}}, reqCtx,
		map[int64]string{ch: "key-a"})
	block()

	if _, seated := waitForHoldState(t, scheduler, 200, slotHoldHeld); !seated {
		t.Fatal("ghost never seated")
	}
	if token := scheduler.borrow(ch, 200, nil); token == nil {
		t.Fatal("borrow failed")
	}

	stats := scheduler.stats()
	if len(stats.Holds) != 1 || stats.Holds[0].State != "borrowed" {
		t.Fatalf("holds=%+v, want single borrowed", stats.Holds)
	}
	if stats.HeldSlots != 1 {
		t.Fatalf("held_slots=%d, want 1 (borrowed still holds the slot)", stats.HeldSlots)
	}
}

// TestRuntimeMetricsIncludesSlotScheduler 验证 HandleRuntimeMetrics 挂载
// slot_scheduler 节点（端到端契约）。
func TestRuntimeMetricsIncludesSlotScheduler(t *testing.T) {
	t.Parallel()

	env, preferred, _, release := setupChannelSlotEnv(t)
	defer release()
	injectTokenPriorityForTest(env.server.authService, "prio-token", 200)

	// 占住槽位让 keyLimits/active 有观测值
	c, w := newTestContext(t, newRequest(http.MethodGet, "/admin/runtime-metrics", nil))
	env.server.HandleRuntimeMetrics(c)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	resp := mustParseAPIResponse[map[string]any](t, w.Body.Bytes())
	snap, ok := resp.Data["slot_scheduler"].(map[string]any)
	if !ok {
		t.Fatalf("slot_scheduler metrics missing: %#v", resp.Data)
	}
	if _, ok := snap["holds"].([]any); !ok {
		t.Fatalf("slot_scheduler.holds missing: %#v", snap)
	}
	if _, ok := snap["channel_keys"].([]any); !ok {
		t.Fatalf("slot_scheduler.channel_keys missing: %#v", snap)
	}
	if _, ok := snap["held_slots"].(float64); !ok {
		t.Fatalf("slot_scheduler.held_slots missing: %#v", snap)
	}
	_ = preferred
	_ = context.Background()
}
