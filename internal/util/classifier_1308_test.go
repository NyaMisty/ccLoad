package util

import (
	"testing"
	"time"
)

func TestParseGLMErrorCooldown(t *testing.T) {
	// 固定 now，便于验证相对冷却(retry-after / 兜底)
	now := time.Date(2026, 6, 24, 15, 0, 0, 0, time.Local)

	tests := []struct {
		name          string
		responseBody  string
		headers       map[string][]string // HTTP 响应头（测试 Retry-After）
		expectOK      bool
		expectLevel   ErrorLevel
		expectReason  string        // GLM 码
		expectAbsTime string        // 绝对重置时间(配额类)，格式 "2006-01-02 15:04:05"；为空则校验 expectOffset
		expectOffset  time.Duration // 相对冷却，配合 now 校验
	}{
		// 配额类 1308：绝对时间优先（type 字段即码）
		{
			name:          "1308配额超限-type字段绝对时间",
			responseBody:  `{"type":"error","error":{"type":"1308","message":"已达到 5 小时的使用上限。您的限额将在 2025-12-09 18:08:11 重置。"},"request_id":"20251209155304a15e2cfd9ae44ae8"}`,
			expectOK:      true,
			expectLevel:   ErrorLevelKey,
			expectReason:  "1308",
			expectAbsTime: "2025-12-09 18:08:11",
		},
		{
			name:          "1308使用code字段-绝对时间",
			responseBody:  `{"error":{"code":"1308","message":"已达到上限。您的限额将在 2025-12-21 15:00:05 重置。"},"request_id":"x"}`,
			expectOK:      true,
			expectLevel:   ErrorLevelKey,
			expectReason:  "1308",
			expectAbsTime: "2025-12-21 15:00:05",
		},
		{
			name:          "1310周月度-绝对时间",
			responseBody:  `{"error":{"code":"1310","message":"Weekly/Monthly Limit Exhausted. Your limit will reset at 2026-04-20 15:24:20"},"request_id":"..."}`,
			expectOK:      true,
			expectLevel:   ErrorLevelKey,
			expectReason:  "1310",
			expectAbsTime: "2026-04-20 15:24:20",
		},
		// 配额类但无绝对时间 → 降级 retry-after(无) > 10s 兜底
		{
			name:         "1308无时间-兜底10s",
			responseBody: `{"error":{"type":"1308","message":"错误信息但没有时间"},"request_id":"xxx"}`,
			expectOK:     true,
			expectLevel:  ErrorLevelKey,
			expectReason: "1308",
			expectOffset: 10 * time.Second,
		},
		{
			name:         "1310无时间-兜底10s",
			responseBody: `{"error":{"code":"1310","message":"Weekly/Monthly Limit Exhausted"}}`,
			expectOK:     true,
			expectLevel:  ErrorLevelKey,
			expectReason: "1310",
			expectOffset: 10 * time.Second,
		},

		// 限流类 1302：码在 code 字段、type 是 rate_limit_error
		{
			name:          "1302速率限制-body顶层retry-after",
			responseBody:  `{"type":"error","error":{"type":"rate_limit_error","code":"1302","message":"[1302][您的账户已达到速率限制]"},"request_id":"20260624145731442e21731d414015","retry-after":"40"}`,
			expectOK:      true,
			expectLevel:   ErrorLevelKey,
			expectReason:  "1302",
			expectOffset:  40 * time.Second,
		},
		{
			name:          "1302速率限制-HTTP header Retry-After(429标准位置)",
			responseBody:  `{"type":"error","error":{"type":"rate_limit_error","code":"1302","message":"[1302][您的账户已达到速率限制]"}}`,
			headers:       map[string][]string{"Retry-After": {"40"}},
			expectOK:      true,
			expectLevel:   ErrorLevelKey,
			expectReason:  "1302",
			expectOffset:  40 * time.Second,
		},
		{
			name:          "1302-header优先于body字段",
			responseBody:  `{"error":{"code":"1302","message":"x","retry-after":"99"}}`,
			headers:       map[string][]string{"Retry-After": {"25"}},
			expectOK:      true,
			expectLevel:   ErrorLevelKey,
			expectReason:  "1302",
			expectOffset:  25 * time.Second,
		},
		{
			name:          "1302-小写retry-after头",
			responseBody:  `{"error":{"code":"1302","message":"x"}}`,
			headers:       map[string][]string{"retry-after": {"18"}},
			expectOK:      true,
			expectLevel:   ErrorLevelKey,
			expectReason:  "1302",
			expectOffset:  18 * time.Second,
		},
		{
			name:          "1302-retry-after在error对象内",
			responseBody:  `{"error":{"code":"1302","message":"x","retry-after":"15"}}`,
			expectOK:      true,
			expectLevel:   ErrorLevelKey,
			expectReason:  "1302",
			expectOffset:  15 * time.Second,
		},
		{
			name:         "1302无retry-after-兜底3s",
			responseBody: `{"type":"error","error":{"type":"rate_limit_error","code":"1302","message":"[1302][您的账户已达到速率限制]"}}`,
			expectOK:     true,
			expectLevel:  ErrorLevelKey,
			expectReason: "1302",
			expectOffset: 3 * time.Second,
		},
		// 限流类 1313：码在 code 字段、type 是 api_error
		{
			name:         "1313公平使用策略-兜底3s",
			responseBody: `{"type":"error","error":{"type":"api_error","code":"1313","message":"[1313][您的账户当前使用模式不符合公平使用策略]"},"request_id":"202606241456268d419043239a420a"}`,
			expectOK:     true,
			expectLevel:  ErrorLevelKey,
			expectReason: "1313",
			expectOffset: 3 * time.Second,
		},

		// 服务类 1305/1312：渠道级
		{
			name:          "1305服务错误-渠道级retry-after",
			responseBody:  `{"error":{"code":"1305","message":"服务错误"},"retry-after":"20"}`,
			expectOK:      true,
			expectLevel:   ErrorLevelChannel,
			expectReason:  "1305",
			expectOffset:  20 * time.Second,
		},
		{
			name:         "1305过载-渠道级兜底3s",
			responseBody: `{"error":{"code":"1305","message":"该模型当前访问量过大"}}`,
			expectOK:     true,
			expectLevel:  ErrorLevelChannel,
			expectReason: "1305",
			expectOffset: 3 * time.Second,
		},
		{
			name:         "1312过载-渠道级兜底10s",
			responseBody: `{"error":{"code":"1312","message":"overloaded"}}`,
			expectOK:     true,
			expectLevel:  ErrorLevelChannel,
			expectReason: "1312",
			expectOffset: 10 * time.Second,
		},

		// 非 GLM 码 / 格式错误
		{
			name:         "非GLM码1307",
			responseBody: `{"error":{"type":"1307","message":"其他错误"},"request_id":"xxx"}`,
			expectOK:     false,
		},
		{
			name:         "格式错误的JSON",
			responseBody: `{invalid json}`,
			expectOK:     false,
		},
		{
			name:         "非GLM的rate_limit_error",
			responseBody: `{"error":{"type":"rate_limit_error","message":"其他限流"}}`,
			expectOK:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			until, level, reason, ok := ParseGLMErrorCooldown([]byte(tt.responseBody), tt.headers, now)

			if ok != tt.expectOK {
				t.Fatalf("ok = %v, want %v", ok, tt.expectOK)
			}
			if !ok {
				return
			}

			if level != tt.expectLevel {
				t.Errorf("level = %v, want %v", level, tt.expectLevel)
			}
			if reason != tt.expectReason {
				t.Errorf("reason = %q, want %q", reason, tt.expectReason)
			}

			if tt.expectAbsTime != "" {
				expected, err := time.ParseInLocation("2006-01-02 15:04:05", tt.expectAbsTime, time.Local)
				if err != nil {
					t.Fatalf("测试用例时间格式错误: %v", err)
				}
				if !until.Equal(expected) {
					t.Errorf("until = %v, want %v",
						until.Format("2006-01-02 15:04:05"), tt.expectAbsTime)
				}
			} else {
				expected := now.Add(tt.expectOffset)
				if !until.Equal(expected) {
					t.Errorf("until = %v, want %v (now+%v)",
						until.Format(time.RFC3339), expected.Format(time.RFC3339), tt.expectOffset)
				}
			}
		})
	}
}

// 测试配额类绝对重置时间使用本地时区
func TestParseGLMErrorCooldown_Timezone(t *testing.T) {
	responseBody := `{"type":"error","error":{"type":"1308","message":"您的限额将在 2025-12-09 18:08:11 重置。"},"request_id":"xxx"}`

	resetTime, _, _, ok := ParseGLMErrorCooldown([]byte(responseBody), nil, time.Now())
	if !ok {
		t.Fatal("解析失败")
	}

	if resetTime.Location() != time.Local {
		t.Errorf("时区不是Local: %v", resetTime.Location())
	}
}

// 测试边界情况: message 中包含多个时间，匹配第一个
func TestParseGLMErrorCooldown_MultipleOccurrences(t *testing.T) {
	responseBody := `{"type":"error","error":{"type":"1308","message":"您之前将在某时 2025-01-01 00:00:00，现在的限额将在 2025-12-09 18:08:11 重置。"},"request_id":"xxx"}`

	resetTime, _, _, ok := ParseGLMErrorCooldown([]byte(responseBody), nil, time.Now())
	if !ok {
		t.Fatal("解析失败")
	}

	// resetTime1308Regex 匹配第一个出现的时间
	expectedTime, _ := time.ParseInLocation("2006-01-02 15:04:05", "2025-01-01 00:00:00", time.Local)
	if !resetTime.Equal(expectedTime) {
		t.Errorf("时间匹配错误: got %v, want %v", resetTime, expectedTime)
	}
}

// 验证主分类路径对 HTTP 429 + 1302(body 含 retry-after)产出固定 Key 级冷却
func TestClassifyHTTPResponseWithMeta_GLM1302UsesRetryAfter(t *testing.T) {
	body := []byte(`{"type":"error","error":{"type":"rate_limit_error","code":"1302","message":"[1302][您的账户已达到速率限制]"},"request_id":"x","retry-after":"40"}`)

	before := time.Now()
	got := ClassifyHTTPResponseWithMeta(429, nil, body)
	after := time.Now()

	if got.Level != ErrorLevelKey {
		t.Fatalf("Level=%v, want ErrorLevelKey", got.Level)
	}
	if !got.HasKeyCooldownUntil {
		t.Fatal("expected fixed key cooldown until (retry-after=40s)")
	}
	if got.KeyCooldownReason != "1302" {
		t.Fatalf("KeyCooldownReason=%q, want 1302", got.KeyCooldownReason)
	}

	minUntil := before.Add(40*time.Second - 2*time.Second)
	maxUntil := after.Add(40*time.Second + 2*time.Second)
	if got.KeyCooldownUntil.Before(minUntil) || got.KeyCooldownUntil.After(maxUntil) {
		t.Fatalf("KeyCooldownUntil=%s, want ~40s window between %s and %s",
			got.KeyCooldownUntil.Format(time.RFC3339),
			minUntil.Format(time.RFC3339),
			maxUntil.Format(time.RFC3339))
	}
}

// 验证主分类路径对 HTTP 429 + Retry-After 头 产出固定冷却（header 是 429 的标准位置）
func TestClassifyHTTPResponseWithMeta_GLM1302UsesHeaderRetryAfter(t *testing.T) {
	body := []byte(`{"type":"error","error":{"type":"rate_limit_error","code":"1302","message":"[1302][您的账户已达到速率限制]"}}`)
	headers := map[string][]string{"Retry-After": {"40"}}

	before := time.Now()
	got := classifyHTTPResponseWithMetaAt(429, headers, body, time.Now())
	after := time.Now()

	if got.Level != ErrorLevelKey {
		t.Fatalf("Level=%v, want ErrorLevelKey", got.Level)
	}
	if !got.HasKeyCooldownUntil {
		t.Fatal("expected fixed key cooldown until (header Retry-After=40s)")
	}
	if got.KeyCooldownReason != "1302" {
		t.Fatalf("KeyCooldownReason=%q, want 1302", got.KeyCooldownReason)
	}

	minUntil := before.Add(40*time.Second - 2*time.Second)
	maxUntil := after.Add(40*time.Second + 2*time.Second)
	if got.KeyCooldownUntil.Before(minUntil) || got.KeyCooldownUntil.After(maxUntil) {
		t.Fatalf("KeyCooldownUntil=%s, want ~40s window between %s and %s",
			got.KeyCooldownUntil.Format(time.RFC3339),
			minUntil.Format(time.RFC3339),
			maxUntil.Format(time.RFC3339))
	}
}

func TestClassifyHTTPResponseWithMeta_ModelCooldownUsesResetSeconds(t *testing.T) {
	body := []byte(`{"error":{"code":"model_cooldown","message":"All credentials for model gpt-5.5 are cooling down via provider codex","model":"gpt-5.5","provider":"codex","reset_seconds":13792,"reset_time":"3h49m51s"}}`)

	before := time.Now()
	got := ClassifyHTTPResponseWithMeta(429, nil, body)
	after := time.Now()

	if got.Level != ErrorLevelKey {
		t.Fatalf("Level=%v, want ErrorLevelKey", got.Level)
	}
	if !got.HasKeyCooldownUntil {
		t.Fatal("expected fixed key cooldown until")
	}
	if got.KeyCooldownReason != "model_cooldown" {
		t.Fatalf("KeyCooldownReason=%q, want model_cooldown", got.KeyCooldownReason)
	}

	minUntil := before.Add(13792*time.Second - 2*time.Second)
	maxUntil := after.Add(13792*time.Second + 2*time.Second)
	if got.KeyCooldownUntil.Before(minUntil) || got.KeyCooldownUntil.After(maxUntil) {
		t.Fatalf("KeyCooldownUntil=%s, want between %s and %s",
			got.KeyCooldownUntil.Format(time.RFC3339),
			minUntil.Format(time.RFC3339),
			maxUntil.Format(time.RFC3339))
	}
}

func TestClassifyHTTPResponseWithMeta_GeminiResourceExhaustedUsesRetryIn(t *testing.T) {
	body := []byte(`{"error":{"code":429,"message":"You exceeded your current quota, please check your plan and billing details. For more information on this error, head to: https://ai.google.dev/gemini-api/docs/rate-limits. To monitor your current usage, head to: https://ai.dev/rate-limit. \n* Quota exceeded for metric: generativelanguage.googleapis.com/generate_content_free_tier_requests, limit: 20, model: gemini-3.5-flash\nPlease retry in 59.409754061s.","status":"RESOURCE_EXHAUSTED"}}`)

	before := time.Now()
	got := ClassifyHTTPResponseWithMeta(429, nil, body)
	after := time.Now()

	if got.Level != ErrorLevelKey {
		t.Fatalf("Level=%v, want ErrorLevelKey", got.Level)
	}
	if !got.HasKeyCooldownUntil {
		t.Fatal("expected fixed key cooldown until")
	}
	if got.KeyCooldownReason != "RESOURCE_EXHAUSTED_RETRY_IN" {
		t.Fatalf("KeyCooldownReason=%q, want RESOURCE_EXHAUSTED_RETRY_IN", got.KeyCooldownReason)
	}

	retryAfter := 59*time.Second + 409754061*time.Nanosecond
	minUntil := before.Add(retryAfter - 2*time.Second)
	maxUntil := after.Add(retryAfter + 2*time.Second)
	if got.KeyCooldownUntil.Before(minUntil) || got.KeyCooldownUntil.After(maxUntil) {
		t.Fatalf("KeyCooldownUntil=%s, want between %s and %s",
			got.KeyCooldownUntil.Format(time.RFC3339Nano),
			minUntil.Format(time.RFC3339Nano),
			maxUntil.Format(time.RFC3339Nano))
	}
}

func TestClassifyHTTPResponseWithMeta_GlobalFixedWindowQuotaUsesRetryClock(t *testing.T) {
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	now := time.Date(2026, 6, 17, 11, 30, 0, 0, loc)
	body := []byte(`{"error":{"message":"当前公益站使用人数较多，本时段全站额度已用完，请在 今天 12:00 后再试。（traceid: 29038189-54e3-472e-b821-e7a5ebef3795）","type":"rate_limit_error","param":null,"code":"global_fixed_window_quota_exhausted","trace_id":"29038189-54e3-472e-b821-e7a5ebef3795"}}`)

	got := classifyHTTPResponseWithMetaAt(429, nil, body, now)

	if got.Level != ErrorLevelChannel {
		t.Fatalf("Level=%v, want ErrorLevelChannel", got.Level)
	}
	if got.HasKeyCooldownUntil {
		t.Fatal("global fixed-window quota must not be represented as key cooldown")
	}
	if !got.HasChannelCooldownUntil {
		t.Fatal("expected fixed channel cooldown until")
	}
	if got.ChannelCooldownReason != "GLOBAL_FIXED_WINDOW_QUOTA_EXHAUSTED" {
		t.Fatalf("ChannelCooldownReason=%q, want GLOBAL_FIXED_WINDOW_QUOTA_EXHAUSTED", got.ChannelCooldownReason)
	}

	want := time.Date(2026, 6, 17, 12, 0, 0, 0, loc)
	if !got.ChannelCooldownUntil.Equal(want) {
		t.Fatalf("ChannelCooldownUntil=%s, want %s",
			got.ChannelCooldownUntil.Format(time.RFC3339),
			want.Format(time.RFC3339))
	}
}
