package app

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"ccLoad/internal/model"

	"github.com/gin-gonic/gin"
)

// newPrioritySchedulerTestServer 构造仅含并发调度所需字段的 Server。
func newPrioritySchedulerTestServer(t testing.TB, capacity int) *Server {
	return &Server{
		concurrencySem: make(chan struct{}, capacity),
		maxConcurrency: capacity,
		authService:    newTestAuthService(t),
	}
}

// TestAcquireConcurrencySlotForContext_HighPriorityWins 验证槽位竞争时高优先级令牌先获得槽位。
func TestAcquireConcurrencySlotForContext_HighPriorityWins(t *testing.T) {
	srv := newPrioritySchedulerTestServer(t, 1)
	ctx := context.Background()

	// 占满唯一槽位
	occupied, err := srv.acquireConcurrencySlotForContext(ctx, "")
	if err != nil {
		t.Fatalf("占用槽位失败: %v", err)
	}

	lowPriority := &model.AuthToken{Token: "low", Priority: 10}
	highPriority := &model.AuthToken{Token: "high", Priority: 200}
	srv.authService.authTokensMux.Lock()
	srv.authService.authTokenPriorities[lowPriority.Token] = lowPriority.EffectivePriority()
	srv.authService.authTokenPriorities[highPriority.Token] = highPriority.EffectivePriority()
	srv.authService.authTokensMux.Unlock()

	acquired := make(chan string, 2)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// 低优先级先入队，高优先级后入队；释放槽位后高优先级应先获得。
	go func() {
		release, err := srv.acquireConcurrencySlotForContext(waitCtx, lowPriority.Token)
		if err == nil {
			acquired <- "low"
			release()
		}
	}()
	go func() {
		release, err := srv.acquireConcurrencySlotForContext(waitCtx, highPriority.Token)
		if err == nil {
			acquired <- "high"
			release()
		}
	}()

	// 等待两个等待者都进入队列
	time.Sleep(50 * time.Millisecond)
	occupied()

	first := <-acquired
	if first != "high" {
		t.Fatalf("高优先级应先获得槽位，实际: %s", first)
	}
	second := <-acquired
	if second != "low" {
		t.Fatalf("低优先级应随后获得槽位，实际: %s", second)
	}
}

// TestAcquireConcurrencySlotForContext_SamePriorityFIFO 验证同优先级按入队顺序派发。
func TestAcquireConcurrencySlotForContext_SamePriorityFIFO(t *testing.T) {
	srv := newPrioritySchedulerTestServer(t, 1)
	ctx := context.Background()

	occupied, err := srv.acquireConcurrencySlotForContext(ctx, "")
	if err != nil {
		t.Fatalf("占用槽位失败: %v", err)
	}

	acquired := make(chan string, 2)
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	go func() {
		release, err := srv.acquireConcurrencySlotForContext(waitCtx, "first")
		if err == nil {
			acquired <- "first"
			release()
		}
	}()
	time.Sleep(20 * time.Millisecond) // 确保 first 先入队
	go func() {
		release, err := srv.acquireConcurrencySlotForContext(waitCtx, "second")
		if err == nil {
			acquired <- "second"
			release()
		}
	}()
	time.Sleep(20 * time.Millisecond)
	occupied()

	if first := <-acquired; first != "first" {
		t.Fatalf("同优先级应按 FIFO，先入队者先得，实际: %s", first)
	}
	if second := <-acquired; second != "second" {
		t.Fatalf("同优先级应按 FIFO，后入队者后得，实际: %s", second)
	}
}

// TestAcquireConcurrencySlotForContext_CancelWhileWaiting 验证等待者取消后不占用队列，
// 槽位正确派发给其余等待者。
func TestAcquireConcurrencySlotForContext_CancelWhileWaiting(t *testing.T) {
	srv := newPrioritySchedulerTestServer(t, 1)
	ctx := context.Background()

	occupied, err := srv.acquireConcurrencySlotForContext(ctx, "")
	if err != nil {
		t.Fatalf("占用槽位失败: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	go func() {
		// 低优先级等待者：入队后立刻取消
		_, err := srv.acquireConcurrencySlotForContext(cancelCtx, "")
		if err == nil {
			t.Error("已取消的等待不应获得槽位")
		}
	}()
	time.Sleep(20 * time.Millisecond) // 等待者入队
	cancel()
	time.Sleep(20 * time.Millisecond) // 取消处理完成

	acquired := make(chan struct{}, 1)
	go func() {
		release, err := srv.acquireConcurrencySlotForContext(ctx, "normal")
		if err != nil {
			t.Errorf("正常等待者应获得槽位: %v", err)
			return
		}
		acquired <- struct{}{}
		release()
	}()
	time.Sleep(20 * time.Millisecond)
	occupied()

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatal("取消的等待者不应阻塞槽位派发")
	}
}

// TestAcquireConcurrencySlotForContext_ConcurrentStress 验证大量并发获取/释放不泄漏槽位。
func TestAcquireConcurrencySlotForContext_ConcurrentStress(t *testing.T) {
	srv := newPrioritySchedulerTestServer(t, 8)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			release, err := srv.acquireConcurrencySlotForContext(ctx, "")
			if err != nil {
				t.Errorf("获取槽位失败: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
			release()
		}(i)
	}
	wg.Wait()

	if len(srv.concurrencySem) != 0 {
		t.Fatalf("全部释放后信号量应为空，实际: %d", len(srv.concurrencySem))
	}
	srv.concurrencyWaitMu.Lock()
	waiters := len(srv.concurrencyWaiters)
	srv.concurrencyWaitMu.Unlock()
	if waiters != 0 {
		t.Fatalf("全部完成后等待队列应为空，实际: %d", waiters)
	}
}

// TestTokenPriority_Defaults 验证未知/空令牌回退默认优先级。
func TestTokenPriority_Defaults(t *testing.T) {
	srv := newPrioritySchedulerTestServer(t, 1)
	auth := srv.authService

	if got := auth.TokenPriority(""); got != model.AuthTokenPriorityDefault {
		t.Fatalf("空哈希应返回默认优先级 %d，实际 %d", model.AuthTokenPriorityDefault, got)
	}
	if got := auth.TokenPriority("unknown"); got != model.AuthTokenPriorityDefault {
		t.Fatalf("未知令牌应返回默认优先级 %d，实际 %d", model.AuthTokenPriorityDefault, got)
	}

	auth.authTokensMux.Lock()
	auth.authTokenPriorities["vip"] = 500
	auth.authTokensMux.Unlock()
	if got := auth.TokenPriority("vip"); got != 500 {
		t.Fatalf("已配置令牌应返回配置优先级 500，实际 %d", got)
	}

	var nilAuth *AuthService
	if got := nilAuth.TokenPriority("vip"); got != model.AuthTokenPriorityDefault {
		t.Fatalf("nil 服务应返回默认优先级，实际 %d", got)
	}
}

// TestAuthTokenEffectivePriority 验证 0/负数优先级归一为默认值。
func TestAuthTokenEffectivePriority(t *testing.T) {
	tests := []struct {
		name     string
		priority int
		want     int
	}{
		{"zero defaults to 100", 0, model.AuthTokenPriorityDefault},
		{"negative defaults to 100", -5, model.AuthTokenPriorityDefault},
		{"positive kept", 250, 250},
		{"one kept", 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := &model.AuthToken{Priority: tt.priority}
			if got := token.EffectivePriority(); got != tt.want {
				t.Fatalf("EffectivePriority()=%d, want %d", got, tt.want)
			}
		})
	}
}

// TestAdminAPI_AuthTokenPriorityCRUD 验证优先级经创建/更新/序列化全链路不丢失。
func TestAdminAPI_AuthTokenPriorityCRUD(t *testing.T) {
	server := newInMemoryServer(t)

	// 创建：显式 priority=200
	c, w := newTestContext(t, newJSONRequest(t, http.MethodPost, "/admin/auth-tokens", map[string]any{
		"description": "vip token",
		"priority":    200,
	}))
	server.HandleCreateAuthToken(c)
	if w.Code != http.StatusOK {
		t.Fatalf("create status=%d, body=%s", w.Code, w.Body.String())
	}
	var created struct {
		Success bool `json:"success"`
		Data    struct {
			ID       int64 `json:"id"`
			Token    string `json:"token"`
			Priority int   `json:"priority"`
		} `json:"data"`
	}
	mustUnmarshalJSON(t, w.Body.Bytes(), &created)
	if !created.Success {
		t.Fatal("create failed")
	}
	if created.Data.Priority != 200 {
		t.Fatalf("create priority=%d, want 200", created.Data.Priority)
	}

	tokenHash := model.HashToken(created.Data.Token)
	if got := server.authService.TokenPriority(tokenHash); got != 200 {
		t.Fatalf("TokenPriority after create=%d, want 200（热更新应同步优先级）", got)
	}

	// 更新：改为 50
	c, w = newTestContext(t, newJSONRequest(t, http.MethodPut,
		"/admin/auth-tokens/"+strconv.FormatInt(created.Data.ID, 10), map[string]any{
			"priority": 50,
		}))
	c.Params = gin.Params{{Key: "id", Value: strconv.FormatInt(created.Data.ID, 10)}}
	server.HandleUpdateAuthToken(c)
	if w.Code != http.StatusOK {
		t.Fatalf("update status=%d, body=%s", w.Code, w.Body.String())
	}
	var updated struct {
		Success bool `json:"success"`
		Data    struct {
			Priority int `json:"priority"`
		} `json:"data"`
	}
	mustUnmarshalJSON(t, w.Body.Bytes(), &updated)
	if updated.Data.Priority != 50 {
		t.Fatalf("update priority=%d, want 50", updated.Data.Priority)
	}
	if got := server.authService.TokenPriority(tokenHash); got != 50 {
		t.Fatalf("TokenPriority after update=%d, want 50", got)
	}

	// 创建缺省 priority 的令牌：默认 100
	c, w = newTestContext(t, newJSONRequest(t, http.MethodPost, "/admin/auth-tokens", map[string]any{
		"description": "default token",
	}))
	server.HandleCreateAuthToken(c)
	if w.Code != http.StatusOK {
		t.Fatalf("create default status=%d, body=%s", w.Code, w.Body.String())
	}
	var createdDefault struct {
		Success bool `json:"success"`
		Data    struct {
			Priority int `json:"priority"`
		} `json:"data"`
	}
	mustUnmarshalJSON(t, w.Body.Bytes(), &createdDefault)
	if createdDefault.Data.Priority != model.AuthTokenPriorityDefault {
		t.Fatalf("default priority=%d, want %d", createdDefault.Data.Priority, model.AuthTokenPriorityDefault)
	}

	// 非法负数：400
	c, w = newTestContext(t, newJSONRequest(t, http.MethodPost, "/admin/auth-tokens", map[string]any{
		"description": "bad token",
		"priority":    -1,
	}))
	server.HandleCreateAuthToken(c)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("negative priority status=%d, want 400", w.Code)
	}
}
