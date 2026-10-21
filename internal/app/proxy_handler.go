package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"ccLoad/internal/config"
	"ccLoad/internal/cooldown"
	"ccLoad/internal/model"
	"ccLoad/internal/protocol"
	"ccLoad/internal/util"

	"github.com/bytedance/sonic"
	"github.com/gin-gonic/gin"
)

var errUnknownClientProtocol = errors.New("unknown client protocol for path")

// errBodyTooLarge 与 errBodyReadTimeout 是两种完全不同的失败：前者是请求体超过
// 配置的大小上限（立即拒绝），后者是客户端在读取超时内没把请求体传完。二者的处置
// 手段不同（调大小上限 vs 调读取超时），错误文案必须区分，否则会被误判。
var errBodyTooLarge = errors.New("request body exceeds the configured size limit (settings: max_body_bytes / max_image_body_bytes)")
var errBodyReadTimeout = errors.New("timed out reading the request body before the client finished uploading (setting: http_read_timeout_seconds)")

// ErrAllKeysUnavailable 表示所有渠道密钥都不可用
var ErrAllKeysUnavailable = errors.New("all channel keys unavailable")

// ErrAllKeysExhausted 表示所有密钥都已耗尽
var ErrAllKeysExhausted = errors.New("all keys exhausted")

// ErrChannelRPMExceeded 表示渠道RPM限制已达到
var ErrChannelRPMExceeded = errors.New("channel rpm limit exceeded")

// ErrKeyConcurrencyExceeded 表示渠道内当前 API Key 的并发限制已达到
var ErrKeyConcurrencyExceeded = errors.New("api key concurrency limit exceeded")

// ============================================================================
// 并发控制
// ============================================================================

// acquireConcurrencySlot 获取并发槽位，返回release函数和状态
// ok=false 表示客户端已取消请求
func (s *Server) acquireConcurrencySlot(c *gin.Context) (release func(), ok bool) {
	tokenHash, _ := c.Get("token_hash")
	tokenHashStr, _ := tokenHash.(string)
	release, err := s.acquireConcurrencySlotForContext(c.Request.Context(), tokenHashStr)
	if err == nil {
		return release, true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": "request timeout while waiting for slot"})
		return nil, false
	}
	c.JSON(StatusClientClosedRequest, gin.H{"error": "request cancelled while waiting for slot"})
	return nil, false
}

// concurrencyWaiter 是全局并发槽位等待队列中的一项。
// ready 在获得槽位时关闭；abandoned 标记等待者已放弃（ctx 取消）。
type concurrencyWaiter struct {
	priority  int
	seq       uint64
	ready     chan struct{}
	granted   bool
	abandoned bool
}

// acquireConcurrencySlotForContext 获取全局并发槽位；槽位不足时按令牌优先级排队。
// tokenHash 为空（未认证路径）使用默认优先级；高优先级等待者先于低优先级获得槽位，
// 同优先级严格 FIFO。客户端取消时立即出队；若槽位已被授予则交还并重新派发。
func (s *Server) acquireConcurrencySlotForContext(ctx context.Context, tokenHash string) (func(), error) {
	priority := s.authService.TokenPriority(tokenHash)

	s.concurrencyWaitMu.Lock()
	select {
	case s.concurrencySem <- struct{}{}:
		s.concurrencyWaitMu.Unlock()
		return s.makeConcurrencyRelease(priority), nil
	default:
	}
	waiter := &concurrencyWaiter{
		priority: priority,
		seq:      s.concurrencyWaitSeq.Add(1),
		ready:    make(chan struct{}),
	}
	s.concurrencyWaiters = append(s.concurrencyWaiters, waiter)
	s.concurrencyWaitMu.Unlock()

	select {
	case <-waiter.ready:
		return s.makeConcurrencyRelease(waiter.priority), nil
	case <-ctx.Done():
		s.concurrencyWaitMu.Lock()
		granted := waiter.granted
		waiter.abandoned = true
		s.dropConcurrencyWaiterLocked(waiter)
		s.concurrencyWaitMu.Unlock()
		if granted {
			// 槽位在与取消竞争的瞬间已被授予：交还并重新派发给其他等待者。
			s.makeConcurrencyRelease(waiter.priority)()
		}
		return nil, ctx.Err()
	}
}

// makeConcurrencyRelease 返回槽位释放函数：归还信号量并立即把槽位派发给队首等待者。
func (s *Server) makeConcurrencyRelease(releasedPriority int) func() {
	return func() {
		s.concurrencyWaitMu.Lock()
		<-s.concurrencySem
		s.dispatchConcurrencySlotLocked()
		s.concurrencyWaitMu.Unlock()
	}
}

// dropConcurrencyWaiterLocked 从等待队列移除指定等待者（放弃或已授予后清理）。
func (s *Server) dropConcurrencyWaiterLocked(waiter *concurrencyWaiter) {
	for i, w := range s.concurrencyWaiters {
		if w == waiter {
			s.concurrencyWaiters = append(s.concurrencyWaiters[:i], s.concurrencyWaiters[i+1:]...)
			return
		}
	}
}

// dispatchConcurrencySlotLocked 把一个空闲槽位授予等待队列中优先级最高的等待者。
// 必须持 concurrencyWaitMu 调用；同优先级按入队顺序（seq）FIFO。
// 没有可派发的等待者时槽位留在信号量里，快路径可直接取用。
func (s *Server) dispatchConcurrencySlotLocked() {
	for len(s.concurrencyWaiters) > 0 {
		best := 0
		for i, w := range s.concurrencyWaiters {
			if w.abandoned || w.granted {
				continue
			}
			if s.concurrencyWaiters[best].abandoned || s.concurrencyWaiters[best].granted ||
				w.priority > s.concurrencyWaiters[best].priority ||
				(w.priority == s.concurrencyWaiters[best].priority && w.seq < s.concurrencyWaiters[best].seq) {
				best = i
			}
		}
		w := s.concurrencyWaiters[best]
		if w.abandoned || w.granted {
			// 全部等待者都已放弃/已授予：整体清理，槽位留在信号量。
			s.pruneConcurrencyWaitersLocked()
			return
		}
		s.concurrencyWaiters = append(s.concurrencyWaiters[:best], s.concurrencyWaiters[best+1:]...)
		w.granted = true
		s.concurrencySem <- struct{}{}
		close(w.ready)
		return
	}
}

// pruneConcurrencyWaitersLocked 清理队列中已放弃/已授予的残留项。
func (s *Server) pruneConcurrencyWaitersLocked() {
	kept := s.concurrencyWaiters[:0]
	for _, w := range s.concurrencyWaiters {
		if w.abandoned || w.granted {
			continue
		}
		kept = append(kept, w)
	}
	s.concurrencyWaiters = kept
}

// ============================================================================
// 请求解析
// ============================================================================

type incomingRequest struct {
	originalModel string
	body          []byte
	isStreaming   bool
	hasModel      bool
}

func (r incomingRequest) authorizationModel() string {
	if !r.hasModel {
		return ""
	}
	return r.originalModel
}

// isRequestReadTimeout 判断读取失败是否来自读取截止时间（HTTP Server ReadTimeout），
// 而不是客户端断开或其它 I/O 故障。
func isRequestReadTimeout(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func parseIncomingRequest(c *gin.Context, bodyLimits requestBodyLimits) (incomingRequest, error) {
	requestPath := c.Request.URL.Path
	requestMethod := c.Request.Method

	// 读取请求体（带上限，防止大包打爆内存）
	maxBody := bodyLimits.maxForPath(requestPath)
	limited := io.LimitReader(c.Request.Body, maxBody+1)
	all, err := io.ReadAll(limited)
	if err != nil {
		if isRequestReadTimeout(err) {
			return incomingRequest{}, fmt.Errorf("%w: %v", errBodyReadTimeout, err)
		}
		return incomingRequest{}, fmt.Errorf("failed to read body: %w", err)
	}
	_ = c.Request.Body.Close()
	if int64(len(all)) > maxBody {
		return incomingRequest{}, errBodyTooLarge
	}

	var reqModel struct {
		Model string `json:"model"`
	}
	_ = sonic.Unmarshal(all, &reqModel)

	// multipart/form-data 支持：当 JSON 解析无 model 时，尝试从 multipart 表单字段提取
	if reqModel.Model == "" {
		if ct := c.Request.Header.Get("Content-Type"); ct != "" {
			mediaType, params, _ := mime.ParseMediaType(ct)
			if mediaType == "multipart/form-data" {
				if boundary := params["boundary"]; boundary != "" {
					reqModel.Model = extractModelFromMultipart(all, boundary)
				}
			}
		}
	}

	// 智能检测流式请求
	isStreaming := isStreamingRequest(requestPath, all)

	// 多源模型名称获取：优先请求体，其次URL路径
	originalModel := reqModel.Model
	if originalModel == "" {
		originalModel = extractModelFromPath(requestPath)
	}
	hasModel := originalModel != ""
	requestFamily := protocol.DetectRequestFamily(requestPath)

	// GET 请求保留既有通配选路语义；Codex alpha/search 的业务模型保持为空。
	if originalModel == "" {
		switch {
		case requestFamily == protocol.RequestFamilyAlphaSearch:
		case requestMethod == http.MethodGet:
			originalModel = "*"
		default:
			return incomingRequest{}, fmt.Errorf("invalid JSON or missing model")
		}
	}

	return incomingRequest{
		originalModel: originalModel,
		body:          all,
		isStreaming:   isStreaming,
		hasModel:      hasModel,
	}, nil
}

// requestBodyLimits 是单个 Server 的不可变请求体上限。
type requestBodyLimits struct {
	standard int64
	images   int64
}

func normalizeMaxBodyBytes(maxBodyBytes int64) int64 {
	if maxBodyBytes <= 0 {
		return config.DefaultMaxBodyBytes
	}
	return maxBodyBytes
}

func newRequestBodyLimits(maxBody, maxImageBody int) requestBodyLimits {
	if maxBody <= 0 {
		maxBody = config.DefaultMaxBodyBytes
	}
	if maxImageBody <= 0 {
		maxImageBody = config.DefaultMaxImageBodyBytes
	}
	return requestBodyLimits{standard: int64(maxBody), images: int64(maxImageBody)}
}

func (l requestBodyLimits) maxForPath(requestPath string) int64 {
	if l.standard <= 0 {
		l.standard = config.DefaultMaxBodyBytes
	}
	if l.images <= 0 {
		l.images = config.DefaultMaxImageBodyBytes
	}
	if strings.HasPrefix(requestPath, "/v1/images/") {
		return l.images
	}
	return l.standard
}

// extractModelFromMultipart 从 multipart/form-data 原始字节中提取 model 字段
func extractModelFromMultipart(body []byte, boundary string) string {
	reader := multipart.NewReader(bytes.NewReader(body), boundary)
	for {
		part, err := reader.NextPart()
		if err != nil {
			break
		}
		if part.FormName() == "model" {
			val, err := io.ReadAll(io.LimitReader(part, 256))
			_ = part.Close()
			if err == nil {
				return strings.TrimSpace(string(val))
			}
			break
		}
		_ = part.Close()
	}
	return ""
}

// ============================================================================
// 路由选择
// ============================================================================

// selectRouteCandidates 根据请求选择路由候选
// 从proxy.go提取，遵循SRP原则
func (s *Server) selectRouteCandidates(ctx context.Context, c *gin.Context, originalModel string, clientProtocol string) ([]*model.Config, error) {
	requestMethod := c.Request.Method
	requestFamily := protocol.DetectRequestFamily(c.Request.URL.Path)

	tokenHash := ""
	if v, ok := c.Get("token_hash"); ok {
		tokenHash, _ = v.(string)
	}

	// 智能路由选择：根据请求类型选择不同的路由策略
	if requestMethod == http.MethodGet && clientProtocol == util.ProtocolGemini {
		// Gemini 模型列表请求仍可路由到任意启用渠道。
		return s.selectCandidatesByClientProtocol(ctx, util.ProtocolGemini, tokenHash)
	}

	if clientProtocol == "" {
		return nil, errUnknownClientProtocol
	}
	if requestFamily == protocol.RequestFamilyAlphaSearch {
		return s.selectAlphaSearchCandidates(ctx, originalModel, tokenHash)
	}

	return s.selectCandidatesByModelAndClientProtocol(ctx, originalModel, clientProtocol, tokenHash)
}

// routeCandidateWaitBudget 是「全部候选冷却/被兜底探测限频」时排队等待的总预算。
// 冷却到期通常是秒级窗口（模型冷却从 1min 起、兜底探测窗口 15s）；
// 10s 覆盖了大部分瞬时窗口，又不会把客户端挂住到网关自己的 read/write timeout 上。
const routeCandidateWaitBudget = 10 * time.Second

// routeCandidateWaitPoll 是等待期间的选路重试间隔。
// 500ms 足够小，能尽快感知冷却解除；足够大，不会把选路查询放大成 DB 压力。
const routeCandidateWaitPoll = 500 * time.Millisecond

// preferredChannelKeySlotWaitBudget 是高优先级令牌在首选渠道 Key 并发满载时的等待预算，
// 对齐 claudeAffinityConcurrencyWait 先例。期间通过渠道级释放事件 + 500ms 轮询重试同一渠道。
const preferredChannelKeySlotWaitBudget = 5 * time.Second

// preferredChannelSlotWaitPoll 是等待渠道 Key 槽位期间的兜底轮询间隔：
// 覆盖「其他 Key 冷却到期」这类不产生释放事件的恢复路径。
const preferredChannelSlotWaitPoll = routeCandidateWaitPoll

// shouldWaitForPreferredChannel 判断是否值得为当前请求等待首选渠道的 Key 槽位。
// 仅当：候选排序首位 + 非兜底/亲和探测 + 令牌优先级高于默认值。
// 默认优先级令牌是背景负载，等待只会推高整体延迟，应立即溢出到下一渠道。
func (s *Server) shouldWaitForPreferredChannel(cfg *model.Config, reqCtx *proxyRequestContext, index int) bool {
	if cfg == nil || reqCtx == nil || index != 0 {
		return false
	}
	if cfg.CooldownFallback || reqCtx.claudeAffinityProbe {
		return false
	}
	return s.authService.TokenPriority(reqCtx.tokenHash) > model.AuthTokenPriorityDefault
}

// waitForPreferredChannelSlot 在首选渠道 Key 并发满载时等待槽位释放并重试同渠道。
// 返回最后一次 tryChannelWithKeys 的结果；客户端断开由 ctx 随重试的 fail-fast 收尾。
// 重试只在真实拿到槽位后才发上游请求，等待本身不放大上游流量。
func (s *Server) waitForPreferredChannelSlot(
	ctx context.Context,
	cfg *model.Config,
	reqCtx *proxyRequestContext,
	w http.ResponseWriter,
) (*proxyResult, error) {
	budget := s.preferredChannelKeySlotWait
	if budget <= 0 {
		budget = preferredChannelKeySlotWaitBudget
	}
	priority := s.authService.TokenPriority(reqCtx.tokenHash)
	log.Printf(
		"[INFO] 渠道 %s (ID=%d) Key 并发满载，优先级 %d 令牌等待槽位（≤%s）",
		cfg.Name, cfg.ID, priority, budget,
	)

	deadline := time.Now().Add(budget)
	for {
		result, err := s.tryChannelWithKeys(ctx, cfg, reqCtx, w)
		if !errors.Is(err, ErrKeyConcurrencyExceeded) {
			return result, err
		}

		if time.Now().After(deadline) {
			return nil, err
		}
		// 等渠道级释放事件；waitCtx 的超时同时是兜底轮询间隔，
		// 覆盖「其他 Key 冷却到期」这类不产生释放事件的恢复路径。
		// 父 ctx 取消时这里立即返回，下一轮 tryChannelWithKeys 的 fail-fast 会收尾。
		waitCtx, cancel := context.WithTimeout(ctx, preferredChannelSlotWaitPoll)
		_ = s.channelConcurrencyLimiter.waitForChannelRelease(waitCtx, cfg.ID)
		cancel()
	}
}

// waitForRouteCandidates 在初次选路为空时排队等待：按固定间隔重试选路，
// 拿到候选立即返回，超过 routeCandidateWaitBudget 或客户端断开才放弃。
// 只在 HTTP 主链路使用；AlphaSearch 与 WebSocket 路径有各自的故障边界，不经过这里。
func (s *Server) waitForRouteCandidates(ctx context.Context, c *gin.Context, originalModel string, clientProtocol string) []*model.Config {
	deadline := time.Now().Add(routeCandidateWaitBudget)
	timer := time.NewTimer(routeCandidateWaitPoll)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			// 客户端断开或上游取消：不白等，让上层按 ctx.Err() 处理
			return nil
		case <-timer.C:
		}

		cands, err := s.selectRouteCandidates(ctx, c, originalModel, clientProtocol)
		if err == nil && len(cands) > 0 {
			return cands
		}
		if err != nil {
			// 令牌渠道限制等错误不会随等待消失，直接放弃避免把客户端挂住
			return nil
		}

		if time.Now().Add(routeCandidateWaitPoll).After(deadline) {
			// 下一轮已经超出预算：不再等，把最后一次空结果交回
			return nil
		}
		timer.Reset(routeCandidateWaitPoll)
	}
}

// ============================================================================
// 主请求处理器
// ============================================================================

// handleSpecialRoutes 处理特殊路由（模型列表、token计数等）
// 返回 true 表示已处理，调用方应直接返回
func (s *Server) handleSpecialRoutes(c *gin.Context) bool {
	path := c.Request.URL.Path
	method := c.Request.Method

	switch {
	case method == http.MethodGet && path == "/v1/models":
		s.handleListOpenAIModels(c)
		return true
	case method == http.MethodGet && path == "/v1beta/models":
		s.handleListGeminiModels(c)
		return true
	case method == http.MethodPost && path == "/v1/messages/count_tokens":
		s.handleCountTokens(c)
		return true
	}
	return false
}

// HandleProxyRequest 通用透明代理处理器
func (s *Server) HandleProxyRequest(c *gin.Context) {
	if isResponsesWebsocketUpgradeRequest(c.Request) {
		s.HandleResponsesWebsocket(c)
		return
	}
	httpMetrics := s.httpRuntime.begin()
	defer func() {
		httpMetrics.finish(c.Writer.Status(), c.Writer.Size())
	}()

	startTime := time.Now()

	// 并发控制
	release, ok := s.acquireConcurrencySlot(c)
	if !ok {
		return
	}
	defer release()

	// 特殊路由优先处理
	if s.handleSpecialRoutes(c) {
		return
	}

	requestMethod := c.Request.Method

	incoming, err := parseIncomingRequest(c, s.bodyLimits)
	if err != nil {
		switch {
		case errors.Is(err, errBodyTooLarge):
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": err.Error()})
		case errors.Is(err, errBodyReadTimeout):
			// 408 而不是 400：请求本身没问题，是客户端没在读取超时内传完。
			c.JSON(http.StatusRequestTimeout, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
		return
	}
	originalModel := incoming.originalModel
	all := incoming.body
	isStreaming := incoming.isStreaming
	httpMetrics.observeRequest(isStreaming, len(all))

	clientProtocol, effectiveRequestPath := clientRequestMetadata(c)
	inboundRequest := captureInboundRequestLog(c.Request, all, model.RequestTransportHTTP)
	if err := validateClientBodyMatchesProtocol(clientProtocol, all); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if protocol.DetectRequestFamily(effectiveRequestPath) == protocol.RequestFamilyAlphaSearch {
		all = sanitizeCodexAlphaSearchBody(all)
	}

	thinkingEffort := extractThinkingEffortFromJSON(all)

	tokenHashStr := ""
	if v, ok := c.Get("token_hash"); ok {
		tokenHashStr, _ = v.(string)
	}
	tokenID, _ := c.Get("token_id")
	tokenIDInt64, _ := tokenID.(int64)

	if !s.enforceTokenLimits(c, tokenHashStr, incoming.authorizationModel()) {
		return
	}

	var claudeAffinity *claudeSessionAffinity
	if clientProtocol == protocol.Anthropic && requestMethod == http.MethodPost &&
		isAnthropicMessagesRequest(protocol.Anthropic, effectiveRequestPath) {
		claudeAffinity = newClaudeSessionAffinity(s.store, tokenHashStr, c.Request.Header, all)
		if err := claudeAffinity.load(c.Request.Context()); err != nil {
			count := claudeAffinityLoadFailureCount.Add(1)
			if count%100 == 1 {
				log.Printf("[WARN] 加载 Claude session Key 亲和失败 (累计: %d): %v", count, err)
			}
		}
	}

	timeout := parseTimeout(c.Request.URL.Query(), c.Request.Header)
	ctx := c.Request.Context()
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var executionSession *responsesExecutionSession
	var routingSession *responsesExecutionSession
	var executionSessionRequestBody []byte
	var nativeRequestBody []byte
	if clientProtocol == protocol.Codex && isStreaming && requestMethod == http.MethodPost &&
		protocol.DetectRequestFamily(effectiveRequestPath) == protocol.RequestFamilyResponses {
		sessionID := responsesExecutionSessionID(c.Request.Header)
		// Ordinary HTTP requests only need process-local state when the client supplied
		// the explicit Session-Id contract. Cache routing hints are not conversation IDs.
		if tokenHashStr != "" && sessionID != "" {
			var releaseSession func()
			var errSession error
			executionSession, releaseSession, errSession = s.responsesExecutionSessions.acquire(tokenHashStr, sessionID)
			if errSession != nil {
				c.JSON(http.StatusTooManyRequests, gin.H{"error": errSession.Error()})
				return
			}
			defer releaseSession()
			if errAcquire := executionSession.acquireTurn(ctx); errAcquire != nil {
				c.JSON(http.StatusRequestTimeout, gin.H{"error": errAcquire.Error()})
				return
			}
			defer executionSession.releaseTurn()
			routingSession = executionSession
			replayBody, incrementalBody, localContinuation, errNormalize :=
				executionSession.transcript.normalizeHTTPRequests(all)
			if errNormalize != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": errNormalize.Error()})
				return
			}
			if !localContinuation {
				executionSession = nil
			} else {
				executionSessionRequestBody = replayBody
				// HTTP may continue an already established upstream websocket, but it must
				// never create one. Without an attached socket, preserve HTTP wire semantics.
				if _, connected := executionSession.upstream.targetSnapshot(); connected {
					all = replayBody
					nativeRequestBody = incrementalBody
				}
			}
		}
	}

	cands, err := s.selectRouteCandidates(ctx, c, originalModel, string(clientProtocol))
	if err != nil {
		if errors.Is(err, errUnknownClientProtocol) {
			c.JSON(http.StatusNotFound, gin.H{"error": "unsupported path"})
			return
		}
		// 令牌渠道限制排除了全部候选（含全冷却兜底候选）：403，不重试不兜底。
		if errors.Is(err, ErrNoAllowedChannelForToken) {
			c.JSON(http.StatusForbidden, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	if len(cands) == 0 {
		if protocol.DetectRequestFamily(effectiveRequestPath) == protocol.RequestFamilyAlphaSearch {
			writeEmptyAlphaSearchResponse(c.Writer)
			return
		}
		// 全冷却兜底被 15s 探测限频拦下时请求会在选路阶段拿到空候选。
		// 冷却到期通常是秒级窗口，让请求排队等待至多 10s 重试选路，
		// 避免把可恢复的瞬时冷却放大成客户端可见的 503。
		cands = s.waitForRouteCandidates(ctx, c, originalModel, string(clientProtocol))
		if len(cands) == 0 {
			s.AddLogAsync(&model.LogEntry{
				Time:               model.JSONTime{Time: time.Now()},
				Model:              originalModel,
				LogSource:          model.LogSourceProxy,
				AuthTokenID:        tokenIDInt64,
				ClientProtocol:     string(clientProtocol),
				StatusCode:         503,
				Message:            "no available upstream (all cooled or none)",
				IsStreaming:        isStreaming,
				ClientIP:           c.ClientIP(),
				ThinkingEffort:     thinkingEffort,
				IsTerminalOverride: true,
				InboundRequest:     inboundRequest.Clone(),
			})
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "no available upstream (all cooled or none)"})
			return
		}
	}

	if tokenHashStr != "" {
		filtered, restricted := s.authService.FilterAllowedChannels(tokenHashStr, cands)
		if restricted {
			cands = filtered
			if len(cands) == 0 {
				c.JSON(http.StatusForbidden, gin.H{
					"error": "no allowed upstream channel for this token",
				})
				return
			}
		}
	}
	if routingSession != nil {
		if channelID, ok := routingSession.routeChannelSnapshot(); ok {
			cands = prioritizePinnedCodexChannel(cands, channelID)
		}
	}

	reqCtx := &proxyRequestContext{
		originalModel:  originalModel,
		clientProtocol: clientProtocol,
		codexClient:    isCodexMultiAgentClient(codexMultiAgentUserAgent(c.Request.Header)),
		requestMethod:  requestMethod,
		requestPath:    effectiveRequestPath,
		rawQuery:       c.Request.URL.RawQuery,
		body:           all,
		translatedBody: all,
		header:         c.Request.Header,
		isStreaming:    isStreaming,
		tokenHash:      tokenHashStr,
		tokenID:        tokenIDInt64,
		clientIP:       c.ClientIP(),
		startTime:      startTime,
		inboundRequest: inboundRequest,
		thinkingEffort: thinkingEffort,
	}
	if routingSession != nil {
		reqCtx.routingSession = routingSession
	}
	if claudeAffinity != nil {
		reqCtx.claudeAffinity = claudeAffinity
	}
	if executionSession != nil {
		reqCtx.codexMultiAgentV2Optimized = executionSession.codexMultiAgentV2StateSnapshot()
	}
	ctx = withCodexMultiAgentV2RequestContext(ctx, reqCtx)
	if executionSession != nil && nativeRequestBody != nil {
		reqCtx.nativeCodexWS = executionSession.upstream
		reqCtx.nativeCodexBody = nativeRequestBody
	}
	reqCtx.observer = &ForwardObserver{
		OnBytesRead: func(n int64) {
			s.activeRequests.AddBytes(reqCtx.activeReqID, n)
		},
		OnFirstByteRead: func() {
			s.activeRequests.SetClientFirstByteTime(reqCtx.activeReqID, time.Since(reqCtx.attemptStartTime))
		},
		OnUpstreamWebsocket: func(upstreamWebsocket bool) {
			s.activeRequests.SetUpstreamWebsocket(reqCtx.activeReqID, upstreamWebsocket)
		},
		OnDebugCapture: func(dc *debugCapture) {
			s.activeRequests.SetDebugCapture(reqCtx.activeReqID, dc)
		},
		OnUpstreamRequest: func(request *model.RequestLogEntry) {
			if request != nil {
				reqCtx.upstreamRequests = append(reqCtx.upstreamRequests, request.Clone())
			}
		},
	}
	defer func() {
		if reqCtx.activeReqID > 0 {
			s.activeRequests.Remove(reqCtx.activeReqID)
		}
	}()

	lastResult, succeeded := s.runProxyAttemptLoop(ctx, cands, reqCtx, c.Writer)
	if executionSession != nil {
		s.updateCodexMultiAgentV2SessionState(executionSession, reqCtx)
	}
	if succeeded {
		if executionSession != nil && lastResult != nil && lastResult.hasResponsesTurn {
			s.responsesExecutionSessions.commit(executionSession, executionSessionRequestBody, lastResult.responsesTurn)
		}
		return
	}

	s.writeFinalProxyResponse(c, reqCtx, originalModel, isStreaming, lastResult, len(cands))
}

func determineFinalClientStatus(lastResult *proxyResult) int {
	if lastResult == nil || lastResult.status == 0 {
		return http.StatusServiceUnavailable
	}

	status := lastResult.status

	// 499处理：区分客户端取消 vs 上游返回的499
	if status == util.StatusClientClosedRequest {
		if lastResult.isClientCanceled {
			return status // 真正的客户端取消，透传499
		}
		return http.StatusBadGateway // 上游499，映射为502
	}

	// 仅映射内部状态码（596-599），其他全部透传
	return util.ClientStatusFor(status)
}

func (s *Server) clientFacingFinalStatus(codexClient bool, lastResult *proxyResult) int {
	status := determineFinalClientStatus(lastResult)
	if s.codexMap429To503 && codexClient && status == http.StatusTooManyRequests {
		return http.StatusServiceUnavailable
	}
	return status
}

func shouldStopTryingChannels(result *proxyResult) bool {
	if result == nil {
		return true
	}
	// 客户端取消：立即停止
	if result.isClientCanceled {
		return true
	}
	return result.nextAction == cooldown.ActionReturnClient
}

// enforceTokenLimits 检查 token 的模型限制与费用限额。
// 违规时已写响应并返回 false，调用方应直接 return。
func (s *Server) enforceTokenLimits(c *gin.Context, tokenHash, originalModel string) bool {
	// 检查令牌模型限制（2026-01新增）
	if tokenHash != "" && originalModel != "" {
		if !s.authService.IsModelAllowed(tokenHash, originalModel) {
			c.JSON(http.StatusForbidden, gin.H{
				"error": fmt.Sprintf("model '%s' is not allowed for this token", originalModel),
			})
			return false
		}
	}

	// 检查令牌费用限额（2026-01新增）
	// 设计决策：在请求开始时检查，费用在请求完成后记账。
	// 超额窗口：预检（IsCostLimitExceeded/RLock）与记账（AddCostToCache/Lock）之间是
	// check-then-act。设了 max_concurrency 时最多超额并发上限个请求；未设上限时 N 个并发
	// 请求可同时通过预检后全部超额——费用最终都会记账，限额是“滞后 N 个请求才封顶”，非永久绕过。
	// 原因：费用只有在请求完成后才能精确计算（token数量由上游返回），此处只能做预检查。
	// 严格“先扣费后请求”需复杂的预估+退款机制，不值得（YAGNI）。
	if tokenHash != "" {
		usedMicro, limitMicro, window, exceeded := s.authService.costLimitState(tokenHash)
		if exceeded {
			used := util.MicroUSDToUSD(usedMicro)
			limit := util.MicroUSDToUSD(limitMicro)
			prefix := "Cost"
			switch window {
			case "daily":
				prefix = "Daily cost"
			case "monthly":
				prefix = "Monthly cost"
			case "total":
				prefix = "Total cost"
			}
			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": gin.H{
					"message": fmt.Sprintf("%s limit exceeded: $%.2f used of $%.2f limit", prefix, used, limit),
					"type":    "insufficient_quota",
					"code":    "cost_limit_exceeded",
				},
			})
			return false
		}
	}

	return true
}

// runProxyAttemptLoop 按优先级遍历候选渠道。
// 返回最后一次结果（可能 nil），调用方据此决定是否兜底响应。
// succeeded 时内部已写响应，调用方应停止后续 writeFinal 步骤。
func (s *Server) runProxyAttemptLoop(
	ctx context.Context,
	cands []*model.Config,
	reqCtx *proxyRequestContext,
	w http.ResponseWriter,
) (lastResult *proxyResult, succeeded bool) {
	affinityResult, affinityAttempted := s.tryClaudeAffinityKey(ctx, cands, reqCtx, w)
	if affinityAttempted && affinityResult != nil {
		if affinityResult.succeeded {
			return affinityResult, true
		}
		if shouldStopTryingChannels(affinityResult) {
			return affinityResult, false
		}
	}

	lastResult, succeeded = s.runProxyAttemptLoopWithFailureBoundary(ctx, cands, reqCtx, w, nil)
	if lastResult == nil && affinityResult != nil {
		lastResult = affinityResult
	}
	return lastResult, succeeded
}

func (s *Server) runProxyAttemptLoopWithFailureBoundary(
	ctx context.Context,
	cands []*model.Config,
	reqCtx *proxyRequestContext,
	w http.ResponseWriter,
	stopAfterFailure func(current, next *model.Config, result *proxyResult) bool,
) (lastResult *proxyResult, succeeded bool) {
	sawAlphaSearchUnsupported := false
	for index, cfg := range cands {
		result, err := s.tryChannelWithKeys(ctx, cfg, reqCtx, w)

		// 高优先级令牌在首选渠道仅因 Key 并发满载失败时，等待槽位后重试同渠道，
		// 而不是立即溢出到低优先级渠道（等待语义见 shouldWaitForPreferredChannel）。
		if err != nil && errors.Is(err, ErrKeyConcurrencyExceeded) &&
			s.shouldWaitForPreferredChannel(cfg, reqCtx, index) {
			result, err = s.waitForPreferredChannelSlot(ctx, cfg, reqCtx, w)
		}

		if err != nil && errors.Is(err, errClaudeAffinityTargetAlreadyTried) {
			continue
		}

		// 所有Key冷却：触发渠道级冷却(503)，防止后续请求重复尝试
		// 使用 cooldownManager.HandleError 统一处理（DRY原则）
		if err != nil && errors.Is(err, ErrAllKeysUnavailable) {
			// 统一走 applyCooldownDecision：断开取消链+按决策执行缓存失效
			s.applyCooldownDecision(ctx, cfg, httpErrorInputFromParts(cfg.ID, cooldown.NoKeyIndex, 503, nil, nil))
			continue
		}

		// [WARN] 所有Key验证失败，尝试下一个渠道
		if err != nil && errors.Is(err, ErrAllKeysExhausted) {
			log.Printf("[WARN] 渠道 %s (ID=%d) 所有Key验证失败，跳过该渠道", cfg.Name, cfg.ID)
			continue
		}

		if err != nil && errors.Is(err, ErrChannelRPMExceeded) {
			log.Printf("[INFO] 渠道 %s (ID=%d) 已达到RPM限制，跳过该渠道", cfg.Name, cfg.ID)
			continue
		}

		if err != nil && errors.Is(err, ErrKeyConcurrencyExceeded) {
			log.Printf(
				"[INFO] 渠道 %s (ID=%d) 已达到单 Key 并发限制，跳过该渠道：%s",
				cfg.Name,
				cfg.ID,
				keyConcurrencySkipReason(err),
			)
			continue
		}

		if result != nil {
			if result.protocolCapabilityMissing && protocol.DetectRequestFamily(reqCtx.requestPath) == protocol.RequestFamilyAlphaSearch {
				sawAlphaSearchUnsupported = true
			}
			if result.succeeded {
				return result, true
			}

			lastResult = result

			// 客户端已取消：别再浪费资源“重试”了。
			if result.isClientCanceled {
				break
			}

			if shouldStopTryingChannels(result) {
				break
			}

			if stopAfterFailure != nil && index+1 < len(cands) &&
				stopAfterFailure(cfg, cands[index+1], result) {
				break
			}
		}
	}
	if sawAlphaSearchUnsupported &&
		protocol.DetectRequestFamily(reqCtx.requestPath) == protocol.RequestFamilyAlphaSearch &&
		(lastResult == nil || (!lastResult.isClientCanceled && lastResult.nextAction != cooldown.ActionReturnClient)) {
		writeEmptyAlphaSearchResponse(w)
		return &proxyResult{status: http.StatusOK, succeeded: true, nextAction: cooldown.ActionReturnClient}, true
	}

	return lastResult, false
}

func writeEmptyAlphaSearchResponse(w http.ResponseWriter) {
	header := make(http.Header, 2)
	header.Set("Content-Type", "application/json; charset=utf-8")
	header.Set("X-CCLoad-Search-Fallback", "empty")
	writeResponseWithHeaders(w, http.StatusOK, header, []byte(`{"encrypted_output":null,"output":"","results":[]}`))
}

// writeFinalProxyResponse 所有渠道失败时写最终响应：
// 计算 finalStatus、决定 skipLog、透传 body 或 JSON 错误。
func (s *Server) writeFinalProxyResponse(
	c *gin.Context,
	reqCtx *proxyRequestContext,
	originalModel string,
	isStreaming bool,
	lastResult *proxyResult,
	candidateCount int,
) {
	// 所有渠道都失败：返回“最后一次实际失败”的状态码（并映射内部状态码），避免一律伪装成503。
	upstreamFinalStatus := determineFinalClientStatus(lastResult)
	finalStatus := s.clientFacingFinalStatus(reqCtx.codexClient, lastResult)

	msg := "exhausted backends"
	if lastResult != nil && lastResult.isClientCanceled {
		msg = "client closed request (context canceled)"
	} else if lastResult != nil && lastResult.status == 499 && finalStatus != 499 {
		// 上游返回 499 没有任何“客户端取消”的语义价值：对外统一视为网关错误。
		msg = "upstream returned 499 (mapped)"
	} else if upstreamFinalStatus != finalStatus {
		msg = fmt.Sprintf("upstream status %d (mapped to %d for Codex)", upstreamFinalStatus, finalStatus)
	} else if finalStatus != http.StatusServiceUnavailable {
		msg = fmt.Sprintf("upstream status %d", finalStatus)
	}

	// 过滤不需要汇总日志的场景
	// - 客户端取消（499）：已在 handleNetworkError 中记录渠道级日志
	// - 客户端错误（400）：已在渠道级日志记录，汇总日志冗余
	// - 候选池 ≤1：实际只尝试了 1 个渠道，渠道级日志已完整反映失败原因，汇总日志冗余
	skipLog := lastResult != nil && (lastResult.isClientCanceled || finalStatus == http.StatusBadRequest)
	skipLog = skipLog || candidateCount <= 1
	if !skipLog {
		s.AddLogAsync(&model.LogEntry{
			Time:               model.JSONTime{Time: reqCtx.startTime},
			Model:              originalModel,
			LogSource:          model.LogSourceProxy,
			ClientProtocol:     string(reqCtx.clientProtocol),
			StatusCode:         upstreamFinalStatus,
			Message:            msg,
			Duration:           time.Since(reqCtx.startTime).Seconds(),
			IsStreaming:        isStreaming,
			ClientIP:           reqCtx.clientIP,
			RequestID:          reqCtx.activeReqID,
			IsTerminalOverride: true,
			InboundRequest:     reqCtx.inboundRequest.Clone(),
		})
	}

	if lastResult != nil && lastResult.status != 0 {
		// 透明代理原则：透传所有上游响应（状态码+header+body）
		writeResponseWithHeaders(c.Writer, finalStatus, lastResult.header, lastResult.body)
		return
	}

	disableResponseWriteTimeout(c.Writer, "最终响应")
	c.JSON(finalStatus, gin.H{"error": "no upstream available"})
}
