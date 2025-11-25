package claude

import (
	"bytes"
	"done-hub/common"
	"done-hub/common/config"
	"done-hub/common/requester"
	"done-hub/model"
	"done-hub/providers/base"
	"done-hub/types"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type ClaudeProviderFactory struct{}

// 创建 ClaudeProvider
func (f ClaudeProviderFactory) Create(channel *model.Channel) base.ProviderInterface {
	return &ClaudeProvider{
		BaseProvider: base.BaseProvider{
			Config:    getConfig(),
			Channel:   channel,
			Requester: requester.NewHTTPRequester(*channel.Proxy, RequestErrorHandle),
		},
	}
}

type ClaudeProvider struct {
	base.BaseProvider
}

func getConfig() base.ProviderConfig {
	return base.ProviderConfig{
		BaseURL:         "https://api.anthropic.com",
		ChatCompletions: "/v1/messages",
		ModelList:       "/v1/models",
	}
}

// 请求错误处理
func RequestErrorHandle(resp *http.Response) *types.OpenAIError {
	claudeError := &ClaudeError{}
	err := json.NewDecoder(resp.Body).Decode(claudeError)
	if err != nil {
		return nil
	}

	return errorHandle(claudeError)
}

// 错误处理
func errorHandle(claudeError *ClaudeError) *types.OpenAIError {
	if claudeError == nil {
		return nil
	}

	if claudeError.Type == "" {
		return nil
	}
	return &types.OpenAIError{
		Message: claudeError.ErrorInfo.Message,
		Type:    claudeError.ErrorInfo.Type,
		Code:    claudeError.Type,
	}
}

// IsPassthrough 检查是否启用透传模式
// 通过渠道的 Other 字段配置: {"passthrough": true}
func (p *ClaudeProvider) IsPassthrough() bool {
	if p.Channel.Other == "" {
		return false
	}
	var other map[string]interface{}
	if err := json.Unmarshal([]byte(p.Channel.Other), &other); err != nil {
		return false
	}
	if passthrough, ok := other["passthrough"].(bool); ok {
		return passthrough
	}
	return false
}

// passthroughRequestHeaders 透传模式下克隆所有请求头
func (p *ClaudeProvider) passthroughRequestHeaders(headers map[string]string) {
	if p.Context == nil {
		return
	}
	// 克隆所有请求头
	for key, values := range p.Context.Request.Header {
		if len(values) > 0 {
			// 跳过 hop-by-hop 头（这些头不应该被转发）
			lowerKey := strings.ToLower(key)
			if lowerKey == "connection" ||
				lowerKey == "keep-alive" ||
				lowerKey == "transfer-encoding" ||
				lowerKey == "te" ||
				lowerKey == "trailer" ||
				lowerKey == "upgrade" ||
				lowerKey == "proxy-authorization" ||
				lowerKey == "proxy-authenticate" {
				continue
			}
			headers[key] = values[0]
		}
	}

	// 自定义 header 覆盖（如果有配置，仍然生效）
	if p.Channel.ModelHeaders != nil {
		var customHeaders map[string]string
		err := json.Unmarshal([]byte(*p.Channel.ModelHeaders), &customHeaders)
		if err == nil {
			for key, value := range customHeaders {
				headers[key] = value
			}
		}
	}
}

// 获取请求头
func (p *ClaudeProvider) GetRequestHeaders() (headers map[string]string) {
	headers = make(map[string]string)

	if p.IsPassthrough() {
		// 透传模式：克隆所有请求头
		p.passthroughRequestHeaders(headers)
	} else {
		// 默认模式：只透传部分头
		p.CommonRequestHeaders(headers)
	}

	// x-api-key 始终使用渠道配置的 key
	headers["x-api-key"] = p.Channel.Key

	// anthropic-version 处理
	if p.IsPassthrough() {
		// 透传模式：如果原始请求没有，也不添加默认值（完全透传）
		// 注意：Anthropic API 要求 anthropic-version 头，透传模式下由客户端负责提供
	} else {
		// 默认模式：如果没有则添加默认值
		anthropicVersion := "2023-06-01"
		if p.Context != nil {
			if v := p.Context.Request.Header.Get("anthropic-version"); v != "" {
				anthropicVersion = v
			}
		}
		headers["anthropic-version"] = anthropicVersion
	}

	return headers
}

func (p *ClaudeProvider) GetFullRequestURL(requestURL string) string {
	baseURL := strings.TrimSuffix(p.GetBaseURL(), "/")
	if strings.HasPrefix(baseURL, "https://gateway.ai.cloudflare.com") {
		requestURL = strings.TrimPrefix(requestURL, "/v1")
	}

	return fmt.Sprintf("%s%s", baseURL, requestURL)
}

func stopReasonClaude2OpenAI(reason string) string {
	switch reason {
	case "end_turn", "stop_sequence":
		return types.FinishReasonStop
	case "max_tokens":
		return types.FinishReasonLength
	case "tool_use":
		return types.FinishReasonToolCalls
	case "refusal":
		return types.FinishReasonContentFilter
	default:
		return reason
	}
}

func convertRole(role string) string {
	switch role {
	case types.ChatMessageRoleUser, types.ChatMessageRoleTool, types.ChatMessageRoleFunction:
		return types.ChatMessageRoleUser
	default:
		return types.ChatMessageRoleAssistant
	}
}

// SendPassthroughRequest 真正的透传模式：直接转发原始请求体，返回原始响应
// 与 gpt-load 保持一致的实现
func (p *ClaudeProvider) SendPassthroughRequest(bodyBytes []byte, isStream bool) (*http.Response, *types.OpenAIErrorWithStatusCode) {
	// 构建上游 URL
	url, errWithCode := p.GetSupportedAPIUri(config.RelayModeChatCompletions)
	if errWithCode != nil {
		return nil, errWithCode
	}
	fullRequestURL := p.GetFullRequestURL(url)
	if fullRequestURL == "" {
		return nil, common.ErrorWrapperLocal(nil, "invalid_claude_config", http.StatusInternalServerError)
	}

	// 创建请求
	req, err := http.NewRequest(http.MethodPost, fullRequestURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, common.ErrorWrapper(err, "create_request_failed", http.StatusInternalServerError)
	}

	// 像 gpt-load 一样：先克隆所有原始请求头
	if p.Context != nil {
		for key, values := range p.Context.Request.Header {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
	}

	// 删除客户端的认证头（与 gpt-load 一致）
	req.Header.Del("Authorization")
	req.Header.Del("X-Api-Key")
	req.Header.Del("X-Goog-Api-Key")

	// 设置渠道的 API key
	req.Header.Set("x-api-key", p.Channel.Key)

	// 设置 Content-Type（确保正确）
	req.Header.Set("Content-Type", "application/json")

	// 流式请求设置 Accept 头
	if isStream {
		req.Header.Set("Accept", "text/event-stream")
	}

	// 自定义 header 覆盖（如果有配置）
	if p.Channel.ModelHeaders != nil {
		var customHeaders map[string]string
		if jsonErr := json.Unmarshal([]byte(*p.Channel.ModelHeaders), &customHeaders); jsonErr == nil {
			for key, value := range customHeaders {
				req.Header.Set(key, value)
			}
		}
	}

	// 使用全局 HTTP client 发送请求
	resp, err := requester.HTTPClient.Do(req)
	if err != nil {
		return nil, common.ErrorWrapper(err, "request_failed", http.StatusInternalServerError)
	}

	// 检查错误状态码
	if resp.StatusCode >= 400 {
		// 不关闭 resp.Body，让调用方处理
		return resp, nil
	}

	return resp, nil
}
