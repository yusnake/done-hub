package claude

import (
	"done-hub/common/requester"
	"done-hub/providers/base"
	"done-hub/types"
	"net/http"
)

type ClaudeChatInterface interface {
	base.ProviderInterface
	CreateClaudeChat(request *ClaudeRequest) (*ClaudeResponse, *types.OpenAIErrorWithStatusCode)
	CreateClaudeChatStream(request *ClaudeRequest) (requester.StreamReaderInterface[string], *types.OpenAIErrorWithStatusCode)
}

// ClaudePassthroughInterface 透传模式接口
type ClaudePassthroughInterface interface {
	base.ProviderInterface
	IsPassthrough() bool
	SendPassthroughRequest(bodyBytes []byte, isStream bool) (*http.Response, *types.OpenAIErrorWithStatusCode)
}
