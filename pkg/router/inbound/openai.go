package inbound

import (
	"control-plane-model-test/pkg/router/core"
	"encoding/json"
	"net/http"
	"strings"
)

// DTO
// 每个特定协议的具体所需参数获取
type openAIRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
	Stream      *bool    `json:"stream,omitempty" default:"false"`
	Temperature *float64 `json:"temperature,omitempty"`
	TopP        *float64 `json:"top_p,omitempty"`
	MaxTokens   *int     `json:"max_tokens,omitempty"`
}
type OpenAIInBoundAdapter struct{}

func NewOpenAIInBoundAdapter() InboundAdapter {
	return &OpenAIInBoundAdapter{}
}

func basicMatch(req *http.Request) bool {
	if req.Method != http.MethodPost {
		return false
	}
	ct := req.Header.Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		return false
	}
	if !strings.Contains(req.URL.Path, "/v1/") {
		return false
	}
	return true
}

func (od *OpenAIInBoundAdapter) Match(req *http.Request) bool {
	if !basicMatch(req) {
		return false
	}
	// todo: 后续针对不同指定协议的业务，需要检查请求的Header是否包含业务的请求头
	// example: OpenAI 需要指明当前请求的Authorizaiton: Bearer <token>
	// 而这部分的处理则需要新建handler模块，去调用处理
	// as: handler.OpenAIChecker.Check(req)
	return true
}

// 解析请求，将inbound适配器解析客户端请求，提取部分参数到中间态，供后续便捷使用
func (od *OpenAIInBoundAdapter) Parse(req *http.Request) (*core.LLMRequest, error) {
	var r openAIRequest
	if err := json.NewDecoder(req.Body).Decode(&r); err != nil {
		return nil, err
	}
	msgs := make([]*core.Message, 0, len(r.Messages))
	for _, msg := range r.Messages {
		msgs = append(msgs, &core.Message{
			Role:    msg.Role,
			Content: msg.Content,
		})
	}
	params := make(map[string]any)
	if r.Temperature != nil {
		params["temperature"] = *r.Temperature
	}
	if r.TopP != nil {
		params["top_p"] = *r.TopP
	}
	return &core.LLMRequest{
		Model:      r.Model,
		Messages:   msgs,
		Stream:     r.Stream,
		Headers:    req.Header,
		Parameters: params,
	}, nil
}
