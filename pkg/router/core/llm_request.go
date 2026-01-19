package core

import "net/http"

// 中间态
// 只负责存储inbound适配器解析的参数和透传数据，供后续outbound适配器使用
type LLMRequest struct {
	// 从Request中提取的参数，将会被使用在路由策略上
	Messages []*Message
	Prompt   string
	Model    string
	Strategy string
	Stream   *bool
	// 元信息透传
	Headers http.Header
	// 原始参数透传
	Parameters map[string]any
}
type Message struct {
	Role    string
	Content string
}
