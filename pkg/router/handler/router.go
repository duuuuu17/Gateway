package handler

import (
	"control-plane-model-test/pkg/config"
	"control-plane-model-test/pkg/router/core"
	"control-plane-model-test/pkg/router/inbound"
	"control-plane-model-test/pkg/router/outbound"
	"net/http"
)

type Router struct {
	cfgs      []config.ConfigReader
	inbounds  InboundRegistry  // router 获取inbound适配器
	outbound  OutboundRegistry // router获取outbound适配器
	forwarder core.Forward
}

// 通过inbound适配器接口的行为函数，来获取适配器实例
type InboundRegistry interface {
	GetAdapter(req *http.Request) (inbound.InboundAdapter, error)
}

// 通过outbound适配器接口的行为函数，来获取适配器实例
type OutboundRegistry interface {
	GetAdapter(backendType string) (outbound.OutboundAdapter, error)
}

// 由main函数调用的初始化函数
func InitialRouterModel(cfgs []config.ConfigReader) *Router {
	//todo: 调用实例的构造函数获取对象

	inboundsRegistry := inbound.NewInboundAdapterRegistry()
	inboundsRegistry.AddInboundAdapter("openai", inbound.NewOpenAIInBoundAdapter())

	br := outbound.NewBackendRegistry()
	br.AddBackendRegistry("qwem2", outbound.NewOpenAIOutBoundAdapter())

	return NewRouter(cfgs, br, inboundsRegistry)
}

func NewRouter(cfgs []config.ConfigReader, outboundRegistry OutboundRegistry, inbounds InboundRegistry) *Router {
	return &Router{
		cfgs:     cfgs,
		outbound: outboundRegistry,
		inbounds: inbounds,
	}
}

// 调试函数
func (r *Router) GetConfigs() []config.ConfigReader {
	return r.cfgs
}

// 实际执行Http处理逻辑
func (r *Router) HandleFunc(w http.ResponseWriter, req *http.Request) {
	// 执行逻辑
	// 查询匹配的inboundAdapter
	inboundAdapter, err := r.inbounds.GetAdapter(req)
	if err != nil {
		http.Error(w, "unsupported protocol", 400)
		return
	}
	// 获取中间态请求对象
	llmRequest, err := inboundAdapter.Parse(req)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	// 根据中间态请求对象和配置参数选择目标后端服务
	backendConfig, err := core.SelectBackend(llmRequest, r.cfgs)
	if err != nil {
		http.Error(w, "no backend support!", 500)
		return
	}
	// 获取对应后端服务的适配器
	outboundAdapter, err := r.outbound.GetAdapter(backendConfig.GetConfig().Protocol)
	if err != nil {
		http.Error(w, "no backend adapter!", 500)
		return
	}
	// 构建适合后端Pod的请求
	backendRequest, err := outboundAdapter.BuildHTTPRequest(req.Context(), llmRequest, backendConfig)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	// 转发请求.不许要使用到客户端请求的上下文，是因为在构建转发请求时就已经使用
	resp, err := r.forwarder.Do(backendRequest)
	// 处理后端Pod返回的请求并转发回给客户端
	outboundAdapter.HandleResponse(req.Context(), w, resp)
}
