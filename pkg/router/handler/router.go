package handler

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	eventbus "github.com/duuuuu17/llm-router-operator/pkg/eventBus"
	"github.com/duuuuu17/llm-router-operator/pkg/router/errs"
	"github.com/duuuuu17/llm-router-operator/pkg/router/filters"
	"github.com/duuuuu17/llm-router-operator/pkg/router/inbound"
	"github.com/duuuuu17/llm-router-operator/pkg/router/outbound"
	"github.com/duuuuu17/llm-router-operator/pkg/router/pipeline"
	"github.com/duuuuu17/llm-router-operator/pkg/router/plugin"
	"go.opentelemetry.io/otel/trace"
)

// 过滤器
type BackendSelectorRegistry interface {
	GetFilter(strategy string) (filters.CandidatesFilter, error)
}

// 通过inbound适配器接口的行为函数，来获取适配器实例
type InboundRegistry interface {
	GetAdapter(req *http.Request) (inbound.InboundAdapter, error)
}

type OutboundRegistry interface {
	GetAdapter(backendType []string) (outbound.OutboundAdapter, error)
}

type PluginRegistry interface {
	GetPlugin(name string) (plugin.Plugin, error)
}

// 构建错误上下文，并利用errors.Is自动匹配错误函数执行处理
type ErrorsRegistry interface {
	HandleErrorFunc(ce *errs.ContextErr, err error)
}
type Forward interface {
	Do(*http.Request) (*http.Response, error)
}
type CoreHandlers struct {
	Parse         pipeline.HandlerFunc
	GetCandidates pipeline.HandlerFunc
	GetFilter     pipeline.HandlerFunc
	BuildRequest  pipeline.HandlerFunc
	Forward       pipeline.HandlerFunc
	HandleResp    pipeline.HandlerFunc
}
type Router struct {
	// 	cfgs            config.RouterConfig
	// backendSelector BackendSelectorRegistry
	// inbounds        InboundRegistry  // router 获取inbound适配器
	// outbound        OutboundRegistry // router获取outbound适配器
	// forwarder       Forward
	coreHandlers *CoreHandlers
	errhandlers  ErrorsRegistry
	routerChain  atomic.Value // *Chain[HandlerFunc]

	// 租户隔离,多租户管线为immutable
	tenants atomic.Value   // *TenantPipelines
	plugins PluginRegistry // 插件注册表
	bus     *eventbus.Bus
}

// 由main函数调用的初始化函数
// 初始化参数聚合，更符合后续工程项目的测试要爱方便
type RouterDeps struct {
	Configs         config.RouterConfig
	BackendSelector BackendSelectorRegistry
	Inbound         InboundRegistry
	Outbound        OutboundRegistry
	Forward         Forward
	ErrsHandleMap   ErrorsRegistry
	RouterChain     atomic.Value
	// 租户隔离,多租户管线为immutable
	Tenants atomic.Value   // *TenantPipelines
	Plugins PluginRegistry // 插件注册表
	Bus     *eventbus.Bus
}

func NewRouter(dep RouterDeps) *Router {

	inboundParseHandler := InboundAdapterParseHandler{Inbounds: dep.Inbound}
	getCandidatesHandler := GetCandidatesHandler{Cfgs: dep.Configs}
	getFilterHandler := GetFilterHandler{BackendSelector: dep.BackendSelector}
	buildHTTPRequestHandler := BuildHTTPRequestHandler{Outbound: dep.Outbound}
	forwardHTTPRequestHandler := ForwardHTTPRequestHandler{Forwarder: dep.Forward}
	handleBackendResponseHandler := HandleBackendResponseHandler{Outbound: dep.Outbound}

	router := &Router{
		coreHandlers: &CoreHandlers{
			Parse:         inboundParseHandler.Handle,
			GetCandidates: getCandidatesHandler.Handle,
			GetFilter:     getFilterHandler.Handle,
			BuildRequest:  buildHTTPRequestHandler.Handle,
			Forward:       forwardHTTPRequestHandler.Handle,
			HandleResp:    handleBackendResponseHandler.Handle,
		},
		errhandlers: dep.ErrsHandleMap,
		routerChain: dep.RouterChain,
		tenants:     dep.Tenants,
		plugins:     dep.Plugins,
		bus:         dep.Bus,
	}
	return router
}

// func NewRouter(dep RouterDeps) *Router {
// 	chain := pipeline.NewPipeline[pipeline.HandlerFunc]()
// 	inboundParseHandler := InboundAdapterParseHandler{Inbounds: dep.Inbound}
// 	getCandidatesHandler := GetCandidatesHandler{Cfgs: dep.Configs}
// 	getFilterHandler := GetFilterHandler{BackendSelector: dep.BackendSelector}
// 	buildHTTPRequestHandler := BuildHTTPRequestHandler{Outbound: dep.Outbound}
// 	forwardHTTPRequestHandler := ForwardHTTPRequestHandler{Forwarder: dep.Forward}
// 	handleBackendResponseHandler := HandleBackendResponseHandler{Outbound: dep.Outbound}
// 	chain.Use(
// 		inboundParseHandler.Handle,
// 		getCandidatesHandler.Handle,
// 		getFilterHandler.Handle,
// 		buildHTTPRequestHandler.Handle,
// 		forwardHTTPRequestHandler.Handle,
// 		handleBackendResponseHandler.Handle,
// 	)
// 	router := &Router{
// 		// 		cfgs:            dep.Configs,
// 		// backendSelector: dep.BackendSelector,
// 		// outbound:        dep.Outbound,
// 		// inbounds:        dep.Inbound,
// 		// forwarder:       dep.Forward,
// 		errhandlers: dep.ErrsHandleMap,
// 		routerChain: dep.RouterChain,
// 		tenants:     dep.Tenants,
// 		plugins:     dep.Plugins,
// 	}
// 	router.routerChain.Store(chain)
// 	return router
// }

// 实际执行Http处理逻辑
// func (r *Router) HandleFunc(w http.ResponseWriter, req *http.Request) {
// 	// NOTE: 当不存在tracer或span时，从spanFromContext获取trace信息会返回noop类型的对象表示不需要操作
// 	rootSpan := trace.SpanFromContext(req.Context())
// 	rootSpan.AddEvent("router receive the client request")
// 	slog.Info("reqeust received",
// 		"request-id", req.Header.Get("X-request-id"),
// 		"method", req.Method,
// 		"path", req.URL.Path,
// 	)
// 	// 执行逻辑
// 	// 查询匹配的inboundAdapter
// 	inboundAdapter, err := r.inbounds.GetAdapter(req)
// 	if err != nil {
// 		slog.Warn("no inbound adapter matched!",
// 			"request-id", req.Header.Get("X-request-id"),
// 			"metbod", req.Method,
// 			"path", req.URL.Path,
// 		)
// 		r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
// 		return
// 	}
// 	// 获取中间态请求对象
// 	llmRequest, err := inboundAdapter.Parse(req)
// 	if err != nil {
// 		slog.Error("convert to IR error!",
// 			"request-id", req.Header.Get("X-request-id"),
// 		)
// 		return
// 	}
// 	// 根据中间态请求对象和配置参数获取候选后端服务
// 	rootSpan.AddEvent("select the backend")
// 	candidates, err := core.FilterCandidates(llmRequest, r.cfgs)
// 	if err != nil {
// 		slog.Warn("backend selected",
// 			"request-id", req.Header.Get("X-request-id"),
// 			"model", llmRequest.Model,
// 		)
// 		r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
// 		return
// 	}
// 	// 业务逻辑过滤Service级，得到选择的候选者
// 	// 当前仅支持Canary, AlwaysPickFirst
// 	strategy := core.ResolveStrategy(llmRequest)
// 	podEndpointFilter, err := r.backendSelector.GetFilter(strategy)
// 	if err != nil {
// 		slog.Warn("got selector failed",
// 			"request-id", req.Header.Get("X-request-id"),
// 			"model", llmRequest.Model,
// 			"strategy", strategy,
// 		)
// 		r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
// 		return
// 	}
// 	// 通过策略选择器实例获取对应的backend信息
// 	backendConfig, err := podEndpointFilter.Filter(candidates)
// 	if err != nil {
// 		slog.Warn("backend selected",
// 			"request-id", req.Header.Get("X-request-id"),
// 			"model", llmRequest.Model,
// 			"strategy", strategy,
// 		)
// 		r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
// 		return
// 	}
// 	// 选择合适的适配器准备转发请求
// 	// 获取对应后端服务的适配器
// 	outboundAdapter, err := r.outbound.GetAdapter(backendConfig.Capabilty.Protocols)
// 	if err != nil {
// 		slog.Error("not sufficient adapte",
// 			"request-id", req.Header.Get("X-request-id"),
// 			"model", backendConfig.Capabilty.Models,
// 		)
// 		r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
// 		return
// 	}
// 	// 实际最终的Endpoint级选择应该在构建HTTP Request时调用
// 	// 构建适合后端Pod的请求
// 	backendRequest, err := outboundAdapter.BuildHTTPRequest(req.Context(), llmRequest, backendConfig)
// 	if err != nil {
// 		rootSpan.SetStatus(codes.Error, "build http request error!")
// 		slog.Error("build http request error!",
// 			"request-id", req.Header.Get("X-request-id"),
// 			"model", backendConfig.Capabilty.Models[0],
// 			"error", err,
// 		)
// 		r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
// 		return
// 	}
// 	// 转发请求.不许要使用到客户端请求的上下文，是因为在构建转发请求时就已经使用
// 	backendRequestStart := time.Now()
// 	resp, err := r.forwarder.Do(backendRequest)
// 	duration := time.Since(backendRequestStart).Seconds()
// 	metrics.BackendRequestDuration.WithLabelValues(req.Host, backendConfig.Capabilty.Protocols[0]).Observe(duration)
// 	if err != nil {
// 		slog.Error("forward request failed",
// 			"request-id", req.Header.Get("X-request-id"),
// 			"backend", backendConfig.Capabilty.Models[0],
// 			"error", err,
// 		)
// 		r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
// 	}
// 	// 处理后端Pod返回的请求并转发回给客户端
// 	ctx := req.Context()
// 	ctx = context.WithValue(req.Context(), "x-model", llmRequest.Model)
// 	req = req.WithContext(ctx)
// 	err = outboundAdapter.HandleResponse(req.Context(), w, resp)
// 	if err != nil {
// 		switch err {
// 		case errs.ErrBackend5xx:
// 			slog.Error("backend service failed",
// 				"backend", backendConfig.Capabilty.Models[0],
// 				"status", resp.StatusCode,
// 			)
// 		case errs.ErrBackend4xx:
// 			slog.Warn("backend rejected rquest",
// 				"backend", backendConfig.Capabilty.Models[0],
// 				"status", resp.StatusCode,
// 			)
// 		}
// 		r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
// 	}
// 	// 请求处理完毕，需要记录到指标中
// 	metrics.HTTPRequestTotal.WithLabelValues(
// 		req.Method, metrics.GetPathTemplate(req.URL.Path), resp.Status,
// 		strconv.FormatBool(errors.Is(err, errs.ErrClientCancel))).Inc()
// 	rootSpan.AddEvent("model pod inference result response to client!")
// }

//	func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
//		// NOTE: 当不存在tracer或span时，从spanFromContext获取trace信息会返回noop类型的对象表示不需要操作
//		rootSpan := trace.SpanFromContext(req.Context())
//		rootSpan.AddEvent("router receive the client request")
//		slog.Info("reqeust received",
//			"request-id", req.Header.Get("X-request-id"),
//			"method", req.Method,
//			"path", req.URL.Path,
//		)
//		chain := r.routerChain.Load().(*pipeline.Pipeline[pipeline.HandlerFunc])
//		chainCtx := pipeline.ChainContext{
//			Req:        req,
//			RespWriter: w,
//			Handlers:   chain.Handlers,
//			Index:      -1,
//		}
//		if err := chainCtx.Next(); err != nil {
//			r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
//			return
//		}
//		chainCtx.RespWriter.Header().Set("Content-Type", "application/json")
//		chainCtx.RespWriter.WriteHeader(200)
//		chainCtx.RespWriter.Write([]byte(`{"content":"test success"}`))
//		// 请求处理完毕，需要记录到指标中
//		rootSpan.AddEvent("model pod inference result response to client!")
//	}

// router模块循环操作，常见用于订阅监听的事件，调用处理等等
func (r *Router) Start(ctx context.Context) {
	ch := r.bus.Subscriber("tenant_config.reloaded") // 订阅多租户配置
	go func() {
		for {
			select {
			case data := <-ch:
				r.Reload(ctx, data.(*config.GlobalConfig))
			case <-ctx.Done():
				return
			}
		}
	}()
}
func (r *Router) Reload(ctx context.Context, tenantCfg *config.GlobalConfig) {
	newTenant := pipeline.NewTenantPipelines()
	for tenantID, tenantCfg := range tenantCfg.TenantCfg {
		chain, err := r.BuildPipeline(tenantCfg.Pipelines, r.plugins)
		if err != nil {
			slog.Error("can't reload tenantsConfig", "err", err.Error())
			return
		}
		newTenant.AddOrUpdateTenantPipeline(tenantID, chain)
	}

}
func resolveTenant(req *http.Request) (string, error) {
	id := req.Header.Get("X-LLM-TenantID")
	if id == "" {
		return "", errs.ErrNotSupportedTenant
	}
	return id, nil
}

// 采取固定+插槽式Plugin的混合模式处理链
// Parse → [PreRouting Plugins] → Route → [PostRouting Plugins] → BuildRequest → Forward → HandleResponse
func (r *Router) BuildPipeline(steps config.PipelineConfig, registry PluginRegistry) (*pipeline.Pipeline[pipeline.HandlerFunc], error) {
	chain := pipeline.NewPipeline[pipeline.HandlerFunc]()
	// 1. Parse
	chain.Use(r.coreHandlers.Parse)
	// 2. pre-routing plugins
	for _, step := range steps.PreRouting {
		plugin, err := registry.GetPlugin(step.Name)
		if err != nil {
			return nil, err
		}
		h, err := plugin.Build(step.Config)
		if err != nil {
			return nil, err
		}
		chain.Use(h)
	}
	// 3. core routing
	chain.Use(r.coreHandlers.GetCandidates)
	chain.Use(r.coreHandlers.GetFilter)

	// 4. post-routing plugins
	for _, step := range steps.PostRouting {
		plugin, err := registry.GetPlugin(step.Name)
		if err != nil {
			return nil, err
		}
		h, err := plugin.Build(step.Config)
		if err != nil {
			return nil, err
		}
		chain.Use(h)
	}
	// 5. forward partial
	chain.Use(r.coreHandlers.BuildRequest)
	chain.Use(r.coreHandlers.Forward)
	chain.Use(r.coreHandlers.HandleResp)
	return chain, nil
}
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// NOTE: 当不存在tracer或span时，从spanFromContext获取trace信息会返回noop类型的对象表示不需要操作
	rootSpan := trace.SpanFromContext(req.Context())
	rootSpan.AddEvent("router receive the client request")
	slog.Info("reqeust received",
		"request-id", req.Header.Get("X-request-id"),
		"method", req.Method,
		"path", req.URL.Path,
	)

	chain := r.tenants.Load().(*pipeline.TenantPipelines)
	tenantID, err := resolveTenant(req)
	if err != nil {
		r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
		return
	}
	handlersChain, err := chain.GetTenantPipeline(tenantID)
	if err != nil {
		r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
		return
	}
	chainCtx := pipeline.ChainContext{
		Req:        req,
		RespWriter: w,
		Handlers:   handlersChain.Handlers,
		Index:      -1,
	}
	if err := chainCtx.Next(); err != nil {
		r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
		return
	}

	chainCtx.RespWriter.Header().Set("Content-Type", "application/json")
	chainCtx.RespWriter.WriteHeader(200)
	chainCtx.RespWriter.Write([]byte(`{"content":"test success"}`))
	// 请求处理完毕，需要记录到指标中
	rootSpan.AddEvent("model pod inference result response to client!")
}
