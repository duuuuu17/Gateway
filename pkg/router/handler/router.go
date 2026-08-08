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
	// v1alpha: not tenants
	routerChain atomic.Value // *Chain[HandlerFunc]

	// 租户隔离,多租户管线为immutable
	// support tenants
	tenants atomic.Pointer[pipeline.TenantPipelines] // atomic.Pointer[map[string]*pipeline.Pipeline[pipeline.HandlerFunc]] // *TenantPipelines
	// tenants atomic.Pointer[pipeline.TenantPipelines]
	plugins PluginRegistry // 插件注册表
	bus     *eventbus.Bus
}

// 由main函数调用的初始化函数
// 初始化参数聚合，更符合后续工程项目的测试要爱方便
type RouterDeps struct {
	Configs         *config.RouterConfig
	BackendSelector BackendSelectorRegistry
	Inbound         InboundRegistry
	Outbound        OutboundRegistry
	Forward         Forward
	ErrsHandleMap   ErrorsRegistry
	RouterChain     atomic.Value
	// 租户隔离,多租户管线为immutable
	// Tenants *pipeline.TenantPipelines // *TenantPipelines
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
		tenants:     atomic.Pointer[pipeline.TenantPipelines]{},
		plugins:     dep.Plugins,
		bus:         dep.Bus,
	}
	router.tenants.Store(pipeline.NewTenantPipelines())
	return router
}

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

	newTenant.CopyFromOldMap(r.tenants.Load())
	// slog.Info("reload start time", "[start-time]", time.Now())
	// Delta update
	for tenantID, tenantCfg := range tenantCfg.TenantCfg {
		chain, err := r.BuildPipeline(tenantCfg.Pipelines, r.plugins)
		if err != nil {
			slog.ErrorContext(ctx, "can't reload tenantsConfig", "err", err.Error())
			continue
		}
		newTenant.AddOrUpdateTenantPipeline(tenantID, chain)
	}
	// COW
	r.tenants.Store(newTenant)
	// slog.Info("reload end time", "[end-time]", time.Now())
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
func (r *Router) BuildPipeline(steps *config.PipelineConfig, registry PluginRegistry) (*pipeline.Pipeline[pipeline.HandlerFunc], error) {
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
	slog.InfoContext(req.Context(), "reqeust received",
		"request-id", req.Header.Get("X-request-id"),
		"method", req.Method,
		"path", req.URL.Path,
	)
	chain := r.tenants.Load()
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
		RootSpan:   rootSpan,
	}
	slog.Info("go into chain handler!", "host", req.Host)
	if err := chainCtx.Next(); err != nil {
		switch err {
		case errs.ErrClientCancel:
			slog.InfoContext(req.Context(), "client disconnectted",
				"req_id", req.Header.Get("X-request-id"),
			)
		case context.Canceled:
			slog.InfoContext(req.Context(), "client disconnectted",
				"req_id", req.Header.Get("X-request-id"),
			)
		case errs.ErrBackendCantUse:
			r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, errs.ErrBackend5xx)
		default:
			slog.InfoContext(req.Context(), "handler's chain error!", "[error]", err.Error())
			r.errhandlers.HandleErrorFunc(&errs.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
		}
		return
	}

	// 请求处理完毕，需要记录到指标中
	rootSpan.AddEvent("model pod inference result response to client!")
}
