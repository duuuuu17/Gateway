package handler

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/metrics"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
	"github.com/duuuuu17/llm-router-operator/pkg/router/filters"
	"github.com/duuuuu17/llm-router-operator/pkg/router/inbound"
	"github.com/duuuuu17/llm-router-operator/pkg/router/outbound"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type Router struct {
	cfgs            config.RouterConfig
	backendSelector BackendSelectorRegistry
	inbounds        InboundRegistry  // router 获取inbound适配器
	outbound        OutboundRegistry // router获取outbound适配器
	forwarder       core.Forward
	errhandlers     ErrorsRegistry
}

type BackendSelectorRegistry interface {
	GetFilter(strategy string) (filters.CandidatesFilter, error)
}

// 通过inbound适配器接口的行为函数，来获取适配器实例
type InboundRegistry interface {
	GetAdapter(req *http.Request) (inbound.InboundAdapter, error)
}

// 通过outbound适配器接口的行为函数，来获取适配器实例
type OutboundRegistry interface {
	GetAdapter(backendType []string) (outbound.OutboundAdapter, error)
}
type ErrorsRegistry interface {
	HandleErrorFunc(ce *core.ContextErr, err error)
}

// 由main函数调用的初始化函数
// 初始化参数聚合，更符合后续工程项目的测试要爱方便
type RouterDeps struct {
	Configs         config.RouterConfig
	BackendSelector BackendSelectorRegistry
	Inbound         InboundRegistry
	Outbound        OutboundRegistry
	Forward         core.Forward
	ErrsHandleMap   ErrorsRegistry
}

func NewRouter(dep RouterDeps) *Router {
	return &Router{
		cfgs:            dep.Configs,
		backendSelector: dep.BackendSelector,
		outbound:        dep.Outbound,
		inbounds:        dep.Inbound,
		forwarder:       dep.Forward,
		errhandlers:     dep.ErrsHandleMap,
	}
}

// 实际执行Http处理逻辑
func (r *Router) HandleFunc(w http.ResponseWriter, req *http.Request) {

	// NOTE: 当不存在tracer或span时，从spanFromContext获取trace信息会返回noop类型的对象表示不需要操作
	rootSpan := trace.SpanFromContext(req.Context())
	rootSpan.AddEvent("router receive the client request")
	slog.Info("reqeust received",
		"request-id", req.Header.Get("X-request-id"),
		"method", req.Method,
		"path", req.URL.Path,
	)

	// 执行逻辑
	// 查询匹配的inboundAdapter
	inboundAdapter, err := r.inbounds.GetAdapter(req)
	if err != nil {
		slog.Warn("no inbound adapter matched!",
			"request-id", req.Header.Get("X-request-id"),
			"metbod", req.Method,
			"path", req.URL.Path,
		)
		r.errhandlers.HandleErrorFunc(&core.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
		return
	}
	// 获取中间态请求对象
	llmRequest, err := inboundAdapter.Parse(req)
	if err != nil {
		slog.Error("convert to IR error!",
			"request-id", req.Header.Get("X-request-id"),
		)
		return
	}

	// 根据中间态请求对象和配置参数获取候选后端服务
	rootSpan.AddEvent("select the backend")
	candidates, err := core.FilterCandidates(llmRequest, r.cfgs)
	if err != nil {
		slog.Warn("backend selected",
			"request-id", req.Header.Get("X-request-id"),
			"model", llmRequest.Model,
		)
		r.errhandlers.HandleErrorFunc(&core.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
		return
	}
	// 业务逻辑过滤Service级，得到选择的候选者
	// 当前仅支持Canary, AlwaysPickFirst
	strategy := core.ResolveStrategy(llmRequest)
	podEndpointFilter, err := r.backendSelector.GetFilter(strategy)
	if err != nil {
		slog.Warn("got selector failed",
			"request-id", req.Header.Get("X-request-id"),
			"model", llmRequest.Model,
			"strategy", strategy,
		)
		r.errhandlers.HandleErrorFunc(&core.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
		return
	}
	// 通过策略选择器实例获取对应的backend信息
	backendConfig, err := podEndpointFilter.Filter(candidates)
	if err != nil {
		slog.Warn("backend selected",
			"request-id", req.Header.Get("X-request-id"),
			"model", llmRequest.Model,
			"strategy", strategy,
		)
		r.errhandlers.HandleErrorFunc(&core.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
		return
	}
	// 选择合适的适配器准备转发请求
	// 获取对应后端服务的适配器
	outboundAdapter, err := r.outbound.GetAdapter(backendConfig.Capabilty.Protocols)
	if err != nil {
		slog.Error("not sufficient adapte",
			"request-id", req.Header.Get("X-request-id"),
			"model", backendConfig.Capabilty.Models,
		)
		r.errhandlers.HandleErrorFunc(&core.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
		return
	}
	// 实际最终的Endpoint级选择应该在构建HTTP Request时调用
	// 构建适合后端Pod的请求
	backendRequest, err := outboundAdapter.BuildHTTPRequest(req.Context(), llmRequest, backendConfig)
	if err != nil {
		rootSpan.SetStatus(codes.Error, "build http request error!")
		slog.Error("build http request error!",
			"request-id", req.Header.Get("X-request-id"),
			"model", backendConfig.Capabilty.Models[0],
			"error", err,
		)
		r.errhandlers.HandleErrorFunc(&core.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
		return
	}
	// 转发请求.不许要使用到客户端请求的上下文，是因为在构建转发请求时就已经使用
	backendRequestStart := time.Now()
	resp, err := r.forwarder.Do(backendRequest)
	duration := time.Since(backendRequestStart).Seconds()
	metrics.BackendRequestDuration.WithLabelValues(req.Host, backendConfig.Capabilty.Protocols[0]).Observe(duration)
	if err != nil {
		slog.Error("forward request failed",
			"request-id", req.Header.Get("X-request-id"),
			"backend", backendConfig.Capabilty.Models[0],
			"error", err,
		)
		r.errhandlers.HandleErrorFunc(&core.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
	}

	// 处理后端Pod返回的请求并转发回给客户端
	ctx := req.Context()
	ctx = context.WithValue(req.Context(), "x-model", llmRequest.Model)
	req = req.WithContext(ctx)

	err = outboundAdapter.HandleResponse(req.Context(), w, resp)
	if err != nil {
		switch err {
		case core.ErrBackend5xx:
			slog.Error("backend service failed",
				"backend", backendConfig.Capabilty.Models[0],
				"status", resp.StatusCode,
			)
		case core.ErrBackend4xx:
			slog.Warn("backend rejected rquest",
				"backend", backendConfig.Capabilty.Models[0],
				"status", resp.StatusCode,
			)
		}
		r.errhandlers.HandleErrorFunc(&core.ContextErr{Req: req, Span: rootSpan, ResponseWriter: w}, err)
	}
	// 请求处理完毕，需要记录到指标中
	metrics.HTTPRequestTotal.WithLabelValues(
		req.Method, metrics.GetPathTemplate(req.URL.Path), resp.Status,
		strconv.FormatBool(errors.Is(err, core.ErrClientCancel))).Inc()
	rootSpan.AddEvent("model pod inference result response to client!")
}
