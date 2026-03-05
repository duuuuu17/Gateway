package handler

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/metrics"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
	"github.com/duuuuu17/llm-router-operator/pkg/router/errs"
	"github.com/duuuuu17/llm-router-operator/pkg/router/pipeline"
	"go.opentelemetry.io/otel/codes"
)

type InboundAdapterParseHandler struct {
	Inbounds InboundRegistry
}

func (r *InboundAdapterParseHandler) Handle(ctx *pipeline.ChainContext) error {
	inboundAdapter, err := r.Inbounds.GetAdapter(ctx.Req)
	if err != nil {
		slog.Warn("no inbound adapter matched!",
			"request-id", ctx.Req.Header.Get("X-request-id"),
			"metbod", ctx.Req.Method,
			"path", ctx.Req.URL.Path,
		)
		return err
	}
	// 获取中间态请求对象
	llmRequest, err := inboundAdapter.Parse(ctx.Req)
	if err != nil {
		slog.Error("convert to IR error!",
			"request-id", ctx.Req.Header.Get("X-request-id"),
		)
		return err
	}
	ctx.LLMRequest = llmRequest
	return ctx.Next()
}

type GetCandidatesHandler struct {
	Cfgs config.RouterConfig
}

func (r *GetCandidatesHandler) Handle(ctx *pipeline.ChainContext) error {
	// 根据中间态请求对象和配置参数获取候选后端服务
	ctx.RootSpan.AddEvent("select the backend")
	candidates, err := core.FilterCandidates(ctx.LLMRequest, r.Cfgs)
	if err != nil {
		slog.Warn("backend selected",
			"request-id", ctx.Req.Header.Get("X-request-id"),
			"model", ctx.LLMRequest.Model,
		)
		return err
	}
	ctx.Candidates = candidates
	return ctx.Next()
}

type GetFilterHandler struct {
	BackendSelector BackendSelectorRegistry
}

func (r *GetFilterHandler) Handle(ctx *pipeline.ChainContext) error {

	// 业务逻辑过滤Service级，得到选择的候选者
	// 当前仅支持Canary, AlwaysPickFirst
	strategy := core.ResolveStrategy(ctx.LLMRequest)
	podEndpointFilter, err := r.BackendSelector.GetFilter(strategy)
	if err != nil {
		slog.Warn("got selector failed",
			"request-id", ctx.Req.Header.Get("X-request-id"),
			"model", ctx.LLMRequest.Model,
			"strategy", strategy,
		)
		return err
	}
	// 通过策略选择器实例获取对应的backend信息
	backendConfig, err := podEndpointFilter.Filter(ctx.Candidates)
	if err != nil {
		slog.Warn("backend selected",
			"request-id", ctx.Req.Header.Get("X-request-id"),
			"model", ctx.LLMRequest.Model,
			"strategy", strategy,
		)
		return err
	}
	ctx.BackendConfig = backendConfig
	return ctx.Next()
}

type BuildHTTPRequestHandler struct {
	Outbound OutboundRegistry // router获取outbound适配器
}

func (r *BuildHTTPRequestHandler) Handle(ctx *pipeline.ChainContext) error {
	outboundAdapter, err := r.Outbound.GetAdapter(ctx.BackendConfig.Capabilty.Protocols)
	if err != nil {
		slog.Error("not sufficient adapte",
			"request-id", ctx.Req.Header.Get("X-request-id"),
			"model", ctx.BackendConfig.Capabilty.Models,
		)
		return err
	}
	// 实际最终的Endpoint级选择应该在构建HTTP Request时调用
	// 构建适合后端Pod的请求
	backendRequest, err := outboundAdapter.BuildHTTPRequest(ctx.Req.Context(), ctx.LLMRequest, ctx.BackendConfig)
	if err != nil {
		ctx.RootSpan.SetStatus(codes.Error, "build http request error!")
		slog.Error("build http request error!",
			"request-id", ctx.Req.Header.Get("X-request-id"),
			"model", ctx.BackendConfig.Capabilty.Models[0],
			"error", err,
		)
		return err
	}
	ctx.BackendReq = backendRequest
	return ctx.Next()
}

type ForwardHTTPRequestHandler struct {
	Forwarder Forward
}

func (r *ForwardHTTPRequestHandler) Handle(ctx *pipeline.ChainContext) error {
	// 转发请求.不许要使用到客户端请求的上下文，是因为在构建转发请求时就已经使用
	backendRequestStart := time.Now()
	resp, err := r.Forwarder.Do(ctx.BackendReq)
	duration := time.Since(backendRequestStart).Seconds()
	metrics.BackendRequestDuration.WithLabelValues(ctx.BackendReq.Host, ctx.BackendConfig.Capabilty.Protocols[0]).Observe(duration)
	if err != nil {
		slog.Error("forward request failed",
			"request-id", ctx.Req.Header.Get("X-request-id"),
			"backend", ctx.BackendConfig.Capabilty.Models[0],
			"error", err,
		)
		return err
	}
	ctx.BackendResp = resp
	return ctx.Next()
}

type ForwardBackendResponseHandler struct {
	Forwarder Forward
}
type HandleBackendResponseHandler struct {
	Outbound OutboundRegistry
}

func (r *HandleBackendResponseHandler) Handle(ctx *pipeline.ChainContext) error {
	outboundAdapter, err := r.Outbound.GetAdapter(ctx.BackendConfig.Capabilty.Protocols)
	if err != nil {
		slog.Error("not sufficient adapte",
			"request-id", ctx.Req.Header.Get("X-request-id"),
			"model", ctx.BackendConfig.Capabilty.Models,
		)
		return err
	}
	ctx2 := ctx.Req.Context()
	ctx2 = context.WithValue(ctx.Req.Context(), "x-model", ctx.LLMRequest.Model)
	ctx.Req = ctx.Req.WithContext(ctx2)

	err = outboundAdapter.HandleResponse(ctx.Req.Context(), ctx.RespWriter, ctx.BackendResp)
	if err != nil {
		switch err {
		case errs.ErrBackend5xx:
			slog.Error("backend service failed",
				"backend", ctx.BackendConfig.Capabilty.Models[0],
				"status", ctx.BackendResp.StatusCode,
			)
		case errs.ErrBackend4xx:
			slog.Warn("backend rejected rquest",
				"backend", ctx.BackendConfig.Capabilty.Models[0],
				"status", ctx.BackendResp.StatusCode,
			)
		}
		return err
	}
	// 请求处理完毕，需要记录到指标中
	metrics.HTTPRequestTotal.WithLabelValues(
		ctx.Req.Method, metrics.GetPathTemplate(ctx.Req.URL.Path), ctx.BackendResp.Status,
		strconv.FormatBool(errors.Is(err, errs.ErrClientCancel))).Inc()

	ctx.RootSpan.AddEvent("model pod inference result response to client!")
	return ctx.Next()
}
