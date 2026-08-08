package pipeline

import (
	"net/http"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
	"go.opentelemetry.io/otel/trace"
)

type ChainContext struct {
	// 原始请求
	Req        *http.Request
	RespWriter http.ResponseWriter
	RootSpan   trace.Span

	// 流水线中间态（由各 handler 填充）
	LLMRequest    *core.LLMRequest
	Candidates    []*config.RuntimeBackend
	BackendConfig *config.RuntimeBackend
	BackendReq    *http.Request
	BackendResp   *http.Response

	Aborted   bool
	Handlers  []HandlerFunc
	Index     int
	Tracer    trace.Tracer
	Responded bool
}

func (ctx *ChainContext) Next() error {
	ctx.Index++
	if ctx.Index >= len(ctx.Handlers) {
		return nil
	}
	if ctx.Aborted {
		return nil
	}
	return ctx.Handlers[ctx.Index](ctx)
}
func (ctx *ChainContext) Abort() {
	ctx.Aborted = true
}
