package outboundtest

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
	"github.com/duuuuu17/llm-router-operator/pkg/router/handler"
	"github.com/duuuuu17/llm-router-operator/pkg/router/inbound"
	"github.com/duuuuu17/llm-router-operator/pkg/router/outbound"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"
)

type fakeQwenCfg struct {
	config.RouterConfig
}

func NewFakeQwenCfg() *fakeQwenCfg {
	tmp := &config.Endpoint{}
	tmp.Address = "127.0.0.1:8080"
	tmp.Health.Store(true)
	tmp.ActiveConn.Store(0)
	tmp2 := &config.Endpoint{}
	tmp2.Address = "127.0.0.2:8080"
	tmp2.Health.Store(true)
	tmp2.ActiveConn.Store(0)
	r := config.InitEndpointLevelSelectorRegistry()
	selector, _ := r.New("least_conn")
	fakeCfg := &fakeQwenCfg{config.RouterConfig{}}
	fakeBackend := make(map[string]*config.RuntimeBackend)
	fakeBackend["test"] = &config.RuntimeBackend{
		Name: "test",
		Capabilty: config.BackendCapability{
			Models:           []string{"qwen"},
			Endpoints:        []*config.Endpoint{tmp, tmp2},
			Protocols:        []string{"openai"},
			EnabledStreaming: true,
		},
		Routing: config.BackendRouting{
			Weight: 50,
			// Selectors:   []string{"canary"},
		},
		EndpointSelector: selector,
	}
	fakeBackend["test"].Endpoints.Store([]*config.Endpoint{tmp, tmp2})
	fakeCfg.Backends.Store(fakeBackend)
	return fakeCfg
}

func TestPickOpenAIAdapter(t *testing.T) {

	reg := outbound.NewOutBoundAdapterRegistry()
	reg.AddOutboundRegistry("openai", outbound.NewOpenAIOutBoundAdapter())

	backendCfg := NewFakeQwenCfg()
	adapter, err := reg.GetAdapter(backendCfg.GetConfig()["test"].Capabilty.Protocols)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := adapter.(*outbound.OpenAIOutBoundAdapter); !ok {
		t.Fatal("is not openai adapter!")
	}
}
func TestOpenAIBuildHTTPReqeust(t *testing.T) {
	a := true
	headers := http.Header{}
	headers.Set("Content-Type", "application/json,charset=utf-8")
	r := &core.LLMRequest{
		Model:    "qwen",
		Messages: []*core.Message{{Role: "user", Content: "hello"}},
		Stream:   &a,
		Headers:  headers,
		Parameters: map[string]any{
			"temperatures": 0.6,
			"max_tokens":   64,
		},
	}
	backendCfg := NewFakeQwenCfg()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := outbound.NewOpenAIOutBoundAdapter()
	if _, ok := adapter.(*outbound.OpenAIOutBoundAdapter); !ok {
		t.Fatal("get adaper was failed,not matched one!")
	}
	req, err := adapter.BuildHTTPRequest(ctx, r, backendCfg.GetConfig()["test"])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(req.Header.Get("Content-Type"), "application/json") {
		t.Fatal(err)
	}
	t.Logf("request URL:%s", req.Host+req.URL.Path)
	cfg := backendCfg.GetConfig()["test"]
	e, err := cfg.EndpointSelector.Select(cfg.Capabilty.Endpoints)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(e.Address)

}

func TestOpenAIHandleResponse(t *testing.T) {
	adapter := outbound.NewOpenAIOutBoundAdapter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := httptest.NewRecorder() // httptest.ResponseRecorder 作为处理response消息的接收者

	headers := http.Header{}
	headers.Set("Content-Type", "application/json")
	// headers.Set("Content-Type","text/event-stream")
	body := `{
		"model": "qwen",
		"protocol": "openai",
		"response": "hello",
		"temperature": 0.7,
		"stream": true
	}`
	resp := &http.Response{StatusCode: 200, Header: headers, Body: io.NopCloser(strings.NewReader(body))}
	err := adapter.HandleResponse(ctx, w, resp)
	assert.NoError(t, err)
	assert.Equal(t, 200, w.Code)
	assert.Contains(t, w.Body.String(), `"response": "hello"`)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

}

// ChainContext 责任链上下文，承载整条链的输入输出
type ChainContext struct {
	// 原始请求
	Req        *http.Request
	RespWriter http.ResponseWriter
	Span       trace.Span

	// 流水线中间态（由各 handler 填充）
	LLMRequest    *core.LLMRequest
	Candidates    []*config.RuntimeBackend
	BackendConfig *config.RuntimeBackend
	BackendReq    *http.Request
	BackendResp   *http.Response

	Aborted bool

	Chain []HandlerFunc
	Index int
}

func (ctx *ChainContext) Next() error {
	ctx.Index++
	if ctx.Index >= len(ctx.Chain) {
		return nil
	}
	if ctx.Aborted {
		return nil
	}
	return ctx.Chain[ctx.Index](ctx)
}
func (ctx *ChainContext) Abort() {
	ctx.Aborted = true
}

type HandlerFunc func(ctx *ChainContext) error

var NoopHandler HandlerFunc = func(ctx *ChainContext) error { return nil }

type Chain[T any] struct {
	Handlers []T
}

func NewChain[T any]() *Chain[T] {
	return &Chain[T]{Handlers: make([]T, 0)}
}
func (c *Chain[T]) Use(h ...T) {
	c.Handlers = append(c.Handlers, h...)
}

type Router struct {
	RouterChain atomic.Value
	// 依赖注入（由 Router 传入，供各 handler 使用）
	Outbound        handler.OutboundRegistry
	BackendSelector handler.BackendSelectorRegistry
	Forwarder       handler.Forward
	ErrHandlers     handler.ErrorsRegistry
	Configs         config.RouterConfig
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	chain := r.RouterChain.Load().(*Chain[HandlerFunc])
	ctx := ChainContext{
		Req:        req,
		RespWriter: w,
		Chain:      chain.Handlers,
		Index:      -1,
	}
	if err := ctx.Next(); err != nil {
		ctx.RespWriter.Header().Set("Content-Type", "application/json")
		ctx.RespWriter.WriteHeader(500)
		ctx.RespWriter.Write([]byte(`{"content":"test failure"}`)) //nolint
		return
	}
	ctx.RespWriter.Header().Set("Content-Type", "application/json")
	ctx.RespWriter.WriteHeader(200)
	ctx.RespWriter.Write([]byte(`{"content":"test success"}`)) //nolint
}

type InboundRegistryHandler struct {
	Inbounds handler.InboundRegistry
}

func (r *InboundRegistryHandler) ParseHandler(ctx *ChainContext) error {

	slog.Info("execute parse!")
	adapter, err := r.Inbounds.GetAdapter(ctx.Req)
	if err != nil {
		slog.Warn("no inbound adapter matched", "path", ctx.Req.URL.Path)
		// ctx.Errhandlers.HandleErrorFunc(toContextErr(ctx), err)
		return err
	}
	llmReq, err := adapter.Parse(ctx.Req)
	if err != nil {
		// ctx.ErrHandlers.HandleErrorFunc(toContextErr(ctx), err)
		return err
	}
	ctx.LLMRequest = llmReq

	return ctx.Next()
}
func TestParseHandler(t *testing.T) {
	router := Router{}
	inboundsRegistry := inbound.NewInboundAdapterRegistry()
	inboundsRegistry.AddInboundAdapter("openai", inbound.NewOpenAIInBoundAdapter())
	inbound := InboundRegistryHandler{inboundsRegistry}
	newBaseChain := NewChain[HandlerFunc]()
	newBaseChain.Use(inbound.ParseHandler)
	router.RouterChain.Store(newBaseChain)
	body := `{
		"model": "qwen",
		"protocol": "openai",
		"messages": [{"role":"system","content":"you need help user"},{"role":"user","content":"hi"}],
		"temperature": 0.7,
		"stream": false
	}`
	server := httptest.NewServer(&router)
	defer server.Close()
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("response:%+v ", resp)
}
