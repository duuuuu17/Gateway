package router_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/duuuuu17/llm-router-operator/pkg/router/filters"
	"github.com/duuuuu17/llm-router-operator/pkg/router/inbound"
	router "github.com/duuuuu17/llm-router-operator/test/integration/router_test"
	"github.com/stretchr/testify/assert"

	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
	"github.com/duuuuu17/llm-router-operator/pkg/router/outbound"
)

func TestRouter_OutboundAdapterBuildHTTPRequest(t *testing.T) {
	body := `{
		"model": "qwen2.5",
		"messages": [{"role":"system","content":"you are a helpful person"},{"role":"user","content":"hi"}],
		"stream": true,
		"temperature": 0.7
	}`
	reg := inbound.NewInboundAdapterRegistry()
	reg.AddInboundAdapter("openai", inbound.NewOpenAIInBoundAdapter())

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-LLM-Routing-Strategy", "FirstPick")
	adapter, err := reg.GetAdapter(req)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := adapter.(*inbound.OpenAIInBoundAdapter); !ok {
		t.Fatal("wrong adapter selected")
	}
	llmReq, err := adapter.Parse(req)
	if err != nil {
		t.Fatal(err)
	}
	if llmReq.Model != "qwen2.5" {
		t.Fatalf("unexpected parse:%+v", llmReq)
	}
	// 加载配置文件信息
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storage := NewFakeQwenCfg()
	// 调用config模块进行主动初始化
	// loader := config.NewYAMLLoader(path)
	candidates, _ := core.FilterCandidates(llmReq, &storage.RouterConfig)
	strategy := core.ResolveStrategy(llmReq)
	// 检测策略选择
	if strategy != "FirstPick" {
		t.Fatal("fetch strategy is not FirstPick")
	}
	selectors := filters.NewSelectorRegistry()
	selectors.AddSelector("FirstPick", filters.NewPickFirstFilterPlicy())
	sele, err := selectors.GetFilter(strategy)
	if err != nil {
		t.Log("can't got selector")
	}
	// 检查selector选取
	if _, ok := sele.(*filters.PickFirstFilterPlicy); !ok {
		t.Fatal("selector is not FirstPick!")
	}

	backendCfg, err := sele.Filter(candidates)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Parse:%+v ", backendCfg)
	outboundReg := outbound.NewOutBoundAdapterRegistry()
	outboundReg.AddOutboundRegistry("openai", outbound.NewOpenAIOutBoundAdapter())
	outboundAdapter, err := outboundReg.GetAdapter(backendCfg.Capabilty.Protocols)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := outboundAdapter.(*outbound.OpenAIOutBoundAdapter); !ok {
		t.Fatal("not selected adapater,maybe registry error")
	}
	httpReq, err := outboundAdapter.BuildHTTPRequest(ctx, llmReq, backendCfg)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(httpReq.Header["Content-Type"], "application/json") {
		t.Fatal("not create correct http request")
	}

}

func TestRouter_Forward(t *testing.T) {
	body := `{
		"model": "qwen2.5",
		"messages": [{"role":"system","content":"you are a helpful person"},{"role":"user","content":"hi"}],
		"stream": true,
		"temperature": 0.7
	}`
	reg := inbound.NewInboundAdapterRegistry()
	reg.AddInboundAdapter("openai", inbound.NewOpenAIInBoundAdapter())

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-LLM-Routing-Strategy", "default")
	adapter, err := reg.GetAdapter(req)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := adapter.(*inbound.OpenAIInBoundAdapter); !ok {
		t.Fatal("wrong adapter selected")
	}
	llmReq, err := adapter.Parse(req)
	if err != nil {
		t.Fatal(err)
	}
	if llmReq.Model != "qwen2.5" {
		t.Fatalf("unexpected parse:%+v", llmReq)
	}
	// 加载配置文件信息
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storage := NewFakeQwenCfg()
	if err != nil {
		t.Fatal(err)
	}
	// 调用config模块进行主动初始化
	// loader := config.NewYAMLLoader(path)
	candidates, _ := core.FilterCandidates(llmReq, &storage.RouterConfig)
	strategy := core.ResolveStrategy(llmReq)
	selectors := filters.NewSelectorRegistry()
	selectors.AddSelector("FirstPick", filters.NewPickFirstFilterPlicy())
	sele, err := selectors.GetFilter(strategy)
	// 检查selector选取
	if _, ok := sele.(*filters.PickFirstFilterPlicy); !ok {
		t.Log("can't got selector")
	}
	backendCfg, err := sele.Filter(candidates)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Parse:%+v ", backendCfg)
	// 设置fake server
	server := router.FakeBackendServer()
	defer server.Close()
	backendCfg.Capabilty.Endpoints[0].Address = strings.TrimPrefix(server.URL, "http://")
	backendCfg.Capabilty.Endpoints[1].Address = strings.TrimPrefix(server.URL, "http://")
	outboundReg := outbound.NewOutBoundAdapterRegistry()
	outboundReg.AddOutboundRegistry("openai", outbound.NewOpenAIOutBoundAdapter())
	outboundAdapter, err := outboundReg.GetAdapter(backendCfg.Capabilty.Protocols)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := outboundAdapter.(*outbound.OpenAIOutBoundAdapter); !ok {
		t.Fatal("not selected adapater,maybe registry error")
	}
	httpReq, err := outboundAdapter.BuildHTTPRequest(ctx, llmReq, backendCfg)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(httpReq.Header["Content-Type"], "application/json") {
		t.Fatal("not create correct http request")
	}
	// forwardClient := core.NewHTTPForward()
	// resp, err := forwardClient.TestDo(httpReq)

	resp, err := server.Client().Do(httpReq)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("response:%+v ", resp)
	w := httptest.NewRecorder()
	err = outboundAdapter.HandleResponse(ctx, w, resp)
	assert.NoError(t, err)
	assert.Equal(t, 200, w.Code)
	assert.Contains(t, w.Body.String(), `{"content":"test success"}`)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
}
