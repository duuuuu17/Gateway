package router_test

import (
	"context"
	"control-plane-model-test/pkg/config"
	"control-plane-model-test/pkg/router/core"
	"control-plane-model-test/pkg/router/inbound"
	"control-plane-model-test/pkg/router/outbound"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func TestRouter_OutboundAdapterBuildHTTPRequest(t *testing.T) {
	body := `{
		"model": "qwem2.5",
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
	if llmReq.Model != "qwem2.5" {
		t.Fatalf("unexpected parse:%+v", llmReq)
	}
	// 加载配置文件信息
	path := "/tmp/config.yaml"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storage, err := config.Initialization(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	// 调用config模块进行主动初始化
	// loader := config.NewYAMLLoader(path)
	storages := []config.ConfigReader{storage}
	backendCfg, err := core.SelectBackend(llmReq, storages)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Parse:%+v ", backendCfg)
	outboundReg := outbound.NewOutBoundAdapterRegistry()
	outboundReg.AddOutboundRegistry("openai", outbound.NewOpenAIOutBoundAdapter())
	outboundAdapter, err := outboundReg.GetAdapter(backendCfg.GetConfig().Protocol)
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
		"model": "qwem2.5",
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
	if llmReq.Model != "qwem2.5" {
		t.Fatalf("unexpected parse:%+v", llmReq)
	}
	// 加载配置文件信息
	path := "/tmp/config.yaml"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storage, err := config.Initialization(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	// 调用config模块进行主动初始化
	// loader := config.NewYAMLLoader(path)
	storages := []config.ConfigReader{storage}
	backendCfg, err := core.SelectBackend(llmReq, storages)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Parse:%+v ", backendCfg)
	outboundReg := outbound.NewOutBoundAdapterRegistry()
	outboundReg.AddOutboundRegistry("openai", outbound.NewOpenAIOutBoundAdapter())
	outboundAdapter, err := outboundReg.GetAdapter(backendCfg.GetConfig().Protocol)
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
	forwardClient := core.NewHTTPForward()
	resp, err := forwardClient.Do(httpReq)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("response:%+v ", resp)
}
