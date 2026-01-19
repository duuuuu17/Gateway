package router_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
	"github.com/duuuuu17/llm-router-operator/pkg/router/filters"
	"github.com/duuuuu17/llm-router-operator/pkg/router/inbound"
	"github.com/duuuuu17/llm-router-operator/pkg/router/outbound"
)

func TestRouter_SelectBackend(t *testing.T) {
	body := `{
		"model": "qwen2.5",
		"messages": [{"role":"system","content":"you are a helpful person"},{"role":"user","content":"hi"}],
		"stream": false,
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
	req.Header.Set("X-LLM-Routing-Strategy", "Canary")

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
	path := "./tmp/config.yaml"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storage, err := config.Initialization(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	// 调用config模块进行主动初始化
	// loader := config.NewYAMLLoader(path)
	candidates, _ := core.FilterCandidates(llmReq, storage)
	strategy := core.ResolveStrategy(llmReq)
	selectors := filters.NewSelectorRegistry()
	selectors.AddSelector("FirstPick", filters.NewPickFirstFilterPlicy())
	selectors.AddSelector("Canary", filters.NewCanaryFilterPolicy())
	sele, err := selectors.GetSelector(strategy)
	if err != nil {
		t.Fatal("can't got selector")
	}
	if _, ok := sele.(*filters.CanaryFilterPolicy); !ok {
		t.Fatal("selector is not Canary!")
	}

	backendCfg, err := sele.Filter(candidates)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", backendCfg)
	outboundReg := outbound.NewOutBoundAdapterRegistry()
	outboundReg.AddOutboundRegistry("openai", outbound.NewOpenAIOutBoundAdapter())
	outboundReg.GetAdapter(backendCfg.Capabilty.Protocols)
}
