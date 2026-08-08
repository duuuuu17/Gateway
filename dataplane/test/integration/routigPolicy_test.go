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
			Models:           []string{"qwen", "qwen2.5"},
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
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	storage := NewFakeQwenCfg()

	// 调用config模块进行主动初始化
	// loader := config.NewYAMLLoader(path)
	candidates, err := core.FilterCandidates(llmReq, &storage.RouterConfig)
	if err != nil {
		t.Fatal("not match backend,check fakeBackends info !")

	}
	strategy := core.ResolveStrategy(llmReq)
	selectors := filters.NewSelectorRegistry()
	selectors.AddSelector("FirstPick", filters.NewPickFirstFilterPlicy())
	selectors.AddSelector("Canary", filters.NewWeightedRandomFilter())
	sele, err := selectors.GetFilter(strategy)
	if err != nil {
		t.Fatal("can't got selector")
	}
	if _, ok := sele.(*filters.WeightedRandomFilter); !ok {
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
