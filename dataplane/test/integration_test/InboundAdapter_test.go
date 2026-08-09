package integration_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/duuuuu17/llm-router-operator/pkg/router/inbound"
)

func TestRouter_SelectInbound(t *testing.T) {
	reg := inbound.NewInboundAdapterRegistry()
	reg.AddInboundAdapter("openai", inbound.NewOpenAIInBoundAdapter())

	req := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	req.Header.Set("Content-Type", "application/json")

	adapter, err := reg.GetAdapter(req)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := adapter.(*inbound.OpenAIInBoundAdapter); !ok {
		t.Fatal("wrong adapter selected")
	}
}

func TestRouter_InboundAdapterParse(t *testing.T) {
	body := `{
		"model": "qwem2.5",
		"messages": [{"role":"system","content":"you are a helpful person"},{"role":"user","content":"hi"}],
		"stream": true ,
		"temperature": 0.7,
		"strategy": "Canary"
	}`
	reg := inbound.NewInboundAdapterRegistry()
	reg.AddInboundAdapter("openai", inbound.NewOpenAIInBoundAdapter())

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-LLM-Routing-Strategy", "RoundRobin")
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
	if llmReq.Model != "qwem2.5" || llmReq.Stream == nil || !*llmReq.Stream {
		t.Fatalf("unexpected parse:%+v", llmReq)
	}
	if llmReq.Strategy != "RoundRobin" {
		t.Fatalf("unexpected parse:%+v", llmReq)
	}
	t.Logf("Parse LLMReuqest:%+v", llmReq)
}
