package router_test

import (
	"control-plane-model-test/pkg/router/inbound"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	if llmReq.Model != "qwem2.5" || llmReq.Stream == nil || !*llmReq.Stream {
		t.Fatalf("unexpected parse:%+v", llmReq)
	}
}
