package inboundtest

import (
	"bytes"
	"control-plane-model-test/pkg/router/inbound"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAIInbound_Match(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/cpmpletions",
		bytes.NewBufferString(`{"model":"gpt-4"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	adapter := inbound.NewOpenAIInBoundAdapter()
	if !adapter.Match(req) {
		t.Fatal("Expected OpenAI adapter to match request")
	}
}
func TestOpenAIInbound_Parse(t *testing.T) {
	body := `{
		"model": "gpt-4",
		"messages": [{"role":"system","content":"you are a helpful person"},{"role":"user","content":"hi"}],
		"stream": true,
		"temperature": 0.7
	}`

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	adapter := inbound.NewOpenAIInBoundAdapter()
	llmReq, err := adapter.Parse(req)

	t.Logf("LLMRequest:%+v\n", llmReq)
	if err != nil {
		t.Fatal(err)
	}

	if llmReq.Model != "gpt-4" {
		t.Fatalf("unexpected model: %s", llmReq.Model)
	}
}
