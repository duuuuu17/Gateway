package outboundtest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
	"github.com/duuuuu17/llm-router-operator/pkg/router/outbound"
	"github.com/stretchr/testify/assert"
)

type fakeQwenCfg struct {
	config.RouterConfig
}

func NewFakeQwenCfg() *fakeQwenCfg {
	tmp := &config.Endpoint{}
	tmp.Address = "http://127.0.0.1:8080"
	tmp.Health.Store(true)
	tmp.ActiveConn.Store(0)
	tmp2 := &config.Endpoint{}
	tmp2.Address = "http://127.0.0.2:8080"
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
	var a bool = true
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
