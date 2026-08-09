package router_test

import (
	"net/http"
	"strings"
	"testing"
)

func TestForwardRequest(t *testing.T) {
	body := `{
		"model": "qwen",
		"protocol": "openai",
		"messages": [{"role":"system","content":"you need help user"},{"role":"user","content":"hi"}],
		"temperature": 0.7,
		"stream": false
	}`
	server := FakeBackendServer()
	defer server.Close()
	req, err := http.NewRequest(http.MethodPost, server.URL, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("response:%+v ", resp)
}
