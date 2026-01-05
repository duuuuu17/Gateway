package outbound

import (
	"context"
	"control-plane-model-test/pkg/config"
	"control-plane-model-test/pkg/router/core"
	"fmt"
	"net/http"
)

type OutboundAdapter interface {
	BuildHTTPRequest(context.Context, *core.LLMRequest, config.ConfigReader) (*http.Request, error)
	HandleResponse(context.Context, http.ResponseWriter, *http.Response)
}

// 所有outbound实例的注册仓库
// outbound只关注后端模型提供的协议
type OutboundRegistry struct {
	// map: protocol : OutboundAdapter
	backends map[string]OutboundAdapter
}

func NewOutBoundAdapterRegistry() *OutboundRegistry {
	m := make(map[string]OutboundAdapter)
	return &OutboundRegistry{backends: m}
}
func (br *OutboundRegistry) AddOutboundRegistry(backend string, backendAdapter OutboundAdapter) {
	if backend == "" {
		return
	}
	if backendAdapter == nil {
		return
	}
	br.backends[backend] = backendAdapter
}
func (br *OutboundRegistry) GetAdapter(backendType string) (OutboundAdapter, error) {
	if backendType == "" {
		return nil, fmt.Errorf("backend type is empty")
	}
	adapter, ok := br.backends[backendType]
	if !ok {
		return nil, fmt.Errorf("not registry the backend!")
	}
	return adapter, nil
}
