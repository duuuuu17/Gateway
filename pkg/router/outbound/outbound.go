package outbound

import (
	"context"
	"net/http"

	"github.com/duuuuu17/llm-router-operator/pkg/config"

	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
)

type OutboundAdapter interface {
	BuildHTTPRequest(context.Context, *core.LLMRequest, *config.RuntimeBackend) (*http.Request, error)
	HandleResponse(context.Context, http.ResponseWriter, *http.Response) error
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
func (br *OutboundRegistry) GetAdapter(protocols []string) (OutboundAdapter, error) {
	for _, protocol := range protocols {
		adapter, ok := br.backends[protocol]
		if !ok {
			continue
		}
		return adapter, nil
	}
	return nil, core.ErrNotMatchingBackend
}
