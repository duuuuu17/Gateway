package outbound

import (
	"context"
	"net/http"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
	"github.com/duuuuu17/llm-router-operator/pkg/router/errs"
)

// 适配器
type OutboundAdapter interface {
	BuildHTTPRequest(context.Context, *core.LLMRequest, *config.RuntimeBackend) (*http.Request, error)
	// todo: Segregation of duties(SOD)
	// OutBound: Receive Backend response, And Parse the response to the Immediately LLMResponse
	//  Func: ParseBackendRsponse(ctx, *http.Response)error
	// Forward: Transform LLMResponse to the http.Response
	//  Func: WriteClientResponse(ctx,LLMResponse,http.WriterResponse) error
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
	return nil, errs.ErrNotMatchingBackend
}
