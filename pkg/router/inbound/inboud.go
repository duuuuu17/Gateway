package inbound

import (
	"net/http"

	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
)

type InboundAdapter interface {
	Match(*http.Request) bool
	Parse(*http.Request) (*core.LLMRequest, error)
}

// inbound adpater的注册工厂，进行管理
// key: from http.headers: "X-LLM-Protocol"
type InboundAdapterRegistry struct {
	InboundAdapters map[string]InboundAdapter // map: {X-LLM-Protocol : inbound adapter}
}

func NewInboundAdapterRegistry() *InboundAdapterRegistry {
	return &InboundAdapterRegistry{
		make(map[string]InboundAdapter),
	}
}

func (ad *InboundAdapterRegistry) AddInboundAdapter(protocol string, adapter InboundAdapter) {
	ad.InboundAdapters[protocol] = adapter
}

// 实际的Select Inbound Adapter逻辑
func (ad *InboundAdapterRegistry) GetAdapter(req *http.Request) (InboundAdapter, error) {
	protocol := req.Header.Get("X-LLM-Protocol")
	if protocol != "" {
		adapter, ok := ad.InboundAdapters[protocol]
		if !ok {
			return nil, core.ErrUnsupportProtocol
		}
		return adapter, nil
	}
	for _, adapter := range ad.InboundAdapters {
		if adapter.Match(req) {
			return adapter, nil
		}
	}
	return nil, core.ErrUnsupportProtocol
}
