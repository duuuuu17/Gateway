package inbound

import (
	"control-plane-model-test/pkg/router/core"
	"fmt"
	"net/http"
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
			return nil, fmt.Errorf("unkown protocol:%s", protocol)
		}
		return adapter, nil
	}
	for _, val := range ad.InboundAdapters {
		if val.Match(req) {
			return val, nil
		}
	}
	return nil, fmt.Errorf("not unsupport protocol!")
}
