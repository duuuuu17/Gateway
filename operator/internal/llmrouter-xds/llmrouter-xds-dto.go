package llmrouterxds

import configv1alpha1 "github.com/duuuuu17/llm-router-operator/api/config/v1alpha1"

type ClusterSpec struct {
	Name string `json:"name" yaml:"name"`
	// Models is a list of supported model names
	Models []string `json:"models,omitempty" yaml:"models,omitempty"`

	// Protocols is a list of supported protocols
	Protocols []string `json:"protocols,omitempty" yaml:"protocols,omitempty"`

	// Endpoints is a list of API endpoints
	// +optional
	Endpoints []string `json:"endpoints,omitempty" yaml:"endpoints,omitempty"`
	// specify port
	Port *Port `json:"port,omitempty"  yaml:"port,omitempty"`
	// Streaming indicates whether the backend supports streaming responses
	Streaming *bool `json:"streaming,omitempty" yaml:"streaming,omitempty"`
	// 此处的loadBalancer策略为最后流量转发到最终目标Pod的Endpoint选择时使用的选择器策略
	LbStrategy string `json:"lbStrategy,omitempty" yaml:"lbStrategy,omitempty"`
}
type Port struct {
	Number *int32 `json:"number" yaml:"number"`
	Name   string `json:"name" yaml:"name"`
}

// 路由策略默认为使用weight的流量路由策略
type RouterSpec struct {
	Name     string `json:"name,omitempty" yaml:"name,omitempty"`
	Weight   *int32 `json:"weight,omitempty" yaml:"weight,omitempty"`
	Priority *int32 `json:"priority,omitempty" yaml:"priority,omitempty"`
	Region   string `json:"region,omitempty" yaml:"region,omitempty"`
}

func NewClusterSpec(config *configv1alpha1.Backend) (*ClusterSpec, *RouterSpec) {
	if config.Capability.Streaming == nil {
		*config.Capability.Streaming = false
	}
	return &ClusterSpec{
			Name:      config.Name,
			Models:    config.Capability.Models,
			Protocols: config.Capability.Protocols,
			Endpoints: config.Capability.Endpoints,
			Port: &Port{
				Name:   config.Capability.Port.Name,
				Number: config.Capability.Port.Number,
			},
			Streaming:  config.Capability.Streaming,
			LbStrategy: config.Routing.Selector,
		}, &RouterSpec{
			Name:     config.Name,
			Weight:   config.Routing.Weight,
			Priority: config.Routing.Priority,
			Region:   config.Routing.Region,
		}
}
func (c *ClusterSpec) ToLLMRouterCluster() *LLMRouterCluster {
	return &LLMRouterCluster{
		Name:       c.Name,
		Models:     c.Models,
		Protocols:  c.Protocols,
		Streaming:  *c.Streaming,
		LbStrategy: c.LbStrategy,
	}
}
func (c *ClusterSpec) ToLLMRouterServiceConfig() *ServiceConfig {
	return &ServiceConfig{
		TargetPort:     c.Port.Number,
		TargetPortName: c.Port.Name,
	}
}
func (r *RouterSpec) ToLLMRouterrouting() *LLMRouterRouting {
	return &LLMRouterRouting{
		ClusterName: r.Name,
		Weight:      *r.Weight,
		Priority:    *r.Priority,
		Region:      r.Region,
	}
}
func ToLLMRouterEndpointAssignment(service ServiceConfig, endpoints []string) []*LLMRouterEndpoint {
	es := make([]*LLMRouterEndpoint, 0)
	for _, e := range endpoints {
		es = append(es, &LLMRouterEndpoint{Address: e})
	}
	return es
}
