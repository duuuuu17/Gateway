package config

import (
	"fmt"
	"log/slog"
	"os"

	"go.yaml.in/yaml/v2"
)

type MultiBackendLoader struct {
	path      string
	resgistry *SelectorRegistry
	backends  BackendConfig
}

// DTO: 对于多推理后端的加载器
// BackendConfig represents the configuration for all backends
type BackendConfig struct {
	// Backends is a list of backend configurations
	Backends []Backend `json:"backends,omitempty" yaml:"backends,omitempty"`
}

// Backend represents a single backend configuration
type Backend struct {
	// Name is the unique identifier for the backend
	Name string `json:"name" yaml:"name"`
	// Capability describes the capabilities of the backend
	Capability *Capability `json:"capability,omitempty" yaml:"capability,omitempty"`
	// Routing defines the routing configuration for the backend
	Routing *Routing `json:"routing,omitempty" yaml:"routing,omitempty"`
}

// Capability describes what the backend can do
// +k8s:deepcopy-gen=true
type Capability struct {
	// Models is a list of supported model names
	Models []string `json:"models,omitempty" yaml:"models,omitempty"`
	// Protocols is a list of supported protocols
	Protocols []string `json:"protocols,omitempty" yaml:"protocols,omitempty"`
	// Endpoints is a list of API endpoints
	Endpoints []string `json:"endpoints,omitempty" yaml:"endpoints,omitempty"`
	// Streaming indicates whether the backend supports streaming responses
	Streaming *bool `json:"streaming,omitempty" yaml:"streaming,omitempty"`
}

// Routing defines how traffic should be routed to the backend
type Routing struct {
	// Weight is the relative weight for load balancing
	Weight *int32 `json:"weight,omitempty" yaml:"weight,omitempty"`
	// Priority determines the order of preference
	Priority *int32 `json:"priority,omitempty" yaml:"priority,omitempty"`

	// Selectors is a list of routing strategies to use
	// validation:Enum=round_robin;weighted;least_connections
	Strategy string `json:"strategy,omitempty" yaml:"strategy,omitempty"`
}

// Canary defines canary deployment settings
type Canary struct {
	// Ratio is the fraction of traffic to send to this backend (0.0 to 1.0)
	// need strconv.parseFloat
	Ratio string `json:"ratio,omitempty" yaml:"ratio,omitempty"`
}

func NewMultiBackendLoader(path string) *MultiBackendLoader {
	return &MultiBackendLoader{path: path, resgistry: InitEndpointLevelSelectorRegistry()}
}
func (mbl *MultiBackendLoader) Load() (RouterConfig, error) {
	data, err := os.ReadFile(mbl.path)
	if err != nil {
		return RouterConfig{}, fmt.Errorf("can't open the yaml file,Err:%s", err.Error())
	}
	if err := yaml.Unmarshal(data, &mbl.backends); err != nil {
		return RouterConfig{}, err
	}
	var cfgs []*RuntimeBackend
	for _, cfg := range mbl.backends.Backends {
		tmp, err := compileRuntimeParameters(cfg, mbl.resgistry)
		if err != nil {
			return RouterConfig{}, err
		}
		cfgs = append(cfgs, tmp)
	}
	// var backends atomic.Value
	// backends.Store(cfgs)
	routerCfg := RouterConfig{}
	routerCfg.Backends.Store(cfgs)
	return routerCfg, nil
}
func compileRuntimeParameters(cfg Backend, e *SelectorRegistry) (*RuntimeBackend, error) {

	tmp := &RuntimeBackend{
		Name: cfg.Name,
		Capabilty: BackendCapability{
			Models:           cfg.Capability.Models,
			Protocols:        cfg.Capability.Protocols,
			Endpoints:        buildEndpoints(cfg.Capability.Endpoints),
			EnabledStreaming: *cfg.Capability.Streaming,
		},
		Routing: BackendRouting{
			Weight:   *cfg.Routing.Weight,
			Priority: *cfg.Routing.Priority,
			// Strategy:    cfg.Routing.Strategy,
		},
	}
	// todo:
	// 这里关于每个backend对于selector必要参数的初始化
	// 实际应该看Selector列表有哪些选择器进行选择性初始化结构体中的对象
	selector, err := selectEndpointLevelRegistry(cfg.Routing.Strategy, e)
	if err != nil {
		slog.Error("can't choose the endpoint level selector", "err", err)
		return nil, err
	}
	tmp.EndpointSelector = selector
	return tmp, nil
}
func selectEndpointLevelRegistry(strategy string, e *SelectorRegistry) (EndpointSelector, error) {
	selector, err := e.New(strategy)
	if err != nil {
		return nil, err
	}
	return selector, nil
}
func buildEndpoints(e []string) []*Endpoint {
	var endpoints []*Endpoint
	for _, endpoint := range e {
		tmp := &Endpoint{}
		tmp.Address = endpoint
		tmp.Health.Store(true)
		tmp.ActiveConn.Store(0)
		endpoints = append(endpoints, tmp)
	}
	return endpoints
}
