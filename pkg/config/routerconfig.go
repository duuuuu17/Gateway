package config

import (
	"sync/atomic"
)

// Router实际使用的对象
// 项目使用的实际运行时的配置信息对象RouterModel
type RouterConfig struct {
	Backends []*RuntimeBackend
	// Backends atomic.Value // map[ServiceName]*RuntimeBackend
}

// Service 粒度
type RuntimeBackend struct {
	Name string

	Capabilty BackendCapability // imutable CDS
	// Endpoints atomic.Value      // []*Endpoint EDS
	Routing BackendRouting // RDS

	// 根据配置人员显示指示endpoint将使用的LoadBalancer策略
	// 表示某selector算法的运行时状态，比如：
	// RoundRobin、LeastConn
	// 并且对于服务级策略还需要需要包含有状态值，比如：RR的Index
	EndpointSelector EndpointSelector // EDS using LB state
}
type BackendCapability struct {
	Models           []string
	Protocols        []string // openai / kserve
	Endpoints        []*Endpoint
	EnabledStreaming bool
}

// 实际保存的后端Pod地址或Service地址
type Endpoint struct {
	Address string
	// 表示Router当前是否认为该backend是否可被选中
	// 可被修改的来源：K8s Pod / Endpoint health check
	// 主动探测（/healthz）
	// 被动熔断（连续 5xx / timeout）
	Health     atomic.Bool
	ActiveConn atomic.Int64 // Least_Conn
}

type BackendRouting struct {
	Weight      int32
	Priority    int32
	CanaryRatio float64
	Region      string
	// Strategy    string
}

// 统一行为接口
type ConfigLoader interface {
	Load() (RouterConfig, error)
}

// func NewRouterConfig(init RouterConfig) *RouterConfig {
// 	p := &RouterConfig{}
// 	p.updateALL(init)
// 	return p
// }
// func (s *RouterConfig) GetConfig() map[string]*RuntimeBackend {
// 	return s.Backends.Load().(map[string]*RuntimeBackend)
// }
// func (s *RouterConfig) updateALL(cfg RouterConfig) {
// 	s.Backends.Store(cfg)
// }
// func (s *RouterConfig) updateRuntimeBackendOfServiceName(serviceName string, cfg *RuntimeBackend) {
// 	old := s.Backends.Load().(map[string]*RuntimeBackend) // old obj is immutable snapshot
// 	// Note: map can't modify in place, using Copy On Write.
// 	newbackends := make(map[string]*RuntimeBackend, len(old))
// 	for k, v := range old {
// 		newbackends[k] = v
// 	}
// 	newbackends[serviceName] = cfg
// 	s.Backends.Store(newbackends)

// }
// func (s *RouterConfig) updateEndpointsOfServiceName(serviceName string, cfg []*Endpoint) {
// 	backends := s.Backends.Load().(map[string]*RuntimeBackend)
// 	backends[serviceName].Endpoints.Store(cfg)
// }
