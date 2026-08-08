package config

import (
	"errors"
	"sync/atomic"
)

type EndpointSelector interface {
	Select(endpoints []*Endpoint) (*Endpoint, error)
}

func InitEndpointLevelSelectorRegistry() *SelectorRegistry {
	registry := NewSelectorRegistry()
	registry.Registry("round_robin", func() EndpointSelector { return &RoundRobinSelector{} })
	registry.Registry("least_conn", func() EndpointSelector { return &LeastConnSelector{} })
	return registry
}

// Note：此时的工厂是作为返回构造器，而非Router模块中Adapter设计的实例注册工厂
// Why? 因为此时的Selector针对不同的backend具有不同状态参数
// [[ 即有状态和无状态对象的区别! ]]
type SelectorRegistry struct {
	factories map[string]func() EndpointSelector
}

func NewSelectorRegistry() *SelectorRegistry {
	return &SelectorRegistry{make(map[string]func() EndpointSelector)}
}
func (r *SelectorRegistry) New(strategy string) (EndpointSelector, error) {
	f, ok := r.factories[strategy]
	if !ok {
		return nil, errors.ErrUnsupported
	}
	return f(), nil
}
func (r *SelectorRegistry) Registry(strategy string, f func() EndpointSelector) {
	r.factories[strategy] = f
}

// 轮询
type RoundRobinSelector struct {
	RRIndex atomic.Uint64 // RoundRobin Index
}

func (rr *RoundRobinSelector) Select(endpoints []*Endpoint) (*Endpoint, error) {
	size := len(endpoints)
	if size == 0 {
		return nil, errors.ErrUnsupported
	}
	endpoint := endpoints[rr.RRIndex.Load()%uint64(size)]
	rr.RRIndex.Add(1)
	return endpoint, nil
}

// 最少连接数
type LeastConnSelector struct{}

func (le *LeastConnSelector) Select(endpoints []*Endpoint) (*Endpoint, error) {
	size := len(endpoints)
	if size == 0 {
		return nil, errors.ErrUnsupported
	}
	var minCounter int64 = endpoints[0].ActiveConn.Load()
	minLeastConnObj := endpoints[0]
	for _, e := range endpoints {
		// fmt.Printf("endpoint:%s,activeConn:%d\n", e.Address, e.ActiveConn.Load())
		if e.ActiveConn.Load() < minCounter {
			minCounter = e.ActiveConn.Load()
			minLeastConnObj = e
		}
	}
	minLeastConnObj.ActiveConn.Add(1)
	return minLeastConnObj, nil
}
