package config

import "sync/atomic"

// 保存读取的文件配置信息，供外部服务模块访问
// 同时利用atomic避免并发的数据一致性
// 避免出现半成品
type ConfigReader interface {
	GetConfig() RouterConfig
}

type MultiConfigStore struct {
	value atomic.Value // stores []RouterConfig
}

func NewMultiConfigStore(init RouterConfig) *MultiConfigStore {
	p := &MultiConfigStore{}
	p.update(init)
	return p
}
func (s *MultiConfigStore) GetConfig() RouterConfig {
	return s.value.Load().(RouterConfig)
}
func (s *MultiConfigStore) update(cfg RouterConfig) {
	s.value.Store(cfg)
}
