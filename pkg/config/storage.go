package config

import "sync/atomic"

// 保存读取的文件配置信息，供外部服务模块访问
// 同时利用atomic避免并发的数据一致性
type AtomicConfigStore struct {
	value atomic.Value
}

func NewAtomicConfigStore(init RouterConfig) *AtomicConfigStore {
	p := &AtomicConfigStore{}
	p.update(init)
	return p
}

// Router 只会调用这个获取Config
func (p *AtomicConfigStore) GetConfig() RouterConfig {
	return p.value.Load().(RouterConfig)
}
func (p *AtomicConfigStore) update(cfg RouterConfig) {
	p.value.Store(cfg)
}
