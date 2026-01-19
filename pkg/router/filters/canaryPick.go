package filters

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
)

type CanaryFilterPolicy struct{}

func NewCanaryFilterPolicy() *CanaryFilterPolicy {
	return &CanaryFilterPolicy{}
}
func (m *CanaryFilterPolicy) Filter(candidates []*config.RuntimeBackend) (*config.RuntimeBackend, error) {
	totalWeight := int32(0)
	for _, backend := range candidates {
		if backend.Routing.Weight <= 0 {
			backend.Routing.Weight = 1
		}
		totalWeight += backend.Routing.Weight
	}
	// 生成随机数，以时间为种子
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// 获取限定范围的int32随机数
	randomNum := r.Int31n(totalWeight)
	fmt.Printf("generate random number:%d\n", randomNum)
	cumulativeWeight := int32(0)
	// Istio的weight方式流量分流设计思想，实现类似于线性区间:
	// 通过不断添加weight来对于生产的随机数, 模拟点映射落点在哪个区间
	for _, backend := range candidates {
		cumulativeWeight += backend.Routing.Weight
		if cumulativeWeight > randomNum {
			return backend, nil
		}
	}
	return candidates[0], nil // 兜底
}
