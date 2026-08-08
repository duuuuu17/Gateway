package filters

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/router/errs"
)

// 实现Canary, 只需要实现X-header-canary的添加,
// 然后返回添加X-LLM-Router-Backend: v2进行表示
type WeightedRandomFilter struct {
	localRand *rand.Rand // 避免全局锁的rand
}

func NewWeightedRandomFilter() *WeightedRandomFilter {
	return &WeightedRandomFilter{
		// 生成随机数，以时间为种子
		localRand: rand.New(rand.New(rand.NewSource(time.Now().UnixNano()))),
	}
}
func (m *WeightedRandomFilter) Filter(candidates []*config.RuntimeBackend) (*config.RuntimeBackend, error) {

	if len(candidates) == 0 { // 没有匹配的候选者
		return nil, errs.ErrNotMatchingBackend
	}
	if len(candidates) == 1 { // 仅有一个候选者直接返回
		return candidates[0], nil
	}
	// 临时保存每个 backend 的有效权重，避免修改原对象
	validWeights := make([]int32, len(candidates))
	totalWeight := int32(0)
	for i, backend := range candidates {
		localWeight := backend.Routing.Weight
		if backend.Routing.Weight <= 0 {
			localWeight = 1
		}
		validWeights[i] = localWeight
		totalWeight += localWeight
	}
	if totalWeight == 0 {
		return candidates[0], nil
	}

	// 获取限定范围的int32随机数
	randomNum := m.localRand.Int31n(totalWeight)
	fmt.Printf("generate random number:%d\n", randomNum)
	cumulativeWeight := int32(0)
	// Istio的weight方式流量分流设计思想，实现类似于线性区间:
	// 通过不断添加weight来对于生产的随机数, 模拟点映射落点在哪个区间
	for i, backend := range candidates {
		cumulativeWeight += validWeights[i]
		if cumulativeWeight > randomNum {
			return backend, nil
		}
	}

	return candidates[len(candidates)-1], nil // 兜底
}
