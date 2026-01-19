package core

import (
	"math/rand"
	"slices"
	"time"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/metrics"
)

// 筛选可用的后端并返回
func FilterCandidates(req *LLMRequest, cfgs config.ConfigReader) ([]*config.RuntimeBackend, error) {
	var candidates []*config.RuntimeBackend
	for _, cfg := range cfgs.GetConfig().Backends {
		if !slices.Contains(cfg.Capabilty.Models, req.Model) {
			continue
		}
		if *req.Stream && !cfg.Capabilty.EnabledStreaming {
			continue
		}
		metrics.LLMBackendSelectedTotal.WithLabelValues(cfg.Name, req.Model)
		candidates = append(candidates, cfg)
	}
	if len(candidates) == 0 {
		return nil, ErrNotMatchingBackend
	}
	return candidates, nil
}

// 获取请求指定的策略
func ResolveStrategy(req *LLMRequest) string {
	if req.Strategy != "" {
		return req.Strategy
	}
	return "RoundRobin"
}
func CanaryPickOne(cfg *config.RuntimeBackend) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	if r.Float64() < cfg.Routing.CanaryRatio {
		return cfg.Capabilty.Endpoints[0].Address
	}
	return cfg.Capabilty.Endpoints[1].Address

}
