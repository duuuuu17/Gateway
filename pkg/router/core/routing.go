package core

import (
	"slices"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/metrics"
	"github.com/duuuuu17/llm-router-operator/pkg/router/errs"
)

// 筛选可用的后端并返回
func FilterCandidates(req *LLMRequest, cfgs config.RouterConfig) ([]*config.RuntimeBackend, error) {
	var candidates []*config.RuntimeBackend
	for _, cfg := range cfgs.GetConfig() {
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
		return nil, errs.ErrNotMatchingBackend
	}
	return candidates, nil
}

// 获取请求指定的策略
func ResolveStrategy(req *LLMRequest) string {
	if req.Strategy != "" {
		return req.Strategy
	}
	return "default"
}
