package core

import (
	"control-plane-model-test/pkg/config"
	"fmt"
	"math/rand"
	"slices"
	"time"
)

func SelectBackend(llmRequest *LLMRequest, cfgs []config.ConfigReader) (config.ConfigReader, error) {
	for _, cfg := range cfgs {
		if !slices.Contains(cfg.GetConfig().Models, llmRequest.Model) {
			continue
		}
		if *llmRequest.Stream && !cfg.GetConfig().EnabledStreaming {
			continue
		}
		return cfg, nil
	}

	return nil, fmt.Errorf("not sufficient backend")
}
func CanaryPickOne(cfg config.RouterConfig) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	if r.Float64() < cfg.CanaryRatio {
		return cfg.Endpoints[0]
	}
	return cfg.Endpoints[1]

}
