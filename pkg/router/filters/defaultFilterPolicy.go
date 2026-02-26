package filters

import (
	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
)

type PickFirstFilterPlicy struct{}

func NewPickFirstFilterPlicy() *PickFirstFilterPlicy {
	return &PickFirstFilterPlicy{}
}
func (m *PickFirstFilterPlicy) Filter(candidates []*config.RuntimeBackend) (*config.RuntimeBackend, error) {
	if len(candidates) == 0 {
		return nil, core.ErrNotMatchingBackend
	}
	return candidates[0], nil
}
