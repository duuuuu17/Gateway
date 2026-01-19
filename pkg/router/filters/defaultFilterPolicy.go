package filters

import (
	"github.com/duuuuu17/llm-router-operator/pkg/config"
)

type PickFirstFilterPlicy struct{}

func NewPickFirstFilterPlicy() *PickFirstFilterPlicy {
	return &PickFirstFilterPlicy{}
}
func (m *PickFirstFilterPlicy) Filter(candidates []*config.RuntimeBackend) (*config.RuntimeBackend, error) {
	return candidates[0], nil
}
