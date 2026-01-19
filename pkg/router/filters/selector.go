package filters

import (
	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
)

type CandidatesFilter interface {
	Filter(candidates []*config.RuntimeBackend) (*config.RuntimeBackend, error)
}

type SelectorRegistry struct {
	m map[string]CandidatesFilter
}

func NewSelectorRegistry() *SelectorRegistry {
	return &SelectorRegistry{make(map[string]CandidatesFilter)}
}
func (sr *SelectorRegistry) AddSelector(strategy string, selector CandidatesFilter) {
	if _, ok := sr.m[strategy]; ok {
		return
	}
	sr.m[strategy] = selector
}
func (sr *SelectorRegistry) GetSelector(strategy string) (CandidatesFilter, error) {
	if strategy == "" {
		return sr.m["RoundRobin"], nil
	}
	s, ok := sr.m[strategy]
	if !ok {
		return &PickFirstFilterPlicy{}, core.ErrBackend5xx
	}
	return s, nil
}
