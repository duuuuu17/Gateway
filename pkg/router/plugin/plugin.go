package plugin

import (
	"fmt"

	"github.com/duuuuu17/llm-router-operator/pkg/router/pipeline"
)

type Plugin interface {
	Name() string
	Build(cfgs map[string]any) (pipeline.HandlerFunc, error)
}

type DefaultRegistry struct {
	plugins map[string]Plugin
}

func NewRegistry() *DefaultRegistry {
	return &DefaultRegistry{
		plugins: make(map[string]Plugin),
	}
}

func (r *DefaultRegistry) Register(plugins ...Plugin) {
	for _, p := range plugins {
		r.plugins[p.Name()] = p
	}
}

func (r *DefaultRegistry) GetPlugin(name string) (Plugin, error) {
	p, ok := r.plugins[name]
	if !ok {
		return nil, fmt.Errorf("plugin not found: %s", name)
	}
	return p, nil
}
