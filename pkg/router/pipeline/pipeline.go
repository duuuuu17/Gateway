package pipeline

import (
	"github.com/duuuuu17/llm-router-operator/pkg/router/errs"
)

type Pipeline[T any] struct {
	Handlers []T
}

func NewPipeline[T any]() *Pipeline[T] {
	return &Pipeline[T]{Handlers: make([]T, 0)}
}
func (c *Pipeline[T]) Use(h ...T) {
	c.Handlers = append(c.Handlers, h...)
}

type HandlerFunc func(ctx *ChainContext) error

var NoopHandler HandlerFunc = func(ctx *ChainContext) error { return nil }

type TenantPipelines struct {
	tenants map[string]*Pipeline[HandlerFunc] // tenantID -> pipeline
}

func NewTenantPipelines() *TenantPipelines {
	return &TenantPipelines{tenants: make(map[string]*Pipeline[HandlerFunc])}
}
func (t *TenantPipelines) GetTenantPipeline(tenantID string) (*Pipeline[HandlerFunc], error) {

	if handlers, ok := t.tenants[tenantID]; ok {
		return handlers, nil
	}
	return nil, errs.ErrNotSupportedTenant
}

// 对于管线，不管什么情况下的加载，都是直接覆盖该租户的当前管线
func (t *TenantPipelines) AddOrUpdateTenantPipeline(tenantID string, pip *Pipeline[HandlerFunc]) {

	t.tenants[tenantID] = pip
}
func (t *TenantPipelines) RemoveTenantPipeline(tenantID string) {

	delete(t.tenants, tenantID)
}
func (t *TenantPipelines) CopyFromOldMap(oldTenant *TenantPipelines) {
	for k, v := range oldTenant.tenants {
		val := &Pipeline[HandlerFunc]{}
		val.Use(v.Handlers...)
		t.tenants[k] = val
	}
}
