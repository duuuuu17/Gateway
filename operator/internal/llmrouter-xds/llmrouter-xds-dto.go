package llmrouterxds

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	configv1alpha1 "github.com/duuuuu17/llm-router-operator/api/config/v1alpha1"
	tenantv1alpha1 "github.com/duuuuu17/llm-router-operator/api/tenant/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	RoundRobin       = "round_robin"
	Weighted         = "weighted"
	LeastConnections = "least_connections"
	Canary           = "Canary"
)

type ClusterSpec struct {
	Name string `json:"name" yaml:"name"`
	// Models is a list of supported model names
	Models []string `json:"models,omitempty" yaml:"models,omitempty"`

	// Protocols is a list of supported protocols
	Protocols []string `json:"protocols,omitempty" yaml:"protocols,omitempty"`

	// Endpoints is a list of API endpoints
	// +optional
	Endpoints []string `json:"endpoints,omitempty" yaml:"endpoints,omitempty"`
	// specify port
	Port *Port `json:"port,omitempty"  yaml:"port,omitempty"`
	// Streaming indicates whether the backend supports streaming responses
	Streaming *bool `json:"streaming,omitempty" yaml:"streaming,omitempty"`
	// 此处的loadBalancer策略为最后流量转发到最终目标Pod的Endpoint选择时使用的选择器策略
	LbStrategy string `json:"lbStrategy,omitempty" yaml:"lbStrategy,omitempty"`
}
type Port struct {
	Number *int32 `json:"number" yaml:"number"`
	Name   string `json:"name" yaml:"name"`
}

// 路由策略默认为使用weight的流量路由策略
type RouterSpec struct {
	Name        string  `json:"name,omitempty" yaml:"name,omitempty"`
	Weight      *int32  `json:"weight,omitempty" yaml:"weight,omitempty"`
	Priority    *int32  `json:"priority,omitempty" yaml:"priority,omitempty"`
	Region      string  `json:"region,omitempty" yaml:"region,omitempty"`
	Selector    string  `json:"selector,omitempty" yaml:"selector,omitempty"`
	CanaryRatio float64 `json:"ratio,omitempty" yaml:"ratio,omitempty"`
	// Ratio is the fraction of traffic to send to this backend (0.0 to 1.0)
}

func NewClusterSpec(config *configv1alpha1.Backend) (*ClusterSpec, *RouterSpec, error) {
	if config.Capability.Streaming == nil {
		*config.Capability.Streaming = false
	}
	clusterSpe := &ClusterSpec{
		Name:      config.Name,
		Models:    config.Capability.Models,
		Protocols: config.Capability.Protocols,
		Endpoints: config.Capability.Endpoints,
		Port: &Port{
			Name:   config.Capability.Port.Name,
			Number: config.Capability.Port.Number,
		},
		Streaming:  config.Capability.Streaming,
		LbStrategy: config.Routing.Selector,
	}
	routerSpe := &RouterSpec{
		Name:     config.Name,
		Priority: config.Routing.Priority,
		Region:   config.Routing.Region,
		Selector: config.Routing.Selector,
	}
	switch config.Routing.Selector {
	case Weighted:
		routerSpe.Weight = config.Routing.Weight
	case Canary:
		ratio, err := strconv.ParseFloat(config.Routing.CanaryRatio, 64)
		if err != nil {
			slog.Info("the CanaryRatio can't convert to float64 ")
			return nil, nil, err
		}
		routerSpe.CanaryRatio = ratio
	}
	return clusterSpe, routerSpe, nil
}
func (c *ClusterSpec) ToLLMRouterCluster() *LLMRouterCluster {
	return &LLMRouterCluster{
		Name:       c.Name,
		Models:     c.Models,
		Protocols:  c.Protocols,
		Streaming:  *c.Streaming,
		LbStrategy: c.LbStrategy,
	}
}
func (c *ClusterSpec) ToLLMRouterServiceConfig() *ServiceConfig {
	return &ServiceConfig{
		TargetPort:     c.Port.Number,
		TargetPortName: c.Port.Name,
	}
}
func (r *RouterSpec) ToLLMRouterrouting() *LLMRouterRouting {
	routing := &LLMRouterRouting{
		ClusterName: r.Name,
		Priority:    *r.Priority,
		Region:      r.Region,
		Selector:    r.Selector,
	}
	switch r.Selector {
	case Weighted:
		routing.Weight = *r.Weight
	case Canary:
		routing.CanaryRatio = r.CanaryRatio
	}
	return routing
}
func ToLLMRouterEndpointAssignment(service ServiceConfig, endpoints []string) []*LLMRouterEndpoint {
	es := make([]*LLMRouterEndpoint, 0, 1)
	for _, e := range endpoints {
		es = append(es, &LLMRouterEndpoint{Address: e})
	}
	return es
}

type TenantsDTO struct {
	ConfigUID string
	Tenants   []*TenantSpec
	rawCR     *tenantv1alpha1.TenantPipeline // 保留原始 CR 引用（调试/审计用）
}
type TenantSpec struct {
	TenantID    string          `json:"tenant_id" yaml:"tenantID"`
	Enabled     bool            `json:"enabled,omitempty" yaml:"enabled"`
	PreRouting  []*PipelineStep `json:"pre_routing,omitempty" yaml:"preRouting,omitempty"`
	PostRouting []*PipelineStep `json:"post_routing,omitempty" yaml:"postRouting,omitempty"`
}
type PipelineStep struct {
	Name   string               `json:"name"`
	Config runtime.RawExtension `json:"config,omitempty" yaml:"config,omitempty"`
}

func NewTenantsDTO(cr *tenantv1alpha1.TenantPipeline) (*TenantsDTO, error) {
	result := strings.Join([]string{cr.Namespace, cr.Name, string(cr.UID)}, "/")
	ts := &TenantsDTO{
		rawCR:     cr,
		ConfigUID: result,
	}
	if len(cr.Spec.Tenants) == 0 {
		return nil, fmt.Errorf("CR tenants is nil! CR objectMeta.UID:%s", string(cr.UID))
	}
	ts.Tenants = make([]*TenantSpec, 0, len(cr.Spec.Tenants))
	for _, cfg := range cr.Spec.Tenants {
		ts.Tenants = append(ts.Tenants, &TenantSpec{
			TenantID:    cfg.TenantID,
			Enabled:     cfg.Enabled,
			PreRouting:  CRDTenantPipelineStepsToDTOTenantPipelineStep(cfg.Pipeline.PreRouting),
			PostRouting: CRDTenantPipelineStepsToDTOTenantPipelineStep(cfg.Pipeline.PostRouting),
		})
	}
	return ts, nil
}

func CRDTenantPipelineStepsToDTOTenantPipelineStep(steps []tenantv1alpha1.PipelineStep) []*PipelineStep {
	toSteps := make([]*PipelineStep, 0, len(steps))
	for _, step := range steps {
		t := &PipelineStep{
			Name: step.Name,
		}
		t.Config = step.Config
		toSteps = append(toSteps, t)
	}
	return toSteps
}

func (ts *TenantsDTO) ToTenantPipeline() *LLMRouterTenantPipelineConfigs {
	tenants := &LLMRouterTenantPipelineConfigs{
		TenantConfigId: ts.ConfigUID,
		Tenants:        make([]*LLMRouterTenantPipelineConfig, 0, len(ts.Tenants)),
	}
	for _, cfg := range ts.Tenants {
		tenants.Tenants = append(tenants.Tenants, &LLMRouterTenantPipelineConfig{
			TenantId:          cfg.TenantID,
			Enabled:           cfg.Enabled,
			TenantPreRouting:  DTOTenantPipelineStepsToLLMRouterTenantPipelineStep(cfg.PreRouting),
			TenantPostRouting: DTOTenantPipelineStepsToLLMRouterTenantPipelineStep(cfg.PostRouting),
		})
	}
	return tenants
}
func DTOTenantPipelineStepsToLLMRouterTenantPipelineStep(steps []*PipelineStep) []*LLMRouterTenantPipelineStep {
	toSteps := make([]*LLMRouterTenantPipelineStep, 0, len(steps))
	for _, step := range steps {
		t := &LLMRouterTenantPipelineStep{
			PluginName: step.Name,
		}
		var err error
		t.Config, err = RawExtensionToStruct(step.Config)
		if err != nil {
			slog.Warn("failed convert to struct:map[string]any")
			return nil
		}
		toSteps = append(toSteps, t)
	}
	return toSteps
}
