package config

import (
	"context"
	"log"
	"log/slog"
	"os"
	"reflect"
	"sync/atomic"
	"time"

	llmrouterxds "github.com/duuuuu17/llm-router-operator/pkg/config/llmrouter-xds"
	eventbus "github.com/duuuuu17/llm-router-operator/pkg/eventBus"
	"github.com/fsnotify/fsnotify"
	"github.com/google/go-cmp/cmp"
	"gopkg.in/yaml.v3"
)

type TenantCfg struct {
	FilePath     string
	GlobalConfig atomic.Pointer[GlobalConfig] // *GlobalConfig
	Bus          *eventbus.Bus
}
type GlobalConfig struct {
	// Version   string                  `yaml:"version"`
	TenantCfg map[string]*TenantConfig `yaml:"tenants"`
}
type TenantConfig struct {
	Enabled   bool            `yaml:"enabled"`
	Pipelines *PipelineConfig `yaml:"pipeline"`
}
type PipelineConfig struct {
	PreRouting  []*PluginStep `yaml:"pre_routing"`
	PostRouting []*PluginStep `yaml:"post_routing"`
}
type PluginStep struct {
	Name   string         `yaml:"name"`
	Config map[string]any `yaml:"config"`
}

func NewTenantCfg(path string, bus *eventbus.Bus) *TenantCfg {
	tg := &TenantCfg{FilePath: path, GlobalConfig: atomic.Pointer[GlobalConfig]{}, Bus: bus}
	defaultTenant := &GlobalConfig{TenantCfg: make(map[string]*TenantConfig)}
	defaultTenant.TenantCfg["default"] = &TenantConfig{
		Enabled: true,
		Pipelines: &PipelineConfig{
			PreRouting:  make([]*PluginStep, 0),
			PostRouting: make([]*PluginStep, 0),
		},
	}
	tg.GlobalConfig.Store(defaultTenant)
	return tg
}
func (g *TenantCfg) LoadFromYamlFile() error {
	cfg := GlobalConfig{}
	content, err := os.ReadFile(g.FilePath)
	if err != nil {
		return err
	}
	if err := yaml.Unmarshal(content, &cfg); err != nil {
		return err
	}
	g.GlobalConfig.Store(&cfg)
	return nil
}

func (g *TenantCfg) WatchFile(ctx context.Context) {
	watcher, _ := fsnotify.NewWatcher()
	if err := watcher.Add(g.FilePath); err != nil {
		slog.Warn("watch file failure")
		return
	}
	var lastTIme time.Time
	const fixedDelayTime = time.Millisecond * 500
	slog.Info("Tenanat YAML file watcher 已启动")
	for {
		select {
		case <-ctx.Done():
			slog.Info("Tenanat YAML file watcher 已停止监听")
			return
		case events := <-watcher.Events:
			if events.Op&fsnotify.Write == fsnotify.Write {
				if time.Since(lastTIme) > fixedDelayTime {
					lastTIme = time.Now()
					err := g.LoadFromYamlFile()
					if err == nil {
						g.Bus.Publish("tenanat_config.reloaded", g.GlobalConfig.Load())
						log.Println("config reloaded")
					}
					slog.Error("file load failure", "Err", err.Error())
				}
			}
		}
	}
}

func TenantMinimalConfig() *GlobalConfig {
	return &GlobalConfig{
		TenantCfg: map[string]*TenantConfig{
			"guest": {
				Enabled: true,
				Pipelines: &PipelineConfig{
					PreRouting: []*PluginStep{
						{
							Name: "rate_limit",
							Config: map[string]any{
								"daily_limit": 3,
							},
						},
					},
				},
			},
		},
	}
}
func (g *TenantCfg) Equal(other *TenantCfg) bool {
	return cmp.Equal(g, other)
}

// or else
func (tc *TenantConfig) Equal(other *TenantConfig) bool {
	return other != nil && tc.Pipelines.Equal(other.Pipelines)
}
func (p *PipelineConfig) Equal(other *PipelineConfig) bool {
	if p == nil || other == nil {
		return p == other
	}
	// 1. 比较切片长度
	if len(p.PreRouting) != len(other.PreRouting) {
		return false
	}
	for i := range p.PreRouting {
		if !p.PreRouting[i].Equal(other.PreRouting[i]) {
			return false
		}
	}
	// PostRouting 同理...
	if len(p.PostRouting) != len(other.PostRouting) {
		return false
	}
	for i := range p.PostRouting {
		if !p.PostRouting[i].Equal(other.PostRouting[i]) {
			return false
		}
	}
	return true
}
func (ps *PluginStep) Equal(other *PluginStep) bool {
	if ps == nil || other == nil {
		return ps == other
	}
	if ps.Name != other.Name {
		return false
	}
	return reflect.DeepEqual(ps.Config, other.Config)
}

// will be tranfrom from LLMRouterTenantPipelineConfigs to TenantPipelineConfigs
func (g *TenantCfg) BatchUpdate(needupdates map[string]*TenantConfig, deletes []string) error {
	oldMap := g.GlobalConfig.Load()
	newMap := make(map[string]*TenantConfig)
	// deepcopy
	for tenantID, tenant := range oldMap.TenantCfg {
		newMap[tenantID] = tenant.Clone()
	}
	// op: update
	for tenantID, tenant := range needupdates {
		if tenant.Equal(newMap[tenantID]) {
			continue
		}
		newMap[tenantID] = tenant
	}
	newCfg := &GlobalConfig{
		TenantCfg: newMap,
	}
	// op: delete
	for _, deleted := range deletes {
		delete(newMap, deleted)
	}
	// cow
	g.GlobalConfig.Store(newCfg)
	// publish
	g.Bus.Publish("tenant_config.reloaded", newCfg)
	return nil
}

func (tc *TenantConfig) Clone() *TenantConfig {
	if tc == nil {
		return nil
	}
	// 仅深拷贝引用类型字段，值类型直接复制
	steps := tc.Pipelines.Clone() // 若 Step 含指针，需递归 Clone

	return &TenantConfig{
		Enabled:   tc.Enabled,
		Pipelines: steps, // 深拷贝切片
	}
}
func (g *TenantCfg) ConvertToDTO(tds *llmrouterxds.LLMRouterTenantPipelineConfig) *TenantConfig {
	tenant := &TenantConfig{
		Enabled: tds.Enabled,
		Pipelines: &PipelineConfig{
			PreRouting:  LLMRouterTenantPipelineStepConvertToSteps(tds.TenantPreRouting),
			PostRouting: LLMRouterTenantPipelineStepConvertToSteps(tds.TenantPostRouting),
		},
	}
	return tenant
}
func LLMRouterTenantPipelineStepConvertToSteps(steps []*llmrouterxds.LLMRouterTenantPipelineStep) []*PluginStep {
	step := make([]*PluginStep, 0, len(steps))
	for _, t := range steps {
		r := &PluginStep{}
		r.Name = t.PluginName
		r.Config = t.Config.AsMap()
		step = append(step, r)
	}
	return step
}

// deep-copy
func (ps *PipelineConfig) Clone() *PipelineConfig {
	pipelines := &PipelineConfig{}
	pipelines.PostRouting = ClonePointerElemsIn(pipelines.PostRouting)
	pipelines.PreRouting = ClonePointerElemsIn(pipelines.PreRouting)
	return pipelines
}

// deepCopy
func ClonePointerElemsIn(src []*PluginStep) []*PluginStep {
	newSlice := make([]*PluginStep, 0, len(src))
	for _, s := range src {
		newElem := &PluginStep{}
		newElem.Config = s.Config
		newElem.Name = s.Name
		newSlice = append(newSlice, newElem)
	}
	return newSlice
}

// metadata:
//   name: <tenantID>
//   namespace: <ns>
// spec:
//     enabled: true
//     pipeline:
//       pre_routing:
//         - name: auth
//           config:
//             mode: jwt

//         - name: rate_limit
//           config:
//             qps: 100

//         - name: cache
//           config:
//             type: redis
//             ttl: 60

//         - name: routing
//           config:
//             strategy: weight
// 	  post_routing:
// 		- name: quota
// 		  config:
// 		  	tokensPerMonth: 1000000
// 			tokenUsage: 0
