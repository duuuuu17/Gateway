package config

import (
	"context"
	"log"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	eventbus "github.com/duuuuu17/llm-router-operator/pkg/eventBus"
	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
)

type TenantCfg struct {
	FilePath     string
	GlobalConfig atomic.Value // *GlobalConfig
	Bus          *eventbus.Bus
}
type GlobalConfig struct {
	// Version   string                  `yaml:"version"`
	TenantCfg map[string]TenantConfig `yaml:"tenants"`
}
type TenantConfig struct {
	Enabled   bool           `yaml:"enabled"`
	Pipelines PipelineConfig `yaml:"pipeline"`
}
type PipelineConfig struct {
	PreRouting  []PluginStep `yaml:"pre_routing"`
	PostRouting []PluginStep `yaml:"post_routing"`
}
type PluginStep struct {
	Name   string         `yaml:"name"`
	Config map[string]any `yaml:"config"`
}

func NewTenantCfg(path string, bus *eventbus.Bus) *TenantCfg {
	return &TenantCfg{FilePath: path, GlobalConfig: atomic.Value{}, Bus: bus}
}
func (g *TenantCfg) Load() error {
	cfg := minimalConfig()

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
	watcher.Add(g.FilePath)
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
					err := g.Load()
					if err == nil {
						g.Bus.Publish("tenanat_config.reloaded", g.GlobalConfig)
						log.Println("config reloaded")
					}
					slog.Error("file load failure", "Err", err.Error())
				}
			}
		}
	}
}

func minimalConfig() GlobalConfig {
	return GlobalConfig{
		TenantCfg: map[string]TenantConfig{
			"guest": {
				Enabled: true,
				Pipelines: PipelineConfig{
					PreRouting: []PluginStep{
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
