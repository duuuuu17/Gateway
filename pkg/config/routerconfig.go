package config

import (
	"fmt"
	"os"
	"strconv"

	"go.yaml.in/yaml/v2"
)

type ConfigReader interface {
	GetConfig() RouterConfig
}

// 实际保存的参数
type RouterConfig struct {
	Models           []string
	Protocol         string // openai / kserve
	Endpoints        []string
	CanaryRatio      float64
	EnabledStreaming bool
}

// 统一行为接口
type ConfigLoader interface {
	Load() (RouterConfig, error)
}

// env 方式加载
type EnvConfigLoader struct{}

func (ecp *EnvConfigLoader) Load() (RouterConfig, error) {
	ratio, _ := strconv.ParseFloat(os.Getenv("CANARY_RATIO"), 64)
	e := []string{os.Getenv("PRIMARY_BACKEND"), os.Getenv("SECONDARDY_BACKEND")}
	return RouterConfig{
		Endpoints:        e,
		CanaryRatio:      ratio,
		EnabledStreaming: os.Getenv("ENABLED_STREAMING") == "ture",
	}, nil
}

// configmap方式: 单键值对就是一个文件的方式，挂载在/mnt目录下
type ConfigMapProvider struct{}

func (cmp *ConfigMapProvider) Load() (RouterConfig, error) {
	ratio, _ := strconv.ParseFloat(readSingleKeyPairConfigFile("/mnt/config/canaryratio"), 64)
	e := []string{readSingleKeyPairConfigFile("/mnt/config/primary"), readSingleKeyPairConfigFile("/mnt/config/secondary")}
	return RouterConfig{
		CanaryRatio:      ratio,
		Endpoints:        e,
		EnabledStreaming: readSingleKeyPairConfigFile("/mnt/config/enabledstreaming") == "true",
	}, nil
}

func readSingleKeyPairConfigFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Printf("can't read file: %s [must the key is filename & the value is file value] Error: %w\n", path, err)
		return ""
	}
	return string(data)
}

// yaml's way to load
type YAMLLoader struct {
	path string
}

func NewYAMLLoader(path string) *YAMLLoader {
	return &YAMLLoader{path: path}
}
func (yamlp *YAMLLoader) Load() (RouterConfig, error) {
	data, err := os.ReadFile(yamlp.path)
	if err != nil {
		return RouterConfig{}, fmt.Errorf("can't open the yaml file,Err:%w", err.Error())
	}
	var raw struct {
		Backend struct {
			Models      []string `yaml:"models"`
			Protocol    string   `yaml:"protocol"`
			Endpoints   []string `yaml:"endpoints"`
			CanaryRatio float64  `yaml:"canaryRatio"`
		} `yaml:"backend"`
		Features struct {
			Streaming bool `yaml:"streaming"`
		} `yaml:"features"`
	}

	if err := yaml.Unmarshal(data, &raw); err != nil {
		return RouterConfig{}, err
	}
	return RouterConfig{
		Models:           raw.Backend.Models,
		Protocol:         raw.Backend.Protocol,
		Endpoints:        raw.Backend.Endpoints,
		CanaryRatio:      raw.Backend.CanaryRatio,
		EnabledStreaming: raw.Features.Streaming,
	}, nil
}
