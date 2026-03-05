/*
Copyright 2026 duuuuu17.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Backend represents a single backend configuration
// +k8s:deepcopy-gen=true
type Backend struct {
	// Name is the unique identifier for the backend
	// +kubebuilder:validation:Required
	Name string `json:"name" yaml:"name"`

	// Capability describes the capabilities of the backend
	// +optional
	Capability *Capability `json:"capability,omitempty" yaml:"capability,omitempty"`

	// Routing defines the routing configuration for the backend
	// +optional
	Routing *Routing `json:"routing,omitempty" yaml:"routing,omitempty"`
}

// Capability describes what the backend can do
// +k8s:deepcopy-gen=true
type Capability struct {
	// Models is a list of supported model names
	// +optional
	Models []string `json:"models,omitempty" yaml:"models,omitempty"`

	// Protocols is a list of supported protocols
	// +optional
	Protocols []string `json:"protocols,omitempty" yaml:"protocols,omitempty"`

	// Endpoints is a list of API endpoints
	// +optional
	Endpoints []string `json:"endpoints,omitempty" yaml:"endpoints,omitempty"`
	// specify port
	// +optional
	Port *Port `json:"port,omitempty"  yaml:"port,omitempty"`
	// Streaming indicates whether the backend supports streaming responses
	// +kubebuilder:default=false
	// +optional
	Streaming *bool `json:"streaming,omitempty" yaml:"streaming,omitempty"`
}

// +k8s:deepcopy-gen=true
type Port struct {
	Number *int32 `json:"number" yaml:"number"`
	Name   string `json:"name" yaml:"name"`
}

// // Endpoint represents an API endpoint
// // +k8s:deepcopy-gen=true
// type Endpoint struct {
// 	// URL is the endpoint URL
// 	// +kubebuilder:validation:Required
// 	URL string `json:"url" yaml:"url"`
// }

// Routing defines how traffic should be routed to the backend
// +k8s:deepcopy-gen=true
type Routing struct {
	// Weight is the relative weight for load balancing
	// +optional
	Weight *int32 `json:"weight,omitempty" yaml:"weight,omitempty"`

	// Priority determines the order of preference
	// +optional
	Priority *int32 `json:"priority,omitempty" yaml:"priority,omitempty"`

	// // Canary defines canary deployment settings
	// // +optional
	// Canary *Canary `json:"canary,omitempty" yaml:"canary,omitempty"`

	// Selectors is a list of routing strategies to use
	// +optional
	// +kubebuilder:validation:Enum=round_robin;weighted;least_connections
	Selector string `json:"selector,omitempty" yaml:"selector,omitempty"`
	// +optional
	Region string `json:"region,omitempty" yaml:"region,omitempty"`
}

// // Canary defines canary deployment settings
// // +k8s:deepcopy-gen=true
// type Canary struct {
// 	// Ratio is the fraction of traffic to send to this backend (0.0 to 1.0)
// 	// need strconv.parseFloat
// 	// +optional
// 	Ratio string `json:"ratio,omitempty" yaml:"ratio,omitempty"`
// }

// BackendConfig represents the configuration for all backends
// +k8s:deepcopy-gen=true
type BackendConfig struct {
	// Backends is a list of backend configurations
	// +required
	Backends []Backend `json:"backends,omitempty" yaml:"backends,omitempty"`
}

// LLMRouterConfigSpec defines the desired state of LLMRouterConfig
// +k8s:deepcopy-gen=true
type LLMRouterConfigSpec struct {
	// Backends defines the backend configurations for the LLM router
	// +required
	Backends *BackendConfig `json:"backends,omitempty" yaml:"backends,omitempty"`
	// ConfigMapName specifies the name of the ConfigMap to store the router configuration
	// +kubebuilder:default=config.yaml
	// +kubebuilder:validation:Required
	// +optional
	ConfigMapName string `json:"configMapName" yaml:"configMapName"`
}

// type LLMRouterConfigSpec struct {
// 	// +kubebuilder:validation:required
// 	// 假设你的应用配置文件中这个字段叫 backend_models
// 	BackendModels []BackendModelSpec `json:"backendModels" yaml:"backendModels"`
// 	// 这个字段是 Operator 逻辑用的，可能不需要写入应用的配置文件？
// 	// 如果不需要写入 config.yaml，可以在 yaml 标签中设为 "-"
// 	// +kubebuilder:default="config"
// 	ConfigMapName string `json:"configMapName,omitempty" yaml:"-"`
// 	// +optional
// 	// +kubebuilder:validation:Enum=RoundRobin;Canary
// 	// +kubebuilder:default="RoundRobin"
// 	DefaultStrategy string `json:"defaultStrategy,omitempty" yaml:"defaultStrategy,omitempty"`
// }

// LLMRouterConfigStatus defines the observed state of LLMRouterConfig.
type LLMRouterConfigStatus struct {
	// The status of each condition is one of True, False, or Unknown.

	// 是否同步到ConfigMap
	Synced bool `json:"synced"`
	// 由Controller创建的ConfigMap的名称
	ConfigMapName string `json:"configMapName,omitempty"`
	// 得到configmap的datahash值
	ConfigDataHash string `json:"configDataHash"`
	// 错误信息
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Synced",type="boolean",JSONPath=".status.synced"
// +kubebuilder:printcolumn:name="ConfigMap",type="string",JSONPath=".status.configMapName"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// LLMRouterConfig is the Schema for the llmrouterconfigs API
type LLMRouterConfig struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of LLMRouterConfig
	// +required
	Spec LLMRouterConfigSpec `json:"spec"`

	// status defines the observed state of LLMRouterConfig
	// +optional
	Status LLMRouterConfigStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// LLMRouterConfigList contains a list of LLMRouterConfig
type LLMRouterConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []LLMRouterConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LLMRouterConfig{}, &LLMRouterConfigList{})
}
