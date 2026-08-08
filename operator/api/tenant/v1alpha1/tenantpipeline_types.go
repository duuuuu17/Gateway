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
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// TenantPipelineSpec defines the desired state of TenantPipeline

type TenantPipelineSpec struct {
	Tenants []Tenants `json:"tenants" yaml:"tenants"`
}
type Tenants struct {
	TenantID string        `json:"tenant_id" yaml:"tenantID"`
	Enabled  bool          `json:"enabled,omitempty" yaml:"enabled"`
	Pipeline PipelineSteps `json:"pipeline" yaml:"pipeline"`
}
type PipelineSteps struct {
	PreRouting  []PipelineStep `json:"pre_routing,omitempty" yaml:"preRouting,omitempty"`
	PostRouting []PipelineStep `json:"post_routing,omitempty" yaml:"postRouting,omitempty"`
}

type PipelineStep struct {
	Name   string               `json:"name"`
	Config runtime.RawExtension `json:"config,omitempty" yaml:"config,omitempty"`
}

// TenantPipelineStatus defines the observed state of TenantPipeline.
type TenantPipelineStatus struct {

	// conditions represent the current state of the TenantPipeline resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// TenantPipeline is the Schema for the tenantpipelines API
type TenantPipeline struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of TenantPipeline
	// +required
	Spec TenantPipelineSpec `json:"spec"`

	// status defines the observed state of TenantPipeline
	// +optional
	Status TenantPipelineStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true
// TenantPipelineList contains a list of TenantPipeline
type TenantPipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TenantPipeline `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TenantPipeline{}, &TenantPipelineList{})
}
