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
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	configv1alpha1 "github.com/duuuuu17/llm-router-operator/api/v1alpha1"
)

// nolint:unused
// log is for logging in this package.
var llmrouterconfiglog = logf.Log.WithName("llmrouterconfig-resource")

// SetupLLMRouterConfigWebhookWithManager registers the webhook for LLMRouterConfig in the manager.
func SetupLLMRouterConfigWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&configv1alpha1.LLMRouterConfig{}).
		WithValidator(&LLMRouterConfigCustomValidator{}).
		WithDefaulter(&LLMRouterConfigCustomDefaulter{}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// +kubebuilder:webhook:path=/mutate-config-llm-router-example-io-v1alpha1-llmrouterconfig,mutating=true,failurePolicy=fail,sideEffects=None,groups=config.llm-router.example.io,resources=llmrouterconfigs,verbs=create;update,versions=v1alpha1,name=mllmrouterconfig-v1alpha1.kb.io,admissionReviewVersions=v1

// LLMRouterConfigCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind LLMRouterConfig when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type LLMRouterConfigCustomDefaulter struct {
	// TODO(user): Add more fields as needed for defaulting
}

var _ webhook.CustomDefaulter = &LLMRouterConfigCustomDefaulter{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind LLMRouterConfig.
func (d *LLMRouterConfigCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	llmrouterconfig, ok := obj.(*configv1alpha1.LLMRouterConfig)

	if !ok {
		return fmt.Errorf("expected an LLMRouterConfig object but got %T", obj)
	}
	llmrouterconfiglog.Info("Defaulting for LLMRouterConfig", "name", llmrouterconfig.GetName())

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: If you want to customise the 'path', use the flags '--defaulting-path' or '--validation-path'.
// +kubebuilder:webhook:path=/validate-config-llm-router-example-io-v1alpha1-llmrouterconfig,mutating=false,failurePolicy=fail,sideEffects=None,groups=config.llm-router.example.io,resources=llmrouterconfigs,verbs=create;update,versions=v1alpha1,name=vllmrouterconfig-v1alpha1.kb.io,admissionReviewVersions=v1

// LLMRouterConfigCustomValidator struct is responsible for validating the LLMRouterConfig resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type LLMRouterConfigCustomValidator struct {
	// TODO(user): Add more fields as needed for validation
}

var _ webhook.CustomValidator = &LLMRouterConfigCustomValidator{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type LLMRouterConfig.
func (v *LLMRouterConfigCustomValidator) ValidateCreate(_ context.Context, obj runtime.Object) (admission.Warnings, error) {
	llmrouterconfig, ok := obj.(*configv1alpha1.LLMRouterConfig)
	if !ok {
		return nil, fmt.Errorf("expected a LLMRouterConfig object but got %T", obj)
	}
	llmrouterconfiglog.Info("Validation for LLMRouterConfig upon creation", "name", llmrouterconfig.GetName())

	// TODO(user): fill in your validation logic upon object creation.

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type LLMRouterConfig.
func (v *LLMRouterConfigCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	llmrouterconfig, ok := newObj.(*configv1alpha1.LLMRouterConfig)
	if !ok {
		return nil, fmt.Errorf("expected a LLMRouterConfig object for the newObj but got %T", newObj)
	}
	llmrouterconfiglog.Info("Validation for LLMRouterConfig upon update", "name", llmrouterconfig.GetName())

	// TODO(user): fill in your validation logic upon object update.

	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type LLMRouterConfig.
func (v *LLMRouterConfigCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	llmrouterconfig, ok := obj.(*configv1alpha1.LLMRouterConfig)
	if !ok {
		return nil, fmt.Errorf("expected a LLMRouterConfig object but got %T", obj)
	}
	llmrouterconfiglog.Info("Validation for LLMRouterConfig upon deletion", "name", llmrouterconfig.GetName())

	// TODO(user): fill in your validation logic upon object deletion.

	return nil, nil
}
