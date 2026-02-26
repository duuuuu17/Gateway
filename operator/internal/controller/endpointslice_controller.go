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

package controller

import (
	"context"
	"fmt"

	"github.com/duuuuu17/llm-router-operator/internal/controller/utils"
	llmrouterxds "github.com/duuuuu17/llm-router-operator/internal/llmrouter-xds"
	"github.com/go-logr/logr"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// EndpointSliceReconciler reconciles a EndpointSlice object
type EndpointSliceReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	XDSManager llmrouterxds.XDSManager
	Logger     logr.Logger
	Debouncer  llmrouterxds.Debouncer
}

func TransformerToLLMRouterEndpointAssignment() llmrouterxds.LLMRouterEndpointAssignment {
	return llmrouterxds.LLMRouterEndpointAssignment{}
}

// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch

func (r *EndpointSliceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// log := logf.FromContext(ctx)
	var eps discoveryv1.EndpointSlice
	if err := r.Get(ctx, req.NamespacedName, &eps); err != nil {
		msg := fmt.Sprintf("Failedt to get EndpointSlice, error: %w", err)
		return utils.RequeueErrCheck(ctx, err, msg)
	}
	svcName := eps.Labels["kubernetes.io/service-name"]

	// 获取当前出发reconciler的endpointslice的servicename的配置
	serviceCfg, exists := r.XDSManager.GetServiceConfig(svcName)
	if !exists {
		return ctrl.Result{}, nil
	}
	edsEndpoints := ExtractReadyEndpointsFromEndpointSlice(eps, serviceCfg)
	r.XDSManager.UpdateOrCreateEDS(svcName, edsEndpoints)
	r.pushEDSEvent(svcName)
	// endpoints :=make([]*LLMRouterEndpoint, len(.))
	return ctrl.Result{}, nil
}
func (r *EndpointSliceReconciler) pushEDSEvent(affectedServices string) {
	r.Debouncer.Enqueue(llmrouterxds.ReconcilerPushEvent{
		Type:             llmrouterxds.EDSType,
		AffectedServices: []string{affectedServices},
		Reason:           "BackendSpecChanged",
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *EndpointSliceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// 自定义过滤条件

	//  该方式属于基于已有的威predicate进行添加对指定监控的key labels的EndpointSlice进行事件过滤
	//   且: 关注ResourcesVersion变化的事件
	// 	 且: 关注endpoint就绪的事件
	epsPredicate := predicate.And(
		// 具有指定key的标签
		predicate.NewPredicateFuncs(func(object client.Object) bool {
			svcName, ok := object.GetLabels()["kubernetes.io/service-name"]
			return ok && r.XDSManager.IsWatchedService(svcName)
		}),
		// predicate.NewPredicateFuncs(func(object client.Object) bool {
		// 	eps := object.(*discoveryv1.EndpointSlice)
		// 	for _, ep := range eps.Endpoints {
		// 		if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
		// 			return true
		// 		}
		// 	}
		// 	return false
		// }),
		// predicate.ResourceVersionChangedPredicate{}, // 资源版本变化
	)
	return ctrl.NewControllerManagedBy(mgr).
		For(&discoveryv1.EndpointSlice{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 5, // concurrency
		}).
		WithEventFilter(epsPredicate).
		Named("endpointslice").
		Complete(r)
}

func ExtractReadyEndpointsFromEndpointSlice(eps discoveryv1.EndpointSlice, serviceCfg llmrouterxds.ServiceConfig) []*llmrouterxds.LLMRouterEndpoint {
	edsEndpoints := make([]*llmrouterxds.LLMRouterEndpoint, 0, len(eps.Endpoints))
	for _, ep := range eps.Endpoints {
		if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
			for _, port := range eps.Ports {
				if port.Port != nil && *port.Port == *serviceCfg.TargetPort {
					for _, addr := range ep.Addresses {
						edsEndpoints = append(edsEndpoints, &llmrouterxds.LLMRouterEndpoint{
							Address: fmt.Sprintf("%s:%d", addr, serviceCfg.TargetPort),
						})
					}
				} else if port.Name != nil && *port.Name == serviceCfg.TargetPortName {
					for _, addr := range ep.Addresses {
						edsEndpoints = append(edsEndpoints, &llmrouterxds.LLMRouterEndpoint{
							Address: fmt.Sprintf("%s:%d", addr, *port.Port),
						})
					}
				}
			}
		}
	}
	return edsEndpoints
}
