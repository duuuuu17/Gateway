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
	"strings"

	"github.com/duuuuu17/llm-router-operator/internal/controller/utils"
	llmrouterxds "github.com/duuuuu17/llm-router-operator/internal/llmrouter-xds"
	"github.com/go-logr/logr"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

const (
	SVCLabel = "kubernetes.io/service-name"
)

func TransformerToLLMRouterEndpointAssignment() llmrouterxds.LLMRouterEndpointAssignment {
	return llmrouterxds.LLMRouterEndpointAssignment{}
}

// +kubebuilder:rbac:groups=discovery.k8s.io,resources=endpointslices,verbs=get;list;watch

func (r *EndpointSliceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// log := logf.FromContext(ctx)
	var eps discoveryv1.EndpointSlice
	if err := r.Get(ctx, req.NamespacedName, &eps); err != nil {
		if apierrors.IsNotFound(err) {
			// 如果 EndpointSlice 被删除了，r.Get 会返回 NotFound。
			// 此时我们无法通过 eps.Labels 获取 svcName。
			// 简单处理：忽略，或者触发一次全局 EDS 重算。
			return utils.Reconciled()
		}
		msg := fmt.Sprintf("Failedt to get EndpointSlice, error: %s", err.Error())
		return utils.RequeueErrCheck(ctx, err, msg)
	}
	svcName := eps.Labels[SVCLabel]
	// 获取当前出发reconciler的endpointslice的servicename的配置
	serviceCfg, exists := r.XDSManager.GetServiceConfig(svcName)
	if !exists {
		return ctrl.Result{}, nil
	}
	// 【关键优化】：当Pod超过100个时，EndpointSlice会进行分片处理，避免分片丢失问题
	// 需要通过该 Service, 去List到所有关联的 EndpointSlice
	var epsList discoveryv1.EndpointSliceList
	if err := r.List(ctx, &epsList,
		client.MatchingLabels{SVCLabel: svcName},
		client.InNamespace(eps.Namespace)); err != nil {
		return utils.RequeueErrCheck(ctx, err, "Failed to list all EndpointSlices for service")
	}
	edsEndpoints := make([]*llmrouterxds.LLMRouterEndpoint, 0)
	for _, slice := range epsList.Items {
		edsEndpoints = append(edsEndpoints, ExtractReadyEndpointsFromEndpointSlice(slice, serviceCfg)...)
	}

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
	// slog.Info("serviceConfig", "[serviceConfig]", *serviceCfg.TargetPort)
	edsEndpoints := make([]*llmrouterxds.LLMRouterEndpoint, 0, len(eps.Endpoints))
	// slog.Info("endpoints", "[endpoints]", eps.Endpoints)
	var targetPort int32 = -1
	for _, port := range eps.Ports {
		if port.Port != nil && *port.Port == *serviceCfg.TargetPort {
			targetPort = *port.Port
			break
		}
		if port.Name != nil && *port.Name == serviceCfg.TargetPortName {
			targetPort = *port.Port
			break
		}
	}
	if targetPort == -1 {
		return []*llmrouterxds.LLMRouterEndpoint{}
	}
	// second hanlde: literally get target service: address+port
	for _, ep := range eps.Endpoints {
		if ep.Conditions.Terminating != nil && *ep.Conditions.Terminating {
			continue
		}
		for _, addr := range ep.Addresses {
			// 简单过滤掉 IPv6，如果你的环境不需要的话
			if strings.Contains(addr, ":") {
				continue
			}
			edsEndpoints = append(edsEndpoints, &llmrouterxds.LLMRouterEndpoint{
				Address: fmt.Sprintf("%s:%d", addr, targetPort),
			})
		}
	}
	return edsEndpoints
}
