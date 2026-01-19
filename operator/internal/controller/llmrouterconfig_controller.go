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
	"strings"

	configv1alpha1 "github.com/duuuuu17/llm-router-operator/api/v1alpha1"
	"github.com/duuuuu17/llm-router-operator/internal/controller/utils"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	configMapPath = ".spec.configMapName"
	finalizer     = "llmrouter_config"
)

// LLMRouterConfigReconciler reconciles a LLMRouterConfig object
type LLMRouterConfigReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Watcher *utils.ResourceWatcher
}

// +kubebuilder:rbac:groups=config.llm-router.example.io,resources=llmrouterconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=config.llm-router.example.io,resources=llmrouterconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=config.llm-router.example.io,resources=llmrouterconfigs/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the LLMRouterConfig object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.4/pkg/reconcile
func (r *LLMRouterConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)
	// Fetch CR
	var cfg configv1alpha1.LLMRouterConfig
	if err := r.Get(ctx, req.NamespacedName, &cfg); err != nil {
		return utils.RequeueErrCheck(ctx, err, "can't got the CR obj")
	}
	// Handle deletion
	if !cfg.DeletionTimestamp.IsZero() {
		r.reconcileDelete(ctx, &cfg)

	}

	fqdn := "my-svc.my-namespace.svc.cluster.local"
	// 获取实际设置的host的endpointlist
	var endpointList discoveryv1.EndpointSliceList
	parts := strings.Split(fqdn, ",")
	namespace, svcName := parts[1], parts[0]
	// svcLabelSelector :=
	err := r.List(ctx, &endpointList,
		client.InNamespace(namespace),
		client.MatchingFields{
			"kubernetes.io/service-name": svcName,
		})
	if err != nil {
		return utils.Reconciled()
	}
	endpoints := []string{}
	for _, item := range endpointList.Items {
		for _, ep := range item.Endpoints {
			if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
				for _, addr := range ep.Addresses {
					endpoints = append(endpoints, addr)
				}
			}
		}
	}

	// Ensure Finalizer
	r.reconcileFinalizer(ctx, cfg)
	// validate spec
	// prepare create from cr setting configmapName
	r.reconcileConfigMapCreateOrUpdate(ctx, &cfg)

	return ctrl.Result{}, nil
}
func (r *LLMRouterConfigReconciler) NeedUpdate(cr *configv1alpha1.LLMRouterConfig, dataHash string) bool {
	if cr.Status.ConfigDataHash == dataHash {
		return false
	}
	return true
}
func (r *LLMRouterConfigReconciler) reconcileConfigMapCreateOrUpdate(ctx context.Context, cr *configv1alpha1.LLMRouterConfig) (reconcile.Result, error) {
	configMapName := utils.GenerateConfigMapName(*cr)
	desireDate, err := utils.BuildConfigMapData(*cr)
	if err != nil {
		return utils.RequeueErr(ctx, err, "build ConfigMap data error")
	}
	// 计算提前计算是否不需要更新操作
	dataHash := utils.ComputeMapHash(desireDate)
	if !r.NeedUpdate(cr, dataHash) {
		return utils.Reconciled()
	}

	// ConfigMap处理
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: cr.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "llm-router-operator"},
		},
	}
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm,
		func() error {
			cm.Data = desireDate
			return controllerutil.SetControllerReference(cr, cm, r.Scheme)
		})
	// ConfigMap Ready/Synced/ConfigMapCreated
	if err != nil {
		r.setCondition(
			ctx, cr,
			"Synced", metav1.ConditionFalse, "UpdateFailed",
			err.Error())
		r.setReady(ctx, cr, false, "SyncFailed")
		return utils.RequeueErr(ctx, err, "update configmap failed")
	}
	log.FromContext(ctx).Info("configmap reconciled", "operation", result)
	// 计算当前的哈希值
	cr.Status.ConfigDataHash = dataHash
	// create or update operator success
	cr.Status.Synced = true
	cr.Status.ConfigMapName = configMapName
	r.setCondition(
		ctx, cr,
		"ConfigMapCreated", metav1.ConditionTrue, string(result),
		"ConfigMap is up to date")
	r.setCondition(
		ctx, cr,
		"Synced", metav1.ConditionTrue, "ConfigMapSynced",
		"CR Spec has been applied")
	r.setReady(ctx, cr, true, "Ready")
	if err := r.Status().Update(ctx, cr); err != nil {
		return utils.Reconciled()
	}
	return utils.Reconciled()
}

func (r *LLMRouterConfigReconciler) setCondition(ctx context.Context, cr *configv1alpha1.LLMRouterConfig,
	conditionType string, status metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&cr.Status.Conditions,
		metav1.Condition{
			Type:               conditionType,
			Status:             status,
			Reason:             reason,
			Message:            msg,
			LastTransitionTime: metav1.Now(),
			ObservedGeneration: cr.Generation, // 注意需要设置此时Controller中CR的配置数据版本
		})
}
func (r *LLMRouterConfigReconciler) setReady(ctx context.Context, cr *configv1alpha1.LLMRouterConfig,
	ready bool, reason string) {
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	r.setCondition(ctx, cr, "Ready", status, reason, "")
}
func (r *LLMRouterConfigReconciler) reconcileDelete(ctx context.Context, cr *configv1alpha1.LLMRouterConfig) (reconcile.Result, error) {
	if controllerutil.ContainsFinalizer(cr, finalizer) {
		cmName := cr.Status.ConfigMapName
		if cmName != "" {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cmName,
					Namespace: cr.Namespace,
				},
			}
			_ = r.Delete(ctx, cm) // Ignore NotFound
			log.FromContext(ctx).Info("delete Configmap", "ConfigMapName", cmName)
		}
		controllerutil.RemoveFinalizer(cr, finalizer)
		if err := r.Update(ctx, cr); err != nil {
			return utils.RequeueErrCheck(ctx, err, "can't removefinalizer")
		}
		log.FromContext(ctx).Info("remove fianlizer field")
	}
	return utils.Reconciled()
}
func (r *LLMRouterConfigReconciler) reconcileFinalizer(ctx context.Context, cr configv1alpha1.LLMRouterConfig) (reconcile.Result, error) {
	if !controllerutil.ContainsFinalizer(&cr, finalizer) {
		controllerutil.AddFinalizer(&cr, finalizer)
		if err := r.Update(ctx, &cr); err != nil {
			return utils.RequeueErr(ctx, err, "add finalizer, but has error")
		}
		return utils.Requeue()
	}
	return utils.Reconciled()
}

// SetupWithManager sets up the controller with the Manager.
func (r *LLMRouterConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// 设置反向索引
	if err := mgr.GetCache().IndexField(
		context.Background(),
		&configv1alpha1.LLMRouterConfig{},
		configMapPath,
		func(o client.Object) []string {
			cfg, ok := o.(*configv1alpha1.LLMRouterConfig)
			if !ok || cfg.Spec.ConfigMapName == "" {
				return nil
			}
			return []string{cfg.Spec.ConfigMapName}
		},
	); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&configv1alpha1.LLMRouterConfig{}).
		Named("llmrouterconfig").
		Watches(
			&corev1.ConfigMap{},
			// r.Watcher,
			handler.EnqueueRequestsFromMapFunc(r.mapConfigMaptoRouters),
		).
		Complete(r)
}

func (r *LLMRouterConfigReconciler) mapConfigMaptoRouters(
	ctx context.Context,
	obj client.Object,
) []reconcile.Request {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return nil
	}
	var llmRouterConfigList configv1alpha1.LLMRouterConfigList

	// 利用indexer获取CR list
	if err := r.List(
		ctx,
		&llmRouterConfigList,
		client.MatchingFields{
			configMapPath: cm.Name,
		},
	); err != nil {
		return nil
	}
	// 遍历获取到的CR实例列表，并对它们依次发起reconcile.Request设置
	// 后续由handler加入到queue中
	reqs := make([]reconcile.Request, 0, len(llmRouterConfigList.Items))
	for _, cfg := range llmRouterConfigList.Items {
		reqs = append(reqs, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: cfg.Namespace,
				Name:      cfg.Name,
			},
		})
	}
	return reqs
}
