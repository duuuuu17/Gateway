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

	configv1alpha1 "github.com/duuuuu17/llm-router-operator/api/config/v1alpha1"
	"github.com/duuuuu17/llm-router-operator/internal/controller/utils"
	llmrouterxds "github.com/duuuuu17/llm-router-operator/internal/llmrouter-xds"
	"github.com/go-logr/logr"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	configMapPath = ".spec.configMapName"
	finalizer     = "github.com/duuuuu17.llm-router-config"
	TypeCDS       = "cds"
	TypeEDS       = "eds"
	TypeRDS       = "rds"
)

//todo: [finished]
// CR Reconciler → CDS / RDS
//EndpointSlice Informer → EDS

// LLMRouterConfigReconciler reconciles a LLMRouterConfig object
type LLMRouterConfigReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	XDSManager llmrouterxds.XDSManager
	Logger     logr.Logger
	Debouncer  llmrouterxds.Debouncer
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
		if apierrors.IsNotFound(err) {
			// r.XDSManager.DeleteAllXDSs() // 未查到CR实例，删除本地缓存
			r.Logger.V(1).Info("the CR is not found!")
			return utils.Reconciled()
		}
		return utils.RequeueErrCheck(ctx, err, "can't got the CR obj")
	}
	// Handle deletion
	if !cfg.DeletionTimestamp.IsZero() {
		// Ensure Finalizer, need deleted  others subresources
		return r.reconcileDelete(ctx, &cfg)
	}
	// check wether finalizer is set
	reconcileResultFunc := r.reconcileFinalizer(ctx, cfg)
	if reconcileResultFunc != nil {
		return reconcileResultFunc()
	}

	// 防止CR并没有backends
	if cfg.Spec.Backends == nil {
		// 可以选择初始化一个空结构，或直接返回（取决于业务需求）
		return ctrl.Result{}, nil
	}
	// need update servicename
	events := r.UpdateReconcile(ctx, cfg.Spec.Backends)
	if events == nil {
		return utils.Reconciled()
	}
	// 根据更新的服务和删除的服务列表构建事件并推送给debouncer
	go r.pushXDSEvent(events)

	return ctrl.Result{}, nil
}
func (r *LLMRouterConfigReconciler) pushXDSEvent(events []llmrouterxds.ReconcilerPushEvent) {
	for _, ev := range events {
		r.Debouncer.Enqueue(ev)
	}
}

func (r *LLMRouterConfigReconciler) GetMatchingLabelsEndpointSlice(ctx context.Context, svcName string) *discoveryv1.EndpointSlice {
	var esList discoveryv1.EndpointSliceList
	if err := r.List(ctx, &esList, client.MatchingLabels{
		"kubernetes.io/service-name": svcName,
	}); err != nil {
		return nil
	}
	if len(esList.Items) == 0 {
		return &discoveryv1.EndpointSlice{}
	}
	return &esList.Items[0]
}
func (r *LLMRouterConfigReconciler) UpdateReconcile(ctx context.Context, backends *configv1alpha1.BackendConfig) (push []llmrouterxds.ReconcilerPushEvent) {
	if backends == nil {
		r.Logger.Info("Backends is nil, skipping reconciliation")
		return nil
	}
	desiredServiceNames := make([]string, 0, len(backends.Backends))
	// 用于收集本轮 Reconciler 发生缓存数据更新的服务列表
	dirtyServices := make([]string, 0)
	// 用于收集本轮 Reconcile 产生的需要推送的事件
	dirtyEvents := make([]llmrouterxds.ReconcilerPushEvent, 0)
	// 处理Create、updae还是delete本质是desire与current的serviceNames列表的集合进行比较
	for _, backend := range backends.Backends {
		serviceName := backend.Name
		desiredServiceNames = append(desiredServiceNames, serviceName)
		// DTO
		clusterSpec, routerSpec, err := llmrouterxds.NewClusterSpec(&backend)
		if err != nil { //  如果出现CanaryRatio转换float64失败就直接传递空推送
			return dirtyEvents
		}

		cluster := clusterSpec.ToLLMRouterCluster()
		serviceCfg := clusterSpec.ToLLMRouterServiceConfig()
		router := routerSpec.ToLLMRouterrouting()
		// ===========================
		// Phase 1: 处理 Create 和 Update
		// ===========================
		isChange := false
		if r.XDSManager.UpdateOrCreateServiceConfig(serviceName, serviceCfg) {
			isChange = true
		}
		if r.XDSManager.UpdateOrCreateCDS(serviceName, cluster, serviceCfg) {
			isChange = true
			dirtyEvents = append(dirtyEvents, llmrouterxds.NewEvent(TypeCDS, serviceName, false))
		}
		if r.XDSManager.UpdateOrCreateRDS(serviceName, router, serviceCfg) {
			isChange = true
			dirtyEvents = append(dirtyEvents, llmrouterxds.NewEvent(TypeRDS, serviceName, false))
		}
		// 需要确认创建的eds,以及修改port时，需要主动推送
		es := r.GetMatchingLabelsEndpointSlice(ctx, serviceName)
		// r.Logger.Info("endpointslice", "[endpointslice]", es.Endpoints)
		eds := ExtractReadyEndpointsFromEndpointSlice(*es, *serviceCfg)
		if r.XDSManager.UpdateOrCreateEDS(serviceName, eds) {
			isChange = true
			dirtyEvents = append(dirtyEvents, llmrouterxds.NewEvent(TypeEDS, serviceName, false))
		}
		if isChange {
			dirtyServices = append(dirtyServices, serviceName)
		}
	}
	// ===========================
	// Phase 2: 处理 Delete (Implicit Deletion)
	// ===========================
	// 此时 desiredServiceNames 包含了 CR 里所有的 Service
	// 调用 NeedRemovedService 找出那些 "Cache 里有但 CR 里没有" 的服务
	removedServices := r.XDSManager.NeedRemovedService(desiredServiceNames...)
	// r.Logger.Info("removekey", "servicename", removedServices)

	return AppendDeleteServiceEvents(removedServices, dirtyEvents)
}
func AppendDeleteServiceEvents(services []string, pushEvents []llmrouterxds.ReconcilerPushEvent) []llmrouterxds.ReconcilerPushEvent {
	if len(pushEvents) == 0 {
		pushEvents = make([]llmrouterxds.ReconcilerPushEvent, 0)
	}
	for _, service := range services {
		// 这里的 rmSvc 已经在 NeedRemovedService 内部被 delete(cache) 了
		// 只需要生成 Remove 事件通知 Debouncer -> Pusher
		// 生成所有类型的删除事件 (因为 Service 删了，CDS/RDS/EDS 都应该删)
		pushEvents = append(pushEvents, llmrouterxds.NewEvent(TypeCDS, service, true))
		pushEvents = append(pushEvents, llmrouterxds.NewEvent(TypeRDS, service, true))
		pushEvents = append(pushEvents, llmrouterxds.NewEvent(TypeEDS, service, true))
	}
	return pushEvents
}
func (r *LLMRouterConfigReconciler) DeleteXDSCache(backends *configv1alpha1.BackendConfig) {
	serviceNames := make([]string, 0, len(backends.Backends))
	for _, backend := range backends.Backends {
		serviceName := backend.Name
		serviceNames = append(serviceNames, serviceName)
	}
	if r.XDSManager.DeleteXDSs(serviceNames...) {
		r.Logger.Info("delete xDSs", "xDSs", serviceNames)
	}
	r.pushXDSEvent(AppendDeleteServiceEvents(serviceNames, nil))
}
func (r *LLMRouterConfigReconciler) reconcileDelete(ctx context.Context, cr *configv1alpha1.LLMRouterConfig) (reconcile.Result, error) {
	if controllerutil.ContainsFinalizer(cr, finalizer) {
		// 逻辑删除
		r.DeleteXDSCache(cr.Spec.Backends)
		controllerutil.RemoveFinalizer(cr, finalizer)
		if err := r.Update(ctx, cr); err != nil {
			return utils.RequeueErrCheck(ctx, err, "can't removefinalizer")
		}
		r.Logger.Info("remove fianlizer field")
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

// SetupWithManager sets up the controller with the Manager.
func (r *LLMRouterConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {

	return ctrl.NewControllerManagedBy(mgr).
		For(&configv1alpha1.LLMRouterConfig{}).
		Named("llmrouterconfig").
		// 当CRD的Spec在Controller中的检测到发生变化就触发
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Complete(r)
}
