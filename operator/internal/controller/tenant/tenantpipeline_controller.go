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

package tenant

import (
	"context"
	"fmt"
	"strings"

	tenantv1alpha1 "github.com/duuuuu17/llm-router-operator/api/tenant/v1alpha1"
	"github.com/duuuuu17/llm-router-operator/internal/controller/utils"
	llmrouterxds "github.com/duuuuu17/llm-router-operator/internal/llmrouter-xds"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	finalizer = "tenant.llm-router.example.io/tenants_pipeline_config"
	TypeTDS   = "tds"
)

// TenantPipelineReconciler reconciles a TenantPipeline object
type TenantPipelineReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	XDSManager llmrouterxds.XDSManager
	Logger     logr.Logger
	Debouncer  llmrouterxds.Debouncer
}

// +kubebuilder:rbac:groups=tenant.llm-router.example.io,resources=tenantpipelines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=tenant.llm-router.example.io,resources=tenantpipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=tenant.llm-router.example.io,resources=tenantpipelines/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.

// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.4/pkg/reconcile
func (r *TenantPipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	var tenantCfgs tenantv1alpha1.TenantPipeline
	if err := r.Get(ctx, req.NamespacedName, &tenantCfgs); err != nil {
		if apierrors.IsNotFound(err) {
			//  conditions:
			// 1. CR is not create
			// 2. CR was deleted
			// r.XDSManager.DeleteAllXDSs()
			r.Logger.Info("tenant is not found", "ItemKey", req.NamespacedName)
			return utils.Reconciled()
		}
		return utils.RequeueErrCheck(ctx, err, "can't got the Tenant Pipeline CR obj")
	}
	if !tenantCfgs.DeletionTimestamp.IsZero() {
		r.Logger.Info("remove tenant!")
		// Ensure Finalizer, need deleted  others subresources
		return r.reconcileDelete(ctx, &tenantCfgs)
	}
	// ensure Finalizer field is exist
	if !controllerutil.ContainsFinalizer(&tenantCfgs, finalizer) {
		controllerutil.AddFinalizer(&tenantCfgs, finalizer)
		if err := r.Update(ctx, &tenantCfgs); err != nil {
			return utils.RequeueErrCheck(ctx, err, "failed to add finalizer")
		}
		return utils.Reconciled()
	}
	// Upsert logic
	events, err := r.Upsert(ctx, &tenantCfgs)
	if err != nil {
		return utils.RequeueErrCheck(ctx, err, "Tenant upsert operation failure")
	}
	r.Logger.Info("Add New Tenant", "tenant-name", tenantCfgs.Name, "tenant-namespace", tenantCfgs.Namespace)
	go r.pushXDSEvent(events)
	return ctrl.Result{}, nil
}
func (r *TenantPipelineReconciler) pushXDSEvent(events []llmrouterxds.ReconcilerPushEvent) {
	for _, ev := range events {
		r.Logger.Info("enqueue TDS", "name", ev.Type)
		r.Debouncer.Enqueue(ev)
	}
}

// upsert TDS
func (r *TenantPipelineReconciler) Upsert(ctx context.Context, cr *tenantv1alpha1.TenantPipeline) ([]llmrouterxds.ReconcilerPushEvent, error) {
	result := strings.Join([]string{cr.Namespace, cr.Name, string(cr.ObjectMeta.UID)}, "/")
	tenantsPipelineCfgDTO, err := llmrouterxds.NewTenantsDTO(cr)
	if err != nil {
		return nil, err
	}
	if !r.XDSManager.UpsertTDSByUID(tenantsPipelineCfgDTO.ToTenantPipeline()) {
		return nil, fmt.Errorf("the tenants pipeline config same as TDS in the TDSController!")
	}
	dirtyEvents := make([]llmrouterxds.ReconcilerPushEvent, 0)
	// TDS直接根据TDS's UID执行全量替换
	dirtyEvents = append(dirtyEvents, llmrouterxds.NewEvent(TypeTDS, result, false))

	return dirtyEvents, nil
}

// check tenantID is duplication
func (r *TenantPipelineReconciler) IsTenantIDDuplication(cr *tenantv1alpha1.TenantPipeline) bool {
	// todo: check all declare tenants whether exists
	return false
}
func (r *TenantPipelineReconciler) DeleteTDSCache(tenantID string) {
	r.XDSManager.DeleteTDSCache(tenantID)
}

// 执行删除逻辑，并在处理完毕后，重新协调
func (r *TenantPipelineReconciler) reconcileDelete(ctx context.Context, cr *tenantv1alpha1.TenantPipeline) (reconcile.Result, error) {
	if controllerutil.ContainsFinalizer(cr, finalizer) {
		// logic deletion
		// namespace/name/uid作为indexer's key
		result := strings.Join([]string{cr.Namespace, cr.Name, string(cr.ObjectMeta.UID)}, "/")
		r.Logger.Info("delete TDSCache", "tenant", result)
		r.DeleteTDSCache(result)
		controllerutil.RemoveFinalizer(cr, finalizer)
		if err := r.Update(ctx, cr); err != nil {
			return utils.RequeueErrCheck(ctx, err, "can't removefinalizer")
		}
		r.Logger.Info("remove fianlizer field", "tenant-name", cr.Name, "tenant-namespace", cr.Namespace)
		dirtyEvents := make([]llmrouterxds.ReconcilerPushEvent, 0)
		dirtyEvents = append(dirtyEvents, llmrouterxds.NewEvent(TypeTDS, result, true))
		go r.pushXDSEvent(dirtyEvents)
	}
	return utils.Reconciled()
}

// SetupWithManager sets up the controller with the Manager.
func (r *TenantPipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&tenantv1alpha1.TenantPipeline{}).
		Named("tenant-tenantpipeline").
		Complete(r)
}
