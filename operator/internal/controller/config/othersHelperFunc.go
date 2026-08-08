package controller

import (
	"context"

	configv1alpha1 "github.com/duuuuu17/llm-router-operator/api/config/v1alpha1"
	"github.com/duuuuu17/llm-router-operator/internal/controller/utils"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func (r *LLMRouterConfigReconciler) reconcileFinalizer(ctx context.Context, cr configv1alpha1.LLMRouterConfig) func() (reconcile.Result, error) {
	if !controllerutil.ContainsFinalizer(&cr, finalizer) {
		controllerutil.AddFinalizer(&cr, finalizer)
		if err := r.Update(ctx, &cr); err != nil {
			return func() (reconcile.Result, error) {
				return utils.RequeueErr(ctx, err, "add finalizer, but has error")
			}
		}
		// 第一次添加finalizer字段后，必须requeue重新协调一次
		// 保证CR资源的resourceVersion最新
		return func() (reconcile.Result, error) {
			return utils.Requeue()
		}
	}
	return nil
}
func (r *LLMRouterConfigReconciler) NeedUpdate(cr *configv1alpha1.LLMRouterConfig, dataHash string) bool {
	return cr.Status.ConfigDataHash == dataHash
}

// func (r *LLMRouterConfigReconciler) reconcileConfigMapCreateOrUpdate(ctx context.Context, cr *configv1alpha1.LLMRouterConfig) (reconcile.Result, error) {
// 	configMapName := utils.GenerateConfigMapName(*cr)
// 	desireDate, err := utils.BuildConfigMapData(*cr)
// 	if err != nil {
// 		return utils.RequeueErr(ctx, err, "build ConfigMap data error")
// 	}
// 	// 计算提前计算是否不需要更新操作
// 	dataHash := utils.ComputeMapHash(desireDate)
// 	if !r.NeedUpdate(cr, dataHash) {
// 		return utils.Reconciled()
// 	}

// 	// ConfigMap处理
// 	cm := &corev1.ConfigMap{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      configMapName,
// 			Namespace: cr.Namespace,
// 			Labels: map[string]string{
// 				"app.kubernetes.io/managed-by": "llm-router-operator"},
// 		},
// 	}
// 	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm,
// 		func() error {
// 			cm.Data = desireDate
// 			return controllerutil.SetControllerReference(cr, cm, r.Scheme)
// 		})
// 	// ConfigMap Ready/Synced/ConfigMapCreated
// 	if err != nil {
// 		r.setCondition(
// 			ctx, cr,
// 			"Synced", metav1.ConditionFalse, "UpdateFailed",
// 			err.Error())
// 		r.setReady(ctx, cr, false, "SyncFailed")
// 		return utils.RequeueErr(ctx, err, "update configmap failed")
// 	}
// 	log.FromContext(ctx).Info("configmap reconciled", "operation", result)
// 	// 计算当前的哈希值
// 	cr.Status.ConfigDataHash = dataHash
// 	// create or update operator success
// 	cr.Status.Synced = true
// 	cr.Status.ConfigMapName = configMapName
// 	r.setCondition(
// 		ctx, cr,
// 		"ConfigMapCreated", metav1.ConditionTrue, string(result),
// 		"ConfigMap is up to date")
// 	r.setCondition(
// 		ctx, cr,
// 		"Synced", metav1.ConditionTrue, "ConfigMapSynced",
// 		"CR Spec has been applied")
// 	r.setReady(ctx, cr, true, "Ready")
// 	if err := r.Status().Update(ctx, cr); err != nil {
// 		return utils.Reconciled()
// 	}
// 	return utils.Reconciled()
// }
