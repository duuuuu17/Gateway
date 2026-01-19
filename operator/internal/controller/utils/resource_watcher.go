package utils

import (
	"context"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var _ handler.EventHandler = &ResourceWatcher{}

type ResourceWatcher struct {
	watched map[types.NamespacedName][]types.NamespacedName
}

func NewResourceWatcher() *ResourceWatcher {
	return &ResourceWatcher{
		watched: make(map[types.NamespacedName][]types.NamespacedName),
	}
}
func (w *ResourceWatcher) Watch(watchedName, dependentName types.NamespacedName) {
	existing := w.watched[watchedName]
	if slices.Contains(existing, dependentName) {
		return
	}
	w.watched[watchedName] = append(existing, dependentName)
}

func (w *ResourceWatcher) handleEvent(ctx context.Context, meta metav1.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	key := types.NamespacedName{
		Name:      meta.GetName(),
		Namespace: meta.GetNamespace(),
	}
	log.FromContext(ctx).V(1).Info("requeue-request once")
	for _, depNN := range w.watched[key] {
		req := reconcile.Request{NamespacedName: depNN}
		q.AddRateLimited(req)
	}
}

func (w *ResourceWatcher) Create(ctx context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	w.handleEvent(ctx, e.Object, q)
}

func (w *ResourceWatcher) Update(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// 通常使用 ObjectNew，除非你有特殊需求用 ObjectOld
	w.handleEvent(ctx, e.ObjectNew, q)
}

func (w *ResourceWatcher) Delete(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	w.handleEvent(ctx, e.Object, q)
}

func (w *ResourceWatcher) Generic(ctx context.Context, e event.GenericEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	w.handleEvent(ctx, e.Object, q)
}
