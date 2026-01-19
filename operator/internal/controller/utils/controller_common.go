package utils

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func Reconciled() (reconcile.Result, error) {
	return reconcile.Result{}, nil
}
func Requeue() (reconcile.Result, error) {
	return reconcile.Result{Requeue: true}, nil
}
func RequeueAfter(ctx context.Context, duration time.Duration, msg string, keysAndValues ...any) (reconcile.Result, error) {
	keysAndValues = append(keysAndValues, "duration", duration.String())
	if msg == "" {
		msg = "requeue-after"
	}
	log.FromContext(ctx).V(1).Info(msg, keysAndValues...)
	return reconcile.Result{
		RequeueAfter: duration,
	}, nil
}
func RequeueErr(ctx context.Context, err error, msg string, keysAndValues ...any) (reconcile.Result, error) {
	if msg == "" {
		msg = "requeue-error"
	}
	log.FromContext(ctx).Error(err, msg, keysAndValues...)
	return reconcile.Result{}, err
}
func RequeueErrCheck(ctx context.Context, err error, msg string, keysAndValues ...any) (reconcile.Result, error) {
	if apierrors.IsNotFound(err) {
		return Reconciled()
	}
	return RequeueErr(ctx, err, msg, keysAndValues...)
}
