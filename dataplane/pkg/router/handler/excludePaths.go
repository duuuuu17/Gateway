package handler

import (
	"net/http"
	"time"

	"github.com/duuuuu17/llm-router-operator/pkg/metrics"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type ExcludedEndpoints struct {
	ExcludedEndpoints map[string]struct{}
}

func NewExcludedEndpoints(paths ...string) *ExcludedEndpoints {
	endpoints := make(map[string]struct{})
	for _, path := range paths {
		endpoints[path] = struct{}{}
	}
	return &ExcludedEndpoints{endpoints}
}
func (e *ExcludedEndpoints) ShouldExcluded(path string) bool {
	if _, ok := e.ExcludedEndpoints[path]; ok {
		return true
	}
	return false
}

func TraceMiddleware(excluder *ExcludedEndpoints) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, req *http.Request) {
			metrics.HTTPConcurrentRequests.Inc()
			defer metrics.HTTPConcurrentRequests.Dec()
			// 检测请求处理花费时间
			start := time.Now()
			// 如果请求没有请求ID就生成
			if req.Header.Get("X-request-id") == "" {
				req.Header.Set("X-request-id", uuid.NewString())
			}
			if excluder.ShouldExcluded(req.URL.Path) {
				next.ServeHTTP(w, req)
			} else {
				// 所有通过router的顶层父Tracer创建
				tracer := otel.Tracer("router")
				// 如果客户端请求有注入trace信息，则需要在创建router的tracer前先提取
				ctx := otel.GetTextMapPropagator().Extract(req.Context(), propagation.HeaderCarrier(req.Header))
				ctx, rootSpan := tracer.Start(ctx, "router.handlerFunc",
					trace.WithAttributes(
						attribute.String("path", req.URL.Path),
						attribute.String("method", req.Method),
					),
				)
				defer rootSpan.End()
				req = req.WithContext(ctx) // 将当前tracer创建的父Span信息注入到上下文中, 并获取最新的req
				next.ServeHTTP(w, req)
			}
			duration := time.Since(start).Seconds()
			metrics.HTTPRequestDuration.WithLabelValues(req.Method, metrics.GetPathTemplate(req.URL.Path)).Observe(duration)
			defer func() { // 避免panic
				if err := recover(); err != nil {
					// 记录 panic 为 500
					status := "500"
					metrics.HTTPRequestTotal.WithLabelValues(req.Method, metrics.GetPathTemplate(req.URL.Path), status).Inc()
					metrics.HTTPRequestDuration.WithLabelValues(req.Method, metrics.GetPathTemplate(req.URL.Path)).Observe(time.Since(start).Seconds())
					panic(err) // re-panic if needed
				}
			}()
		}
	}
}
