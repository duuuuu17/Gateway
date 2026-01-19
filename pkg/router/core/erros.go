package core

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/duuuuu17/llm-router-operator/pkg/metrics"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// 此错误类型为覆盖业务上下文类型，应具有公共性
type ContextErr struct {
	Req            *http.Request
	Span           trace.Span
	ResponseWriter http.ResponseWriter
}
type handleFunc func(*ContextErr, error)
type ErrorHandleFuncMap struct {
	m map[error]handleFunc
}

func NewErrorHandleFuncMap() *ErrorHandleFuncMap {
	errMap := make(map[error]handleFunc)
	errMap[ErrBackend4xx] = errBackend4xxFunc
	errMap[ErrBackend5xx] = errBackend5xxFunc
	errMap[ErrClientCancel] = errClientCancel
	errMap[ErrInvalidRequest] = errInvalidRequest
	errMap[ErrUnsupportProtocol] = errUnsupportProtocol
	errMap[ErrNotMatchingBackend] = errNotMatchingBackend
	errMap[ErrStreamUnsupport] = errStreamUnsupport
	return &ErrorHandleFuncMap{errMap}
}
func (e *ErrorHandleFuncMap) HandleErrorFunc(ce *ContextErr, err error) {

	// 如果子模块返回的错误为自定以的简单错误类型，则能相等性匹配
	if handler, ok := e.m[err]; ok {
		handler(ce, err)
		return
	}
	// 否则需要进行语义匹配
	for target, handler := range e.m {
		if errors.Is(err, target) {
			handler(ce, err)
			return
		}
	}
	defaultErrorHandler(ce, err)
}
func defaultErrorHandler(ce *ContextErr, err error) {

	ce.Span.SetStatus(codes.Error, "internal server error")
	ce.Span.RecordError(err)

	metrics.HTTPRequestTotal.WithLabelValues(
		ce.Req.Method,
		metrics.GetPathTemplate(ce.Req.URL.Path),
		strconv.Itoa(500),
		"false",
	).Inc()
	http.Error(ce.ResponseWriter, "internal server error", 500)
}

func errStreamUnsupport(ce *ContextErr, err error) {

	ce.Span.SetStatus(codes.Error, err.Error())
	ce.Span.RecordError(err)

	metrics.HTTPRequestTotal.WithLabelValues(
		ce.Req.Method,
		metrics.GetPathTemplate(ce.Req.URL.Path),
		"500",
		"false",
	).Inc()
	http.Error(ce.ResponseWriter, "internal server error", 500)
}
func errInvalidRequest(ce *ContextErr, err error) {

	ce.Span.SetStatus(codes.Error, err.Error())
	ce.Span.RecordError(err)

	metrics.HTTPRequestTotal.WithLabelValues(
		ce.Req.Method,
		metrics.GetPathTemplate(ce.Req.URL.Path),
		"400",
		"false",
	).Inc()
	http.Error(ce.ResponseWriter, "invalid request,please check the request body", 400)
}
func errNotMatchingBackend(ce *ContextErr, err error) {

	ce.Span.SetStatus(codes.Error, err.Error())
	ce.Span.RecordError(err)

	metrics.HTTPRequestTotal.WithLabelValues(
		ce.Req.Method,
		metrics.GetPathTemplate(ce.Req.URL.Path),
		"500",
		"false",
	).Inc()
	http.Error(ce.ResponseWriter, "internal server error", 500)
}
func errUnsupportProtocol(ce *ContextErr, err error) {

	ce.Span.SetStatus(codes.Error, err.Error())
	ce.Span.RecordError(err)

	metrics.HTTPRequestTotal.WithLabelValues(
		ce.Req.Method,
		metrics.GetPathTemplate(ce.Req.URL.Path),
		"400",
		"false",
	).Inc()
	http.Error(ce.ResponseWriter, "unsupported protocol", 400)
}
func errBackend4xxFunc(ce *ContextErr, err error) {
	slog.Error(err.Error(),
		"request-id", ce.Req.Header.Get("X-request-id"),
	)
	ce.Span.SetStatus(codes.Error, err.Error())
	ce.Span.RecordError(err)

	metrics.BackendErrorsTotal.
		WithLabelValues("4xx").
		Inc()

	http.Error(ce.ResponseWriter, "invalid request", 400)
}

func errBackend5xxFunc(ce *ContextErr, err error) {
	slog.Error(err.Error(),
		"request-id", ce.Req.Header.Get("X-request-id"),
	)
	ce.Span.SetStatus(codes.Error, err.Error())
	ce.Span.RecordError(err)

	metrics.BackendErrorsTotal.
		WithLabelValues("5xx").
		Inc()
	http.Error(ce.ResponseWriter, "backend service unavailable", 500)
}

func errClientCancel(ce *ContextErr, err error) {
	ce.Span.AddEvent("Client cancel connection")
	ce.Span.SetStatus(codes.Error, err.Error())

	metrics.HTTPRequestTotal.WithLabelValues(
		ce.Req.Method,
		metrics.GetPathTemplate(ce.Req.URL.Path),
		strconv.Itoa(500),
		"true",
	).Inc()
	http.Error(ce.ResponseWriter, "Client cancel connection", 499) // 使用nginx的客户端取消连接的错误码
}
