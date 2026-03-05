package core

import (
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type HTTPForward struct {
	http.Client
}

func NewHTTPForward() *HTTPForward {
	// 传输层配置信息，提高复用
	transport := http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // 当没有使用gzip时，阻止自动压缩
	}
	return &HTTPForward{
		Client: http.Client{
			Transport: &transport,
			Timeout:   0},
	}
}
func (hf *HTTPForward) Do(req *http.Request) (*http.Response, error) {
	// todo: using ctx print log/span
	ctx, span := otel.Tracer("forward").Start(req.Context(), "router.forward",
		trace.WithAttributes(
			attribute.String("path", req.URL.Path),
			attribute.String("method", req.Method),
		),
	)
	defer span.End()
	// 注入当前otel上下文到http头，并发送给后端Pod扩展该Trace
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
	return hf.Client.Do(req)
}

func (hf *HTTPForward) TestDo(req *http.Request) (*http.Response, error) {
	// todo: using ctx print log/span
	fmt.Printf("HTTP Rquest:%+v ", req)
	return nil, nil
}
