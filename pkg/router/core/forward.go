package core

import (
	"net"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type HTTPForward struct {
	http.Client
}

func NewHTTPForward() *HTTPForward {
	// 传输层配置信息，提高复用
	transport := http.Transport{
		// 1. 连接池配置：直连 Pod IP 时，PerHost 决定了到单个 Pod 的最大复用连接数
		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 500,
		IdleConnTimeout:     90 * time.Second,
		// 2. 防止标准库自动解压 gzip
		DisableCompression: true,
		// 3. 底层TCP连接的超时控制
		// 如果某个 Pod 突然假死或网络不通，保证在 3s 内快速失败，不让请求一直卡着
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		// 4. 首字节超时控制
		// LLM 推理 TTFT (首字时间) 可能较长，但如果超过 30s 还没返回响应头，大概率后端挂了
		ResponseHeaderTimeout: 30 * time.Second,
		// 5. todo: 如果后端 LLM 支持 HTTP/2 (如某些 vLLM 配置)，可以尝试开启
		// ForceAttemptHTTP2: true,

	}
	return &HTTPForward{
		Client: http.Client{
			Transport: &transport,
			// 5. LLM的流式响应必须设置超时为0
			Timeout: 0},
	}
}

func (hf *HTTPForward) Do(req *http.Request) (*http.Response, error) {

	// ctx, span := otel.Tracer("forward").Start(req.Context(), "router.forward",
	// 	trace.WithAttributes(
	// 		attribute.String("path", req.URL.Path),
	// 		attribute.String("method", req.Method),
	// 	),
	// )
	// defer span.End()
	// 注入当前otel上下文到http头，并发送给后端Pod扩展该Trace
	otel.GetTextMapPropagator().Inject(req.Context(), propagation.HeaderCarrier(req.Header))
	return hf.Client.Do(req)
}

// Test
// func (hf *HTTPForward) Do(req *http.Request) (*http.Response, error) {
// 	// todo: using ctx print log/span
// 	fmt.Printf("HTTP Rquest:%+v ", req)
// 	ret := []byte(`{"content":"test success"}`)
// 	resp := &http.Response{
// 		Status:        "200 OK",
// 		StatusCode:    200,
// 		Proto:         "HTTP/1.1",
// 		ProtoMajor:    1,
// 		ProtoMinor:    1,
// 		Body:          io.NopCloser(bytes.NewReader(ret)),
// 		ContentLength: int64(len(ret)),
// 		Header:        make(http.Header),
// 	}
// 	resp.Header.Set("Content-Type", "application/json")
// 	return resp, nil
// }
