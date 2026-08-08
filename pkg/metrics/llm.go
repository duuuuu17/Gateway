package metrics

import "github.com/prometheus/client_golang/prometheus"

// llm相关的业务指标
// 统计选择后端Pod的累计数
// {"pod_service_name", "model"},
var LLMBackendSelectedTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "llm",
		Subsystem: "router",
		Name:      "llm_backend_selected_total",
		Help:      "totail number of LLM-Router selected backend to requests.",
	},
	[]string{"pod_service_name", "model"},
)

// 执行Stream返回的当前连接数
// {"backend", "model"}
var LLMStreamActiveConnections = prometheus.NewGaugeVec(

	prometheus.GaugeOpts{
		Namespace: "llm",
		Subsystem: "router",
		Name:      "stream_active_connections",
		Help:      "Number of active streaming connections",
	},
	[]string{"backend", "model"},
)

// var LLMStreamActiveConnections = prometheus.NewGauge(
// 	prometheus.GaugeOpts{
// 		Namespace: "llm",
// 		Subsystem: "router",
// 		Name:      "stream_active_connections",
// 		Help:      "Number of active streaming connections",
// 	},
// )

// 执行Stream返回消息的块累计数
// {"backend", "model"}
var LLMStreamChunksTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "llm",
		Subsystem: "router",
		Name:      "stream_chunks_total",
		Help:      "Total number of streamed chunks",
	},
	[]string{"backend", "model"},
)

// 后端Pod处理请求的耗时
// {"backend", "protocol"}
var BackendRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "llm",
		Subsystem: "router",
		Name:      "backend_request_duration_seconds",
		Buckets:   []float64{0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 30, 60},
	},
	[]string{"backend", "protocol"},
)

// 后端执行发送错误的累计数
// {"backend", "status"}
var BackendErrorsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "llm",
		Subsystem: "router",
		Name:      "backend_errors_total",
		Help:      "Total backend errors",
	},
	[]string{"backend", "status"},
)

func init() {

	prometheus.MustRegister(
		LLMBackendSelectedTotal,
		LLMStreamActiveConnections,
		LLMStreamChunksTotal,
		BackendErrorsTotal,
		BackendRequestDuration,
	)

}
