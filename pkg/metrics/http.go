package metrics

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// http 指标
var HTTPRequestTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "llm",
	Subsystem: "router",
	Name:      "http_request_total",
	Help:      "total number of HTTP requests.",
},
	[]string{"method", "path", "status", "cancel"},
)

var HTTPRequestDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "llm",
		Subsystem: "router",
		Name:      "http_request_duration_seconds",
		Help:      "HTTP request duration in seconds.",
		Buckets:   prometheus.DefBuckets,
	},
	[]string{"method", "path"},
)

var HTTPConcurrentRequests = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Namespace: "llm",
		Subsystem: "router",
		Name:      "http_concurrecnt_requests",
		Help:      "Current number of concurrent HTTP requests.",
	},
)

func init() {

	prometheus.MustRegister(
		HTTPRequestTotal,
		HTTPRequestDuration,
		HTTPConcurrentRequests,
	)

}

// 获取路径样本，而避免统计时得到相同前缀，不同用户参数的路径样本导致出现高基数
func GetPathTemplate(path string) string {
	switch {
	case strings.HasPrefix(path, "/v1/chat/completions"):
		return "/v1/chat/completions"
	default:
		return path
	}
}
