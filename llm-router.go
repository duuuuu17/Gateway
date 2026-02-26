package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/duuuuu17/llm-router-operator/pkg/cmd"
	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/logs"
	"github.com/duuuuu17/llm-router-operator/pkg/metrics"
	"github.com/duuuuu17/llm-router-operator/pkg/otels"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
	"github.com/duuuuu17/llm-router-operator/pkg/router/filters"
	"github.com/duuuuu17/llm-router-operator/pkg/router/handler"
	"github.com/duuuuu17/llm-router-operator/pkg/router/inbound"
	"github.com/duuuuu17/llm-router-operator/pkg/router/outbound"
)

const configPath = "/etc/llm-router/config/config.yaml"
const path = "./config.yaml"
const grpcServerEndpoint = ":50051"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// logger initial
	logs.InitSlog(slog.LevelInfo)
	// traace inital
	otelConfig := otels.Config{
		ServiceName:      "llm-route",
		Version:          "0.0.1",
		TracePodEndpoint: "172.18.0.2:30318", // 当打包镜像时，可以通过环境变量或yaml配置文件声明
		Insecure:         true,
		Probability:      1,
	}
	_, shuwdown, err := otels.Init(ctx, otelConfig)
	defer shuwdown(ctx)
	if err != nil {
		slog.Error("start otel metrics model error", "Err: ", err)
	}

	// load yaml configuration
	// storage, err := config.Initialization(ctx, path)
	if err != nil {
		slog.Error("load yaml config file error", "Err: ", err)
	}
	storage := config.RouterConfig{}
	// 上下文监听整个项目，实现graceful停止项目
	// 启用监听
	route := InitialRouterModel(storage)

	excluder := handler.NewExcludedEndpoints("/healthz", "/readyz", "/metrics", "/debug/pprof", "/favicon.ico")
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", metrics.Healthz)
	mux.HandleFunc("/", handler.TraceMiddleware(excluder)(route.HandleFunc))
	apiServe := &http.Server{
		Addr:    ":8080",
		Handler: mux, // 可能需要实现一个handler
	}
	// Client与控制面的grpc server构建tcp连接
	endpointSelector := config.NewSelectorRegistry()
	// 创建的同时，自动调用处理循环函数
	clientStream := cmd.NewStreamClient(ctx, grpcServerEndpoint, &storage, endpointSelector)
	// 子线程监听服务意外的错误没
	apiErrors := make(chan error, 1)
	go func() {
		slog.Info("LLM Router listening on " + apiServe.Addr)
		if err := apiServe.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			apiErrors <- err
		}
	}()

	select {
	case <-ctx.Done():
		fmt.Println()
		slog.Info("开始退出HTTP服务...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		slog.Info("client grpc bidStream Closing...")
		clientStream.CloseStreamClient()
		slog.Info("client grpc bidStream Closed...")
		if err := apiServe.Shutdown(shutdownCtx); err != nil {
			slog.Error("HTTP服务退出失败, 强制关闭中", "Error: ", err)
			os.Exit(1)
		}
		slog.Info("HTTP 服务退出结束")
		os.Exit(0)
	case err := <-apiErrors:
		fmt.Printf("HTTP 服务启动/运行异常：%s\n", err.Error())
		os.Exit(1) // 异常退出，退出码 1
	}
}
func InitialRouterModel(cfgs config.RouterConfig) *handler.Router {
	//todo: 调用实例的构造函数获取对象

	inboundsRegistry := inbound.NewInboundAdapterRegistry()
	inboundsRegistry.AddInboundAdapter("openai", inbound.NewOpenAIInBoundAdapter())

	outboundsRegistry := outbound.NewOutBoundAdapterRegistry()
	outboundsRegistry.AddOutboundRegistry("openai", outbound.NewOpenAIOutBoundAdapter())

	forwardClient := core.NewHTTPForward()
	errsHandleMap := core.NewErrorHandleFuncMap()

	filtersRegistry := filters.NewSelectorRegistry()
	filtersRegistry.AddSelector("default", filters.NewPickFirstFilterPlicy())
	filtersRegistry.AddSelector("canary", filters.NewCanaryFilterPolicy())

	dep := handler.RouterDeps{
		Configs:         cfgs,
		Inbound:         inboundsRegistry,
		Outbound:        outboundsRegistry,
		Forward:         forwardClient,
		BackendSelector: filtersRegistry,
		ErrsHandleMap:   errsHandleMap,
	}
	// 调试代码：start
	slogYAMLFileConfig := slog.AnyValue(cfgs)
	config := slog.Attr{Key: "YAMLFileConfig", Value: slogYAMLFileConfig}
	slog.Info("load backend configuration:", config)
	// 调试代码：end
	return handler.NewRouter(dep)
}
