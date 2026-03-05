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
	eventbus "github.com/duuuuu17/llm-router-operator/pkg/eventBus"
	"github.com/duuuuu17/llm-router-operator/pkg/logs"
	"github.com/duuuuu17/llm-router-operator/pkg/metrics"
	"github.com/duuuuu17/llm-router-operator/pkg/otels"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
	"github.com/duuuuu17/llm-router-operator/pkg/router/errs"
	"github.com/duuuuu17/llm-router-operator/pkg/router/filters"
	"github.com/duuuuu17/llm-router-operator/pkg/router/handler"
	"github.com/duuuuu17/llm-router-operator/pkg/router/inbound"
	"github.com/duuuuu17/llm-router-operator/pkg/router/outbound"
	"github.com/duuuuu17/llm-router-operator/pkg/router/plugin"
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
	// 上下文监听整个项目，实现graceful停止项目
	// 启用监听
	_, shuwdown, err := otels.Init(ctx, otelConfig)
	defer shuwdown(ctx)
	if err != nil {
		slog.Error("start otel metrics model error", "Err: ", err)
	}

	// load yaml configuration
	// RouterCfg, err := config.Initialization(ctx, path)
	if err != nil {
		slog.Error("load yaml config file error", "Err: ", err)
	}
	// 事件总线初始化
	eventBus := eventbus.NewBus()
	// xDS 配置对象初始化
	RouterCfg := config.RouterConfig{}
	// 本地初始化多租户文件
	tenantCfg, err := InitialTenantConfig(ctx, eventBus)
	if err != nil {
		slog.Error("TenantFile", "err", err.Error())
		return
	}
	// 初始化router模块
	route := InitialRouterModel(RouterCfg, eventBus)
	// 首次加载tenantConfig，创建多租户执行管线
	route.Reload(ctx, tenantCfg.GlobalConfig.Load().(*config.GlobalConfig))
	go route.Start(ctx)

	excluder := handler.NewExcludedEndpoints("/healthz", "/readyz", "/metrics", "/debug/pprof", "/favicon.ico")
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", metrics.Healthz)
	mux.HandleFunc("/", handler.TraceMiddleware(excluder)(route.ServeHTTP))
	// http.Handle("/", route)
	apiServe := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}
	// Client与控制面的grpc server构建tcp连接
	endpointSelector := config.NewSelectorRegistry()
	// 创建的同时，自动调用处理循环函数
	streamClientDep := &cmd.StreamClientDependencies{
		Endpoint:         grpcServerEndpoint,
		RouterCfg:        &RouterCfg,
		SelectorRegistry: endpointSelector,
		TenantCfg:        tenantCfg,
	}
	clientStream := cmd.NewStreamClient(ctx, streamClientDep)

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
func InitialTenantConfig(ctx context.Context, bus *eventbus.Bus) (*config.TenantCfg, error) {
	tenantFilePath := os.Getenv("BOOTSTRAP_FILE_PATH")
	tenantCfg := config.NewTenantCfg(tenantFilePath, bus)
	err := tenantCfg.Load()
	if err != nil {
		return nil, err
	}
	go tenantCfg.WatchFile(ctx)
	return tenantCfg, nil
}
func InitialRouterModel(cfgs config.RouterConfig, bus *eventbus.Bus) *handler.Router {
	//todo: 调用实例的构造函数获取对象

	inboundsRegistry := inbound.NewInboundAdapterRegistry()
	inboundsRegistry.AddInboundAdapter("openai", inbound.NewOpenAIInBoundAdapter())

	outboundsRegistry := outbound.NewOutBoundAdapterRegistry()
	outboundsRegistry.AddOutboundRegistry("openai", outbound.NewOpenAIOutBoundAdapter())

	forwardClient := core.NewHTTPForward()
	errsHandleMap := errs.NewErrorHandleFuncMap()

	filtersRegistry := filters.NewSelectorRegistry()
	filtersRegistry.AddSelector("default", filters.NewPickFirstFilterPlicy())
	filtersRegistry.AddSelector("canary", filters.NewCanaryFilterPolicy())
	pluginRegistry := plugin.NewRegistry()
	// pluginRegistry.Register()
	dep := handler.RouterDeps{
		Configs:         cfgs,
		Inbound:         inboundsRegistry,
		Outbound:        outboundsRegistry,
		Forward:         forwardClient,
		BackendSelector: filtersRegistry,
		ErrsHandleMap:   errsHandleMap,
		Plugins:         pluginRegistry,
		Bus:             bus,
	}
	// 调试代码：start
	slogYAMLFileConfig := slog.AnyValue(cfgs)
	config := slog.Attr{Key: "YAMLFileConfig", Value: slogYAMLFileConfig}
	slog.Info("load backend configuration:", config)
	// 调试代码：end

	return handler.NewRouter(dep)
}
