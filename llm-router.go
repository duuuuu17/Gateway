package main

import (
	"context"
	"control-plane-model-test/pkg/config"
	"control-plane-model-test/pkg/router/core"
	"control-plane-model-test/pkg/router/handler"
	"control-plane-model-test/pkg/router/inbound"
	"control-plane-model-test/pkg/router/outbound"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// var (
// 	models           []string
// 	Endpoints        []string
// 	canaryRatio      float64
// 	enabledStreaming bool
// 	uriSuffix        = "/openai/v1/chat/completions"
// )

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// load yaml configuration
	path := "./config.yaml"
	storage, err := config.Initialization(ctx, path)
	if err != nil {
		panic(err)
	}

	// 上下文监听整个项目，实现graceful停止项目
	// 启用监听
	storages := []config.ConfigReader{storage}
	route := InitialRouterModel(storages)
	// 调试代码：start
	slogYAMLFileConfig := slog.AnyValue(route.GetConfigs()[0].GetConfig())
	config := slog.Attr{Key: "YAMLFileConfig", Value: slogYAMLFileConfig}
	slog.Info("load backend configuration:", config)
	// 调试代码：end
	apiServe := &http.Server{
		Addr: ":8080",
		// Handler: , // 可能需要实现一个handler
	}
	http.HandleFunc("/", route.HandleFunc)

	// 子线程监听服务意外的错误没
	apiErrors := make(chan error, 1)
	go func() {
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

	// "/openai/v1/chat/completions"
	// log.Println("LLM Router listening on :8080")
}
func InitialRouterModel(cfgs []config.ConfigReader) *handler.Router {
	//todo: 调用实例的构造函数获取对象

	inboundsRegistry := inbound.NewInboundAdapterRegistry()
	inboundsRegistry.AddInboundAdapter("openai", inbound.NewOpenAIInBoundAdapter())

	outboundsRegistry := outbound.NewOutBoundAdapterRegistry()
	outboundsRegistry.AddOutboundRegistry("openai", outbound.NewOpenAIOutBoundAdapter())

	forwardClient := core.NewHTTPForward()
	dep := handler.RouterDeps{
		Configs:  cfgs,
		Inbound:  inboundsRegistry,
		Outbound: outboundsRegistry,
		Forward:  forwardClient,
	}
	return handler.NewRouter(dep)
}
