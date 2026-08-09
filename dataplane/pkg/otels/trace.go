package otels

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Config struct {
	ServiceName      string  `yaml:"serviceName"`
	Version          string  `yaml:"version"`
	TracePodEndpoint string  `yaml:"tracePodEndpoint"`
	Probability      float64 `yaml:"probability"`
	Insecure         bool    `yaml:"insecure"`
}

func Init(ctx context.Context, cfg Config) (trace.TracerProvider, func(context.Context), error) {

	var tp trace.TracerProvider
	var shutdown func(context.Context)
	if cfg.TracePodEndpoint == "" {
		tp = noop.NewTracerProvider()
		shutdown = func(ctx context.Context) {}
	} else {
		// 采集后端Pod地址
		opts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(cfg.TracePodEndpoint),
			otlptracegrpc.WithTimeout(20 * time.Second),
		}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		// 导出器声明: 使用的otlp的grpc方式
		exporter, err := otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return nil, nil, err
		}
		// 服务元信息声明
		hostname, _ := os.Hostname()
		res := resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(cfg.ServiceName),
			semconv.ServiceVersionKey.String(cfg.Version),
			semconv.HostNameKey.String(hostname),
		)
		// traceProvider配置声明
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.ParentBased(
				sdktrace.TraceIDRatioBased(cfg.Probability),
			)),
		)
		shutdown = func(ctx context.Context) {
			//nolint:errcheck
			if err := tp.(*sdktrace.TracerProvider).Shutdown(ctx); err != nil {
				slog.Warn("close trace provider failure")
			}
		}
	}
	//  全局注册
	otel.SetTracerProvider(tp)
	// 跨服务注入tracer信息到请求头的配置声明
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.Baggage{},
		propagation.TraceContext{},
	))
	slog.Info("TraceProvider global initialization successful!")
	return tp, shutdown, nil
}
