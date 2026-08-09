package utils

import (
	"context"
	"net"
	"strings"

	llmrouterxds "github.com/duuuuu17/llm-router-operator/internal/llmrouter-xds"
	"github.com/go-logr/logr"
	"google.golang.org/grpc"
)

var (
	GRPCSever  *grpc.Server
	grpcLogger logr.Logger
)

type DependenciesRPCListenerAndRegistryLLMRouterxDS struct {
	Log              logr.Logger
	Port             string
	XdsStore         llmrouterxds.XDSStore
	PushCh           llmrouterxds.Debouncer
	RespVersionCache llmrouterxds.CacheStreamAggregateResponses
}

func NewRPCListenerAndRegistryLLMRouterxDS(dep *DependenciesRPCListenerAndRegistryLLMRouterxDS) (*llmrouterxds.LLMRouterXDSServer, error) {
	grpcLogger = dep.Log
	if !strings.HasPrefix(dep.Port, ":50051") {
		dep.Port = ":" + dep.Port
	}
	lis, err := net.Listen("tcp", dep.Port)
	if err != nil {
		return nil, err
	}
	// 设置grpc Server
	GRPCSever = grpc.NewServer(
		grpc.ChainUnaryInterceptor(unaryInterceptorSlog),      // 一元拦截器（日志）
		grpc.ChainStreamInterceptor(streamLogInterceptorSlog), // 流拦截器（日志）
	)

	llmrouterxdsServer := llmrouterxds.NewLLMRouterXDSServer(grpcLogger, dep.XdsStore, dep.PushCh, dep.RespVersionCache)
	llmrouterxds.RegisterAggregatedDiscoveryServiceServer(GRPCSever, llmrouterxdsServer)
	grpcLogger.Info("Strating grpc server", "port", dep.Port)
	go func() {
		if err := GRPCSever.Serve(lis); err != nil || err != grpc.ErrServerStopped {
			grpcLogger.Error(err, "grpc server exits not execpted!")
			return
		}
		grpcLogger.V(1).Info("grcp has been exist!")
	}()
	return llmrouterxdsServer, nil
}
func unaryInterceptorSlog(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	grpcLogger.Info("grpc call", "unary request:", info.FullMethod)
	return handler(ctx, req)
}
func streamLogInterceptorSlog(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	grpcLogger.Info("grpc stream call", "stream request:", info.FullMethod)
	return handler(srv, ss)
}
