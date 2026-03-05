package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	llmrouterxds "github.com/duuuuu17/llm-router-operator/pkg/config/llmrouter-xds"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type XDSType string

const (
	EDSType XDSType = " type.googleapis.com.llmrouter.xds.v1alpha1.LLMRouterEndpointAssignment"
	CDSType XDSType = " type.googleapis.com.llmrouter.xds.v1alpha1.LLMRouterCluster"
	RDSType XDSType = " type.googleapis.com.llmrouter.xds.v1alpha1.LLMRouterRouting"
	TDSType XDSType = " type.googleapis.com.llmrouter.xds.v1alpha1.LLMRouterTenantPipelineConfig"
)

// 每个 xDS 类型的客户端状态（按 TypeUrl 粒度）
type XDSClientTypeState struct {
	LastAckVersion string
}

// 聚合的 gRPC XDS 客户端（数据面）
type StreamClient struct {
	NodeID string

	Conn      *grpc.ClientConn                                                        // 连接
	ADSClient llmrouterxds.AggregatedDiscoveryServiceClient                           // client
	Stream    llmrouterxds.AggregatedDiscoveryService_StreamAggregatedResourcesClient // stream

	mu    sync.Mutex
	Types map[string]*XDSClientTypeState // key: type_url，例如 string(EDSType)

	// 指向 Router 运行时配置信息（控制面推送 → RuntimeBackend）
	Router           *config.RouterConfig
	TenantCfg        *config.TenantCfg
	SelectorRegistry *config.SelectorRegistry
}
type StreamClientDependencies struct {
	Endpoint         string
	RouterCfg        *config.RouterConfig
	SelectorRegistry *config.SelectorRegistry
	TenantCfg        *config.TenantCfg
}

func NewStreamClient(ctx context.Context, dep *StreamClientDependencies) *StreamClient {

	sc := &StreamClient{}
	sc.mu.Lock()
	defer sc.mu.Unlock()

	conn, err := grpc.NewClient(dep.Endpoint, grpc.WithTransportCredentials(
		insecure.NewCredentials(),
	))
	if err != nil {
		slog.Error(fmt.Sprintf("连接服务端失败: %v\n", err))
	}
	client := llmrouterxds.NewAggregatedDiscoveryServiceClient(conn)
	sc.Conn = conn
	sc.ADSClient = client
	stream, err := client.StreamAggregatedResources(ctx) // 获取双向流函数的stream对象
	sc.Stream = stream
	sc.Types = make(map[string]*XDSClientTypeState)
	sc.SelectorRegistry = dep.SelectorRegistry
	sc.Router = dep.RouterCfg
	sc.NodeID = uuid.NewString() + time.Now().String()
	sc.TenantCfg = dep.TenantCfg
	go sc.HandleLoop(ctx)
	return sc
}
func (sc *StreamClient) HandleLoop(ctx context.Context) {

	// 初始化时第一次发出接收Stow全量配置数据
	sc.Stream.Send(&llmrouterxds.DiscoveryRequest{
		TypeUrl:       string(CDSType),
		NodeId:        sc.NodeID,
		ResourceNames: nil,
	})

	for {
		select {
		case <-ctx.Done():
			return
		default:
			resp, err := sc.Stream.Recv()
			if err != nil {
				slog.Error(fmt.Sprintf("grpc client send failure,error:%w", err.Error()))
				return
			}

			// 更新发生错误
			if err := sc.handleDiscoveryResponse(resp); err != nil {
				sc.sendNACK(resp, err)
				continue
			} else {
				sc.sendACK(resp)
			}
			//  正常返回
			slog.Info("grpc client got control plane data", "resources", resp.Resources)
		}
	}
}
func (sc *StreamClient) handleDiscoveryResponse(resp *llmrouterxds.DiscoveryResponse) error {
	typeURL := resp.TypeUrl
	switch typeURL {
	case string(CDSType):
		return sc.applyCDS(resp)
	case string(EDSType):
		return sc.applyEDS(resp)
	case string(RDSType):
		return sc.applyRDS(resp)
	default:
		return fmt.Errorf("unsupported type_url: %s", typeURL)
	}
}
func (sc *StreamClient) applyCDS(resp *llmrouterxds.DiscoveryResponse) error {
	backends := sc.Router.GetConfig()
	for _, anyRes := range resp.Resources {
		var cds llmrouterxds.LLMRouterCluster
		if err := anyRes.UnmarshalTo(&cds); err != nil {
			return err
		}
		name := cds.Name
		// 构造或更新 RuntimeBackend.Capability
		rb, ok := backends[name]
		if !ok {
			rb = &config.RuntimeBackend{Name: name}
		}
		rb.Capabilty = config.BackendCapability{
			Models:           append([]string(nil), cds.Models...),
			Protocols:        append([]string(nil), cds.Protocols...),
			EnabledStreaming: cds.Streaming,
		}
		var err error
		// endpoint级的选择器
		rb.EndpointSelector, err = sc.SelectorRegistry.New(cds.LbStrategy)
		if err != nil {
			slog.Error("got endpoint selector failure", "[error]", err.Error())
			return err
		}
		// 使用 RouterConfig 提供的原子更新方法
		sc.Router.UpdateRuntimeBackendOfServiceName(name, rb)
	}
	// 处理 remove_resources：把那些服务从 Backends 中删除（或标记）
	if len(resp.RemoveResources) > 0 {
		sc.Router.DeleteRuntimeBackendOfServiceName(resp.RemoveResources)
	}
	// 更新 LastAckVersion
	sc.mu.Lock()
	st := sc.getOrCreateTypeState(resp.TypeUrl)
	st.LastAckVersion = resp.VersionInfo
	sc.mu.Unlock()
	return nil
}
func (sc *StreamClient) applyEDS(resp *llmrouterxds.DiscoveryResponse) error {
	backends := sc.Router.GetConfig()
	for _, anyRes := range resp.Resources {
		var eds llmrouterxds.LLMRouterEndpointAssignment
		if err := anyRes.UnmarshalTo(&eds); err != nil {
			return err
		}
		name := eds.ClusterName
		rb, ok := backends[name]
		if !ok {
			rb = &config.RuntimeBackend{Name: name}
			sc.Router.UpdateRuntimeBackendOfServiceName(name, rb) // 当发现cluster级数据不存在时创建并更新
		}
		enpoints := []*config.Endpoint{}
		for _, e := range eds.Endpoints {
			enpoints = append(enpoints, config.NewEndpoint(e.Address))
		}
		rb.Capabilty.Endpoints = enpoints // 更新缓存
		sc.Router.UpdateEndpointsOfServiceName(name, enpoints)

	}
	// 处理 remove_resources：把那些服务从 Backends 中删除（或标记）
	if len(resp.RemoveResources) > 0 {
		sc.Router.DeleteEndpointsOfServiceName(resp.RemoveResources)

	}
	// 更新 LastAckVersion
	sc.mu.Lock()
	st := sc.getOrCreateTypeState(resp.TypeUrl)
	st.LastAckVersion = resp.VersionInfo
	sc.mu.Unlock()
	return nil
}
func (sc *StreamClient) applyRDS(resp *llmrouterxds.DiscoveryResponse) error {
	backends := sc.Router.GetConfig()
	for _, anyRes := range resp.Resources {
		var rds llmrouterxds.LLMRouterRouting
		if err := anyRes.UnmarshalTo(&rds); err != nil {
			return err
		}
		name := rds.ClusterName
		rb, ok := backends[name]
		if !ok {
			rb = &config.RuntimeBackend{Name: name}
			sc.Router.UpdateRuntimeBackendOfServiceName(name, rb) // 当发现cluster级数据不存在时创建并更新
		}
		rb.Routing = config.BackendRouting{
			Weight:   rds.Weight,
			Priority: rds.Priority,
			Region:   rds.Region,
		}

	}
	// 处理 remove_resources：把那些服务从 Backends 中删除（或标记）
	if len(resp.RemoveResources) > 0 {
		sc.Router.DeleteBackendRoutingOfServiceName(resp.RemoveResources)

	}
	// 更新 LastAckVersion
	sc.mu.Lock()
	st := sc.getOrCreateTypeState(resp.TypeUrl)
	st.LastAckVersion = resp.VersionInfo
	sc.mu.Unlock()
	return nil
}
func (sc *StreamClient) sendACK(resp *llmrouterxds.DiscoveryResponse) {
	// 更新Client本地对typeUrl类型资源更新的缓存版本号
	st := sc.getOrCreateTypeState(resp.TypeUrl)
	st.LastAckVersion = resp.VersionInfo

	sc.Stream.Send(&llmrouterxds.DiscoveryRequest{
		TypeUrl:       resp.TypeUrl,
		VersionInfo:   resp.VersionInfo,
		ResponseNonce: resp.Nonce,
		NodeId:        sc.NodeID,
		// ResourceNames: resp.RemoveResources,
		ErrorDetail: "",
	})
}
func (sc *StreamClient) sendNACK(resp *llmrouterxds.DiscoveryResponse, err error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// 获取Client本地对typeUrl类型资源更新的缓存版本号
	st := sc.getOrCreateTypeState(resp.TypeUrl)
	sc.Stream.Send(&llmrouterxds.DiscoveryRequest{
		ErrorDetail: err.Error(),
		NodeId:      sc.NodeID,
		VersionInfo: st.LastAckVersion,
		TypeUrl:     resp.TypeUrl,
	})
}
func (sc *StreamClient) getOrCreateTypeState(typ string) *XDSClientTypeState {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	st, exists := sc.Types[typ]
	if !exists {
		st = &XDSClientTypeState{LastAckVersion: ""}
		sc.Types[typ] = st
	}
	return st
}

// todo: Client主动关闭时，需要主动通知?
func (sc *StreamClient) CloseStreamClient() {
	sc.Stream.CloseSend()
}
