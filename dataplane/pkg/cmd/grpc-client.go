package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	llmrouterxds "github.com/duuuuu17/llm-router-operator/pkg/config/llmrouter-xds"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ConfigSource int

const (
	SourceMinimal ConfigSource = iota
	SourceFile
	SourceRemote
)

type XDSType string

const (
	EDSType XDSType = "type.googleapis.com.llmrouter.xds.v1alpha1.LLMRouterEndpointAssignment"
	CDSType XDSType = "type.googleapis.com.llmrouter.xds.v1alpha1.LLMRouterCluster"
	RDSType XDSType = "type.googleapis.com.llmrouter.xds.v1alpha1.LLMRouterRouting"
	TDSType XDSType = "type.googleapis.com.llmrouter.xds.v1alpha1.LLMRouterTenantPipelineConfig"
)
const (
	MaxRetry int = 30
)
const (
	MaxTimeout time.Duration = 60 * time.Second
)

// 每个 xDS 类型的客户端状态（按 TypeUrl 粒度）
type XDSClientTypeState struct {
	LastAckVersion  string       //  最新确认的版本
	currentSource   ConfigSource // 表示当前XDS的配置来源
	currentState    atomic.Int32 // 表示当前XDS的配置状态
	retry           int          // 当前重试次数
	LastSuccessTime time.Time    // 最新确认配置的时间
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

	// 传统时直接在Config模块中更新Router处理时所需的配置信息
	Router *config.RouterConfig
	// 事件推送方式：config模块中TenantConfig多租户信息更新时推送更新事件，由Router模块HandleLoop监听并获取主动处理
	TenantCfg        *config.TenantCfg
	SelectorRegistry *config.SelectorRegistry
	// 重试次数
	retries int
}
type StreamClientDependencies struct {
	Endpoint         string
	RouterCfg        *config.RouterConfig
	SelectorRegistry *config.SelectorRegistry
	TenantCfg        *config.TenantCfg
}

func NewStreamClient(ctx context.Context, dep *StreamClientDependencies) *StreamClient {

	sc := &StreamClient{
		mu:               sync.Mutex{},
		NodeID:           generateNodeID(),
		Types:            make(map[string]*XDSClientTypeState),
		SelectorRegistry: dep.SelectorRegistry,
		Router:           dep.RouterCfg,
		TenantCfg:        dep.TenantCfg,
		retries:          0,
	}
	go sc.Run(ctx, dep.Endpoint)
	return sc

}
func (sc *StreamClient) Run(ctx context.Context, endpoint string) {

	for {
		select {
		case <-ctx.Done():
			// 正常退出
			// nolint:errcheck
			if err := sc.Conn.Close(); err != nil {
				slog.Warn("grpc client exiting error", "Stream Client NodeID:", sc.NodeID)
				return
			}
			slog.Info("grpc client exiting now", "Stream Client NodeID:", sc.NodeID)
			return
		default:
		}
		// 1. 设置带timeout的Ctx尝试建立gprc连接
		if err := sc.establishStream(ctx, endpoint); err != nil {
			sc.handleColdStartFallback(ctx)
			continue
		}

		// 2. 连接成功，发送接收Stow全量推送请求
		slog.Info("grpc Client send message", "NodeID", sc.NodeID, "TypeUrl", string(CDSType))
		if err := sc.Stream.Send(&llmrouterxds.DiscoveryRequest{
			TypeUrl:       string(CDSType),
			NodeId:        sc.NodeID,
			ResourceNames: nil,
		}); err != nil {
			sc.handleColdStartFallback(ctx)
			continue
		}

		// 3. 进入正常的循环处理
		sc.HandleLoop(ctx)
		// 4. 一旦退出循环处理，表示遇到了Recv错误，连接断开
		sc.handleColdStartFallback(ctx) // 执行降级保活和退避重试处理
		// 如果 HandleLoop 退出 (说明断连了)，循环会继续，重新尝试建立连接
		slog.Warn("Stream disconnected, attempting to reconnect...")
	}

}

// 对于服务关闭和网络不可达的情况下，数据面都应执行降级保活并退避重试
func (sc *StreamClient) establishStream(ctx context.Context, endpoint string) error {

	// grpc.NewClient 默认非阻塞，这里直接创建即可
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		// 这里的 err 一般是参数错误，极少是网络错误
		return err
	}

	client := llmrouterxds.NewAggregatedDiscoveryServiceClient(conn)

	// 这一步才会真正发起 TCP 握手，如果控制面没起，会在这里超时或报错
	stream, err := client.StreamAggregatedResources(ctx)
	if err != nil {
		// 记得关闭底层连接，防止泄漏
		err = conn.Close()
		slog.Error("连接服务端失败", "[error]: ", err)
		return err
	}
	// save some the grpc-client infos into StreamClient
	sc.mu.Lock()
	sc.ADSClient = client
	sc.Conn = conn
	sc.Stream = stream
	sc.mu.Unlock()
	return nil
}
func (sc *StreamClient) HandleLoop(ctx context.Context) {
	defer sc.Conn.Close() //nolint:errcheck

	for {
		select {
		case <-ctx.Done():
			// 正常退出
			slog.Info("grpc client exiting now", "Stream Client NodeID:", sc.NodeID)
			return
		default:
			resp, err := sc.Stream.Recv()
			if err != nil {
				// Note: 当grpc接收报错，说明该连接已经关闭，应当退出该循环处理重新建立连接
				slog.Warn("grpc client send failure", "error:", err.Error())
				return
			}
			sc.retries = 0 // 一旦能够正常Recv消息，就应该将退避重置为0
			if err := sc.handleDiscoveryResponse(resp); err != nil {
				// 更新发生错误
				sc.sendNACK(resp, err)
				continue
			} else {
				sc.sendACK(resp)
			}
			//  正常返回
			slog.Info("grpc client got control plane data", "resp", resp)
		}
	}
}
func (sc *StreamClient) handleColdStartFallback(_ context.Context) {
	if sc.hasConfigInMemory() {
		slog.Warn("Lost connection, keeping current in-meneory config.")
		backoff(sc.retries)
		sc.retries++
		return
	}
	// 如果内存都没有配置(冷启动)
	// 进入全局Reject模式
	slog.Info("Cold start: No config available! Entering Reject-All mode.")
	backoff(sc.retries)
	sc.retries++
}
func backoff(retries int) {
	waitTime := time.Duration(1<<retries) * 5 * time.Second
	if waitTime > MaxTimeout {
		waitTime = MaxTimeout
	}
	slog.Info("Backing off before reconnect", "attmpt", retries, "duration", waitTime.Seconds())
	time.Sleep(waitTime)
}
func (sc *StreamClient) hasConfigInMemory() bool {
	RuntimeBackends := sc.Router.GetConfig()
	return len(RuntimeBackends) != 0
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
	case string(TDSType):
		return sc.applyTDS(resp)
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
		// var err error
		// endpoint级的选择器
		// rb.EndpointSelector, err = sc.SelectorRegistry.New(cds.LbStrategy)
		// if err != nil {
		// 	slog.Warn("got endpoint selector failure", "[error]", err.Error())
		// 	return nil
		// }
		// 使用 RouterConfig 提供的原子更新方法
		sc.Router.UpdateRuntimeBackendOfServiceName(name, rb)
	}
	// 处理 remove_resources：把那些服务从 Backends 中删除（或标记）
	if len(resp.RemoveResources) > 0 {
		sc.Router.DeleteRuntimeBackendOfServiceName(resp.RemoveResources)
	}
	// 更新 LastAckVersion
	st := sc.getOrCreateTypeState(resp.TypeUrl)
	st.LastAckVersion = resp.VersionInfo
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
	st := sc.getOrCreateTypeState(resp.TypeUrl)
	st.LastAckVersion = resp.VersionInfo
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
			Weight:      rds.Weight,
			Priority:    rds.Priority,
			Region:      rds.Region,
			Selector:    rds.Selector,
			CanaryRatio: rds.CanaryRatio,
		}
		endpointSelector, err := sc.SelectorRegistry.New(rb.Routing.Selector)
		if err != nil {
			slog.Info("can't get the endpoint selector")
			return err
		}
		rb.EndpointSelector = endpointSelector // 不用加锁，因为只有RDS会处理EDS的这个字段
	}
	// 处理 remove_resources：把那些服务从 Backends 中删除（或标记）
	if len(resp.RemoveResources) > 0 {
		sc.Router.DeleteBackendRoutingOfServiceName(resp.RemoveResources)

	}
	// 更新 LastAckVersion
	st := sc.getOrCreateTypeState(resp.TypeUrl)
	st.LastAckVersion = resp.VersionInfo
	return nil
}

func (sc *StreamClient) applyTDS(resp *llmrouterxds.DiscoveryResponse) error {
	// 处理更新
	needUpdates := make(map[string]*config.TenantConfig, 0)
	for _, anyRes := range resp.Resources {
		var tds llmrouterxds.LLMRouterTenantPipelineConfigs
		if err := anyRes.UnmarshalTo(&tds); err != nil {
			return err
		}
		for _, tenanat := range tds.Tenants {
			needUpdates[tenanat.TenantId] = sc.TenantCfg.ConvertToDTO(tenanat)
		}
	}
	// 统一更新
	err := sc.TenantCfg.BatchUpdate(needUpdates, resp.RemoveResources)
	if err != nil {
		return err
	}
	// 更新 LastAckVersion
	st := sc.getOrCreateTypeState(resp.TypeUrl)
	st.LastAckVersion = resp.VersionInfo
	// 通知更新
	sc.TenantCfg.Bus.Publish("tenant_config.reloaded", sc.TenantCfg.GlobalConfig.Load())

	return nil
}

func (sc *StreamClient) sendACK(resp *llmrouterxds.DiscoveryResponse) {
	// 更新Client本地对typeUrl类型资源更新的缓存版本号
	st := sc.getOrCreateTypeState(resp.TypeUrl)
	st.LastAckVersion = resp.VersionInfo
	st.retry = 0
	st.LastSuccessTime = time.Now()
	st.currentState.Store(int32(StateRemoteHealthy))
	st.currentSource = SourceRemote
	slog.Info("success update client state after ACK",
		"nodeId", sc.NodeID,
		"typeUrl", resp.TypeUrl,
		"ACKNonce", resp.Nonce,
		"ACKVersion", resp.VersionInfo)
	if err := sc.Stream.Send(&llmrouterxds.DiscoveryRequest{
		TypeUrl:       resp.TypeUrl,
		VersionInfo:   resp.VersionInfo,
		ResponseNonce: resp.Nonce,
		NodeId:        sc.NodeID,
		// ResourceNames: resp.RemoveResources,
		ErrorDetail: "",
	}); err != nil {
		slog.Error("controller error can't send ack message")
	}
}
func (sc *StreamClient) sendNACK(resp *llmrouterxds.DiscoveryResponse, err error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	// 获取Client本地对typeUrl类型资源更新的缓存版本号
	st := sc.getOrCreateTypeState(resp.TypeUrl)
	slog.Info("failed update client state after NACK",
		"nodeId", sc.NodeID,
		"typeUrl", resp.TypeUrl,
		"ACKNonce", resp.Nonce,
		"ACKVersion", resp.VersionInfo)
	if err := sc.Stream.Send(&llmrouterxds.DiscoveryRequest{
		ErrorDetail: err.Error(),
		NodeId:      sc.NodeID,
		VersionInfo: st.LastAckVersion,
		TypeUrl:     resp.TypeUrl,
	}); err != nil {
		slog.Error("controller error can't send nack message")
	}
}
func (sc *StreamClient) getOrCreateTypeState(typ string) *XDSClientTypeState {
	// sc.mu.Lock()
	// defer sc.mu.Unlock()
	st, exists := sc.Types[typ]
	if !exists {
		st = &XDSClientTypeState{LastAckVersion: ""}
		sc.Types[typ] = st
	}
	return st
}
