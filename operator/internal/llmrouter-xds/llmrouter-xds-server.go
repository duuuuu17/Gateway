package llmrouterxds

import (
	context "context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type nodeIDCtxKey struct{}

type LLMRouterXDSServer struct {
	UnimplementedAggregatedDiscoveryServiceServer
	Logger       logr.Logger
	Events       Debouncer               // 资源事件变动通知
	clients      map[string]*ClientState // map[NodeID]*ClientState
	rwMutex      sync.RWMutex
	xdsStore     XDSStore // 利用接口访问，Operator的本地缓存对象
	versionCache CacheStreamAggregateResponses
}

type ClientState struct {
	ClientID       string
	Stream         AggregatedDiscoveryService_StreamAggregatedResourcesServer
	Nonce          map[string]*ClientTypeState // [XDSType]->Map ClientXDSState
	SendCh         chan *DiscoveryResponse
	CloseCh        chan struct{}
	LastActiveTime time.Time
	Mu             sync.Mutex
}

// ############################
// ##  PerClient XDS状态同步  ##
// ############################
type ClientTypeState struct {
	LastSentNonce  string // Server 最后一次发给该 Client 的 Nonce
	LastAckNonce   string // Client 最后一次确认的 Nonce
	LastAckVersion string

	InFlight    bool
	PendingPush *XDSPushEvent

	NackCount  int
	LastNackAt time.Time
}
type XDSType string

const (
	EDSType XDSType = "type.googleapis.com.llmrouter.xds.v1alpha1.LLMRouterEndpointAssignment"
	CDSType XDSType = "type.googleapis.com.llmrouter.xds.v1alpha1.LLMRouterCluster"
	RDSType XDSType = "type.googleapis.com.llmrouter.xds.v1alpha1.LLMRouterRouting"
	TDSType XDSType = "type.googleapis.com.llmrouter.xds.v1alpha1.LLMRouterTenantPipelineConfig"
)

func NewLLMRouterXDSServer(logger logr.Logger, xdsStore XDSStore, pushCh Debouncer, respVersionCache CacheStreamAggregateResponses) *LLMRouterXDSServer {
	return &LLMRouterXDSServer{

		Logger:       logger,
		clients:      make(map[string]*ClientState),
		Events:       pushCh,
		xdsStore:     xdsStore,
		versionCache: respVersionCache,
	}
}

// 退避，当Client NACK时，此时任何的推送只需要在backoff时间段内执行状态Merge
func (s *ClientTypeState) ShouldBackoff() bool {
	if s.NackCount == 0 {
		return false
	}
	backoff := time.Duration(1<<min(s.NackCount, 5)) * time.Second
	return time.Since(s.LastNackAt) < backoff
}
func (s *LLMRouterXDSServer) resendLastAckedVersion(
	client *ClientState,
	typeURL string,
) {
	// 获取到历史成功的ACK版本
	resp := s.versionCache.Get(typeURL, client.Nonce[typeURL].LastAckVersion)
	if resp == nil {
		return
	}
	client.SendCh <- resp
}

// Note: 此方式类似subscribe-push 方式
// 且为长连接
func (s *LLMRouterXDSServer) StreamAggregatedResources(stream AggregatedDiscoveryService_StreamAggregatedResourcesServer) error {
	// 客户端第一次调用时，进行的注册
	req, err := stream.Recv()
	stop, err := s.handleStreamError(err)
	if stop {
		s.cleanupClientResourceCache(stream.Context())
		return nil
	}
	if err != nil {
		s.Logger.Error(err, "Receive stream message failure")
		return err
	}

	// todo: 额外验证实现NodeID
	// 校验NodeID（必须非空）
	if req.NodeId == "" {
		s.Logger.Error(nil, "empty NodeId in first request")
		return status.Errorf(codes.InvalidArgument, "nodeId is required")
	}
	// 表示注册的请求，数据面初始化执行
	ctxWithNodeID := context.WithValue(stream.Context(), nodeIDCtxKey{}, req.NodeId)
	_, cancel := context.WithCancel(ctxWithNodeID)
	// if req.TypeUrl == string(CDSType) && req.ResponseNonce == "" && req.VersionInfo == "" {
	newClient := &ClientState{
		Mu:             sync.Mutex{},
		ClientID:       req.NodeId,
		Stream:         stream,
		Nonce:          make(map[string]*ClientTypeState),
		SendCh:         make(chan *DiscoveryResponse, 100),
		CloseCh:        make(chan struct{}),
		LastActiveTime: time.Now(),
	}
	s.AddClient(req.NodeId, newClient)

	s.PushResourcesSotW()
	// 子协程接收Server发送的推送事件，减少Server阻塞，分散发送压力给实际每个客户端对象
	// 任务处理的子协程退出有专用的关闭信号通道
	go func() {
		// 注意defer执行方式为声明的逆序(出栈 )
		defer cancel()
		defer close(newClient.SendCh)
		for {
			select {
			case <-newClient.CloseCh:
				s.Logger.V(1).Info("client send goroutine exit", "nodeId", req.NodeId)
				return
			case resp, ok := <-newClient.SendCh:
				if !ok {
					s.Logger.Info("failure send, the send's channel was closed!")
					// SendCh 已关闭，退出协程
					return
				}
				s.Logger.Info("send response", "response", resp)
				newClient.Mu.Lock()
				err := stream.Send(resp)
				newClient.Mu.Unlock()
				if err != nil {
					// 发送失败，也应该关闭客户端连接以清理资源
					s.Logger.Error(err, "failed to send to client", "nodeId", req.NodeId)
					return
				}
			}
		}
	}()
	// 主要处理Client 资源更新的ACK，更新本地
	// 当server发现长连接出现错误：client关闭或网络问题，执行连接关闭和连接缓存有状态数据清除操作
	defer s.cleanupClientResourceCache(ctxWithNodeID)
	for {
		select {
		case <-ctxWithNodeID.Done():
			s.Logger.V(1).Info("grpc server exiting")
			close(newClient.CloseCh)
			return nil
		default:
		}
		if err := ctxWithNodeID.Err(); err != nil {
			return nil
		}
		req, err := stream.Recv() // 实际是不许要它主动请求查询的
		stop, err := s.handleStreamError(err)
		if stop {
			close(newClient.CloseCh)
			return nil
		}
		if err != nil {
			s.Logger.Error(err, "Receive stream message failure")
			close(newClient.CloseCh)
			return err
		}
		if req.NodeId == "" {
			s.Logger.Info("empty NodeId in client request")
			continue
		}
		if req.TypeUrl == "" {
			s.Logger.Info("empty TypeUrl in client request", "nodeId", req.NodeId)
			continue
		}
		// 校验NodeID一致性（防止客户端篡改）
		if req.NodeId != ctxWithNodeID.Value(nodeIDCtxKey{}).(string) {
			s.Logger.Info("nodeId mismatch in request", "expected", ctxWithNodeID.Value(nodeIDCtxKey{}), "got", req.NodeId)
			continue
		}
		client, ok := s.GetClient(req.NodeId)
		if !ok {
			s.Logger.Info("It's Not data-plane node!")
			continue
		}
		client.Mu.Lock()
		typeState, ok := client.Nonce[req.TypeUrl]
		if !ok {
			// 如果是 ACK/NACK 消息，但服务器没有为该类型维护状态，可能是客户端发错了或状态已过期
			s.Logger.Info("No state found for TypeURL, skipping ACK/NACK", "nodeId", req.NodeId, "typeUrl", req.TypeUrl)
			client.Mu.Unlock()
			continue
		}
		// NACK
		if req.ErrorDetail != "" {
			typeState.InFlight = false
			// 增加NACK计数
			typeState.NackCount++
			typeState.LastNackAt = time.Now()
			client.LastActiveTime = time.Now()
			// 工业界：记录日志 + metric
			s.Logger.V(0).Info(req.ErrorDetail,
				"node_id", req.NodeId,
				"type", req.TypeUrl,
				"nack_count", typeState.NackCount,
			)
			if typeState.LastAckVersion != "" {
				go s.resendLastAckedVersion(client, req.TypeUrl)
			}
			client.Mu.Unlock()
			continue
		}
		// 如果Client回复的nonce不是刚才Server推送的，说明Client的进度还没有跟上
		if typeState.LastSentNonce != req.ResponseNonce {
			client.Mu.Unlock()
			continue
		}

		// 确定时可接受ACK，更新Client的Nonce/Version 防止重放
		typeState.InFlight = false
		typeState.LastAckNonce = req.ResponseNonce
		typeState.LastAckVersion = req.VersionInfo
		typeState.NackCount = 0
		// metrics: client_synced = 1

		client.LastActiveTime = time.Now()
		s.Logger.V(1).Info("success update client state after ACK",
			"nodeId", req.NodeId,
			"typeUrl", req.TypeUrl,
			"ACKNonce", req.ResponseNonce,
			"ACKVersion", req.VersionInfo) // ACK处理完毕后，发现有待推送的状态，直接同步推送状态

		if typeState.PendingPush != nil {
			pending := *typeState.PendingPush
			typeState.PendingPush = nil
			client.Mu.Unlock()
			go s.PushDeltaResources(pending)
			continue
		}
		client.Mu.Unlock()
	}
}

// 在main.go中调用, 持续监听并推送
func (s *LLMRouterXDSServer) LoopHandlePushEventDispatchBus(ctx context.Context) {
	for {
		select {
		// A: 收到新事件
		case event := <-s.Events.Events():
			s.Logger.Info("get new events", "event_type", event.Type)
			if err := s.xdsStore.RefreshSnapshot(); err != nil {
				s.Logger.Error(err, "grpc server call xds-controller update snapshot failure")
				continue // 跳过本次的更新处理
			}
			// s.Logger.Info("snapshot refresh", "event", event)
			s.PushDeltaResources(event)

		case <-ctx.Done():
			// todo: 当operator关闭时，需要主动通知所有保存的grpc client
			for _, node := range s.clients {
				close(node.CloseCh)
			}
			return
		}
	}
}

// 分发推送函数
func (s *LLMRouterXDSServer) dispatchPushEvent(
	client *ClientState,
	event XDSPushEvent,
	buildResp func(XDSPushEvent) (*DiscoveryResponse, error),
) {
	// start race area
	client.Mu.Lock()
	typeState, ok := client.Nonce[string(event.Type)]
	if !ok {
		typeState = &ClientTypeState{}
		client.Nonce[string(event.Type)] = typeState
	}
	// 如果正在同步中或NACK情况下,则将新推送的状态进行同步
	if typeState.InFlight || typeState.ShouldBackoff() {
		if typeState.PendingPush == nil { // 若不存在等待推送的状态，则新推送的状态就是
			cp := event // copy
			typeState.PendingPush = &cp
		} else { // 存在等待的同步的状态，与新状态进行合并
			MergeXDSPushEvent(typeState.PendingPush, event)
		}
		client.Mu.Unlock()
		return
	}
	// 没有 InFlight ，则需要将该推送状态进行同步
	resp, err := buildResp(event)
	if err != nil {
		s.Logger.Error(err, "Failed to build response for push event", "client", client.ClientID, "type", event.Type)
		client.Mu.Unlock()
		return
	}
	newNonce := uuid.NewString()
	newVersion := fmt.Sprintf("ver-%d", time.Now().UnixNano())
	// 把response添加到缓存中
	s.versionCache.Add(string(event.Type), VersionedResponse{
		Version: newVersion,
		Resp:    resp,
	})
	// 更新状态：标记为 InFlight，清空 PendingPush，设置新 Nonce
	client.Nonce[string(event.Type)].LastSentNonce = newNonce
	resp.Nonce = newNonce
	resp.VersionInfo = newVersion
	client.Nonce[string(event.Type)].InFlight = true
	client.Nonce[string(event.Type)].PendingPush = nil // 推送前清空待处理事件
	client.Mu.Unlock()
	// end race area
	select {
	case client.SendCh <- resp:
		s.Logger.V(2).Info("Pushed resource to client", "client", client.ClientID, "type", event.Type, "version", newVersion)
	default:
		s.Logger.Info("Client buffer full, dropping update", "client", client.ClientID)
	}
}
func (s *LLMRouterXDSServer) buildDeltaDiscoveryResponse(event XDSPushEvent) (*DiscoveryResponse, error) {
	if event.Type == "" {
		s.Logger.Error(nil, "empty XDSType in push event")
		return nil, fmt.Errorf("empty XDSType")
	}
	resp := &DiscoveryResponse{
		// Resources:       make([]*anypb.Any, 0),
		TypeUrl:         string(event.Type),
		RemoveResources: make([]string, 0, len(event.RemoveDService)),
	}
	resources := make([]proto.Message, 0, len(event.AffectedServices))
	var err error
	switch event.Type {
	case EDSType:
		eds, removeService := s.xdsStore.GetEDS(event.AffectedServices...)
		// s.Logger.Info("EDSsnapshot_needRemoveServices", "removeServiceServicename", removeService, "EventRemoveService", event.RemoveDService, "addEvents", event.AffectedServices)
		resp.RemoveResources = append(resp.RemoveResources, removeService...)
		resources = EDSsTransformerToProtoMessages(eds)
	case CDSType:
		cds, removeService := s.xdsStore.GetCDS(event.AffectedServices...)
		// s.Logger.Info("CDSsnapshot_needRemoveServices", "removeServiceServicename", removeService, "EventRemoveService", event.RemoveDService, "addEvents", event.AffectedServices)
		// s.Logger.Info("snapshot-save", "CDS", s.xdsStore.GetCDSAll()[0].GetName())
		resp.RemoveResources = append(resp.RemoveResources, removeService...)
		resources = CDSsTransformerToProtoMessages(cds)
	case RDSType:
		rds, removeService := s.xdsStore.GetRDS(event.AffectedServices...)
		// s.Logger.Info("RDSsnapshot_needRemoveServices", "removeServiceServicename", removeService, "EventRemoveService", event.RemoveDService, "addEvents", event.AffectedServices)
		resp.RemoveResources = append(resp.RemoveResources, removeService...)
		resources = RDSsTransformerToProtoMessages(rds)
	case TDSType: // StoW
		// tds, removeService := s.xdsStore.GetTDS(event.AffectedServices...)
		// resp.RemoveResources = append(resp.RemoveResources, removeService...)
		resources = TDSsTransformerToProtoMessages(s.xdsStore.GetTDSAll())
	default:
		s.Logger.Error(nil, "unsupported XDSType", "type", event.Type)
		return nil, fmt.Errorf("unsupported XDSType")
	}
	if resources == nil {
		s.Logger.V(1).Info("no resource for service", "type", event.Type)
		return resp, nil
	}

	subResource, err := transformerToAnyResource(resources...)
	if err != nil {
		return nil, fmt.Errorf("xDS convert to anypb failed")
	}
	resp.Resources = subResource
	resp.RemoveResources = append(resp.RemoveResources, event.RemoveDService...)
	return resp, nil
}

// 实现了单Inflight 机制和pendingPush
func (s *LLMRouterXDSServer) PushDeltaResources(event XDSPushEvent) error {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	// 在推送的同时，更新本次协调处理后的XDSController快照
	for _, client := range s.clients {
		s.dispatchPushEvent(client, event, s.buildDeltaDiscoveryResponse)
	}
	return nil
}
func (s *LLMRouterXDSServer) buildSToWDiscoveryResponse(event XDSPushEvent) (*DiscoveryResponse, error) {
	var resources []proto.Message
	var err error
	switch event.Type {
	case EDSType:
		resources = EDSsTransformerToProtoMessages(s.xdsStore.GetEDSAll())
	case CDSType:
		resources = CDSsTransformerToProtoMessages(s.xdsStore.GetCDSAll())
	case RDSType:
		resources = RDSsTransformerToProtoMessages(s.xdsStore.GetRDSAll())
	case TDSType:
		resources = TDSsTransformerToProtoMessages(s.xdsStore.GetTDSAll())
	default:
		s.Logger.Error(nil, "unsupported XDSType", "type", event.Type)
		return nil, fmt.Errorf("unsupported XDSType")
	}
	if len(resources) == 0 {
		s.Logger.V(1).Info("no resource for service need push", "type", event.Type)
		resources = make([]proto.Message, 0)
	}

	respResources, err := transformerToAnyResource(resources...)
	if err != nil {
		return nil, fmt.Errorf("xDS convert to anypb failed")
	}
	resp := &DiscoveryResponse{
		TypeUrl:   string(event.Type),
		Resources: respResources,
	}
	return resp, nil
}

func (s *LLMRouterXDSServer) PushResourcesSotW() error {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	for _, xdsType := range []XDSType{EDSType, CDSType, RDSType, TDSType} {
		event := XDSPushEvent{
			Type:             xdsType,
			AffectedServices: nil,
			Full:             true,
			Reason:           "ClientInit",
		}
		for _, client := range s.clients {
			s.dispatchPushEvent(client, event, s.buildSToWDiscoveryResponse)
		}
	}
	return nil
}

// 发送协程 (Send Worker) [deprecate]
func (c *ClientState) sendLoop() {
	for {
		select {
		case resp := <-c.SendCh:
			c.Stream.Send(resp)
		case <-c.CloseCh:
			return
		}
	}
}
func transformerToAnyResource(resources ...proto.Message) ([]*anypb.Any, error) {
	anyResources := make([]*anypb.Any, 0, len(resources))
	for _, resource := range resources {
		subResource, err := anypb.New(resource)
		if err != nil {
			return nil, err
		}
		anyResources = append(anyResources, subResource)
	}
	return anyResources, nil
}
func (s *LLMRouterXDSServer) cleanupClientResourceCache(ctx context.Context) {
	nodeID, ok := ctx.Value(nodeIDCtxKey{}).(string)
	if !ok || nodeID == "" {
		s.Logger.V(1).Info("no nodeID found in context, skip clean up")
		return
	}
	s.RemoveClient(nodeID)
	s.Logger.V(1).Info("nodeID Client remove", "nodeId", nodeID)

}

// AddClient 注册客户端 State（连接建立时调用）
func (s *LLMRouterXDSServer) AddClient(nodeID string, state *ClientState) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	state.LastActiveTime = time.Now()
	s.clients[nodeID] = state
	s.Logger.V(1).Info("client registered", "nodeID", nodeID)
}

// RemoveClient 移除客户端 State（连接断开时调用）
func (s *LLMRouterXDSServer) RemoveClient(nodeID string) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	delete(s.clients, nodeID)
	s.Logger.V(1).Info("client unregistered", "nodeID", nodeID)
}

// GetClient 获取单个客户端 State（按需查找时调用）
func (s *LLMRouterXDSServer) GetClient(nodeID string) (*ClientState, bool) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	state, ok := s.clients[nodeID]
	return state, ok
}

// 在main.go中调用, 持续监听并推送
// func (s *LLMRouterXDSServer) LoopHandlePushEventDispatchBusWithTimer(ctx context.Context) {
// 	var (
// 		timer          *time.Timer
// 		firstEventTime time.Time
// 	)
// 	dirtySet := NewDirtySet()
// 	for {
// 		select {
// 		// A: 收到新事件
// 		case event := <-s.Events.Events():
// 			for _, svc := range event.AffectedServices {
// 				dirtySet.Mark(event.Type, svc)
// 			}
// 			// 如果该推送事件是第一次, 并启动最小计时器
// 			if firstEventTime.IsZero() {
// 				firstEventTime = time.Now()
// 				timer = time.NewTimer(DebounceMin)
// 			} else {
// 				// 在计时器未触发范围内。非第一次推送事件
// 				// 如果当前推送时间，计时器触发，超过了最大等待时间
// 				if time.Since(firstEventTime) >= DebounceMax {
// 					if timer != nil {
// 						timer.Stop()
// 					}
// 					s.PushResourcesSotW(dirtySet)
// 					dirtySet = NewDirtySet()
// 					firstEventTime = time.Time{}
// 				} else {
// 					// 没有超过最大的等待时间，那么需要重置计时器，来实现防抖
// 					if timer != nil {
// 						timer.Stop()
// 					}
// 					timer.Reset(DebounceMin)
// 				}
// 			}
// 			// B: 计时器触发
// 		case <-func() <-chan time.Time {
// 			if timer == nil {
// 				return nil
// 			}
// 			return timer.C
// 		}():
// 			s.PushResourcesSotW(dirtySet)
// 			timer = nil
// 			dirtySet = NewDirtySet()
// 			firstEventTime = time.Time{}
// 		case <-ctx.Done():
// 			// todo: 当operator关闭时，需要主动通知所有保存的grpc client
// 			// for _, node := range s.clients {
// 			// 	close(node.CloseCh)
// 			// 	close(node.SendCh)
// 			// }
// 			return
// 		}
// 	}
// }

func (s *LLMRouterXDSServer) handleStreamError(err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if err == io.EOF {
		s.Logger.Info("client close stream connection")
		return true, nil
	}
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		s.Logger.V(1).Info("client stream canceled")
		return true, nil
	}
	if status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded {
		s.Logger.Error(err, "client stream unavailable/deadline exceeded")
		return true, nil
	}
	// 非致命错误（如网络抖动）：返回 stop=false，错误由上层处理
	if status.Code(err) == codes.Unknown || status.Code(err) == codes.Internal {
		s.Logger.Error(err, "non-fatal stream error")
		return false, err
	}
	return true, status.Errorf(codes.Internal, "stream recv failed: %v", err)
}

// heartbeat: 返回空消息
