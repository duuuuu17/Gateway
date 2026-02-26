package utils

import (
	"testing"
	"time"

	llmrouterxds "github.com/duuuuu17/llm-router-operator/internal/llmrouter-xds"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type fakeXDSStore struct{}

func (f *fakeXDSStore) GetEDS(services ...string) ([]*llmrouterxds.LLMRouterEndpointAssignment, []string) {
	return []*llmrouterxds.LLMRouterEndpointAssignment{{ClusterName: "svc-a"}}, []string{}
}
func (f *fakeXDSStore) GetEDSAll() []*llmrouterxds.LLMRouterEndpointAssignment {
	return []*llmrouterxds.LLMRouterEndpointAssignment{{ClusterName: "svc-a"}, {ClusterName: "svc-b"}}
}

func (f *fakeXDSStore) GetCDS(services ...string) ([]*llmrouterxds.LLMRouterCluster, []string) {
	return []*llmrouterxds.LLMRouterCluster{{Name: "cluster-a"}}, []string{}
}
func (f *fakeXDSStore) GetCDSAll() []*llmrouterxds.LLMRouterCluster {
	return []*llmrouterxds.LLMRouterCluster{{Name: "cluster-a"}}
}

func (f *fakeXDSStore) GetRDS(services ...string) ([]*llmrouterxds.LLMRouterRouting, []string) {
	return []*llmrouterxds.LLMRouterRouting{{ClusterName: "route-a"}}, []string{}
}
func (f *fakeXDSStore) GetRDSAll() []*llmrouterxds.LLMRouterRouting {
	return []*llmrouterxds.LLMRouterRouting{{ClusterName: "route-a"}}
}
func (f *fakeXDSStore) RefreshSnapshot() error {
	return nil
}
func newTestClient(id string) *llmrouterxds.ClientState {
	return &llmrouterxds.ClientState{
		ClientID: id,
		Nonce:    make(map[string]*llmrouterxds.ClientTypeState),
		SendCh:   make(chan *llmrouterxds.DiscoveryResponse, 10),
		CloseCh:  make(chan struct{}),
	}
}
func TestPushDelta_FirstSend(t *testing.T) {
	logger := logr.Discard()
	store := &fakeXDSStore{}
	respVersionCache := llmrouterxds.NewRespVersionCache(15)
	server := llmrouterxds.NewLLMRouterXDSServer(logger, store, nil, respVersionCache)

	client := newTestClient("node-1")
	server.AddClient(uuid.NewString(), client)

	event := llmrouterxds.XDSPushEvent{
		Type:             llmrouterxds.EDSType,
		AffectedServices: []string{"svc-a"},
	}

	err := server.PushDeltaResources(event)
	require.NoError(t, err)

	select {
	case resp := <-client.SendCh:
		require.Equal(t, string(llmrouterxds.EDSType), resp.TypeUrl)
		require.NotEmpty(t, resp.Nonce)
		require.NotEmpty(t, resp.VersionInfo)

		state := client.Nonce[string(llmrouterxds.EDSType)]
		require.True(t, state.InFlight)
		require.Equal(t, resp.Nonce, state.LastSentNonce)

	default:
		t.Fatal("expected delta response to be sent")
	}
}
func TestPushDelta_InFlightMerge(t *testing.T) {
	logger := logr.Discard()
	store := &fakeXDSStore{}
	respVersionCache := llmrouterxds.NewRespVersionCache(15)
	server := llmrouterxds.NewLLMRouterXDSServer(logger, store, nil, respVersionCache)

	client := newTestClient("node-1")
	client.Nonce[string(llmrouterxds.EDSType)] = &llmrouterxds.ClientTypeState{
		InFlight: true,
	}
	server.AddClient(uuid.NewString(), client)

	event := llmrouterxds.XDSPushEvent{
		Type:             llmrouterxds.EDSType,
		AffectedServices: []string{"svc-b"},
	}

	err := server.PushDeltaResources(event)
	require.NoError(t, err)

	select {
	case <-client.SendCh:
		t.Fatal("should not send when inflight")
	default:
	}

	state := client.Nonce[string(llmrouterxds.EDSType)]
	require.NotNil(t, state.PendingPush)
	require.Contains(t, state.PendingPush.AffectedServices, "svc-b")
}
func TestPushSotW(t *testing.T) {
	logger := logr.Discard()
	store := &fakeXDSStore{}
	respVersionCache := llmrouterxds.NewRespVersionCache(15)
	server := llmrouterxds.NewLLMRouterXDSServer(logger, store, nil, respVersionCache)

	client := newTestClient("node-1")
	server.AddClient(uuid.NewString(), client)

	err := server.PushResourcesSotW()
	require.NoError(t, err)

	// SotW 会对 EDS/CDS/RDS 都推
	received := 0

loop:
	for {
		select {
		case resp := <-client.SendCh:
			require.NotEmpty(t, resp.TypeUrl)
			require.NotEmpty(t, resp.Nonce)
			received++
		default:
			break loop
		}
	}

	require.Equal(t, 3, received)

	for _, xdsType := range []llmrouterxds.XDSType{llmrouterxds.EDSType, llmrouterxds.CDSType, llmrouterxds.RDSType} {
		state := client.Nonce[string(xdsType)]
		require.True(t, state.InFlight)
		require.NotEmpty(t, state.LastSentNonce)
	}
}
func newTestServerWithClient() (*llmrouterxds.LLMRouterXDSServer, *llmrouterxds.ClientState) {
	logger := logr.Discard()
	store := &fakeXDSStore{}
	respVersionCache := llmrouterxds.NewRespVersionCache(15)
	server := llmrouterxds.NewLLMRouterXDSServer(logger, store, nil, respVersionCache)

	client := &llmrouterxds.ClientState{
		ClientID: "node-1",
		Nonce:    make(map[string]*llmrouterxds.ClientTypeState),
		SendCh:   make(chan *llmrouterxds.DiscoveryResponse, 10),
		CloseCh:  make(chan struct{}),
	}
	server.AddClient(uuid.NewString(), client)
	return server, client
}
func TestACKTriggersPendingPush(t *testing.T) {
	server, client := newTestServerWithClient()

	// 第一次 Delta Push
	event1 := llmrouterxds.XDSPushEvent{
		Type:             llmrouterxds.EDSType,
		AffectedServices: []string{"svc-a"},
	}
	err := server.PushDeltaResources(event1)
	require.NoError(t, err)

	// 收到第一次推送
	var firstResp *llmrouterxds.DiscoveryResponse
	select {
	case firstResp = <-client.SendCh:
		require.NotNil(t, firstResp)
	default:
		t.Fatal("expected first delta push")
	}

	state := client.Nonce[string(llmrouterxds.EDSType)]
	require.True(t, state.InFlight)
	require.Equal(t, firstResp.Nonce, state.LastSentNonce)

	// 第二次 Delta Push（InFlight 中 → Pending）
	event2 := llmrouterxds.XDSPushEvent{
		Type:             llmrouterxds.EDSType,
		AffectedServices: []string{"svc-b"},
	}
	err = server.PushDeltaResources(event2)
	require.NoError(t, err)

	require.NotNil(t, state.PendingPush)
	require.Contains(t, state.PendingPush.AffectedServices, "svc-b")

	// ===== 模拟 Client ACK =====
	client.Mu.Lock()
	state.InFlight = false
	state.LastAckNonce = firstResp.Nonce
	state.LastAckVersion = firstResp.VersionInfo

	pending := *state.PendingPush
	state.PendingPush = nil
	client.Mu.Unlock()

	// ACK 后触发 PendingPush
	go server.PushDeltaResources(pending)

	// 应该收到第二次推送
	time.Sleep(50 * time.Millisecond)
	select {
	case secondResp := <-client.SendCh:
		require.NotEqual(t, firstResp.Nonce, secondResp.Nonce)
		require.Equal(t, string(llmrouterxds.EDSType), secondResp.TypeUrl)
	default:
		t.Fatal("expected pending push after ACK")
	}
}
