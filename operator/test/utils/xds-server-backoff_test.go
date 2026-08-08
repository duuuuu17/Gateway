package utils

import (
	"testing"
	"time"

	llmrouterxds "github.com/duuuuu17/llm-router-operator/internal/llmrouter-xds"
)

func TestNACKTriggersBackoff(t *testing.T) {
	server, client := newTestServerWithClient()

	// 第一次推送
	event := llmrouterxds.XDSPushEvent{
		Type:             llmrouterxds.EDSType,
		AffectedServices: []string{"svc-a"},
	}
	server.PushDeltaResources(event)

	resp := <-client.SendCh
	t.Logf("mock server send resp to client, StreamAggregateResponse:%+v\n", resp)
	state := client.Nonce[string(llmrouterxds.EDSType)]
	t.Logf("Nack Before ClientState:%+v PendingPush:%+v\n", state, state.PendingPush)

	// 模拟 NACK
	client.Mu.Lock()
	state.InFlight = false
	state.NackCount = 1
	state.LastNackAt = time.Now()
	client.Mu.Unlock()
	// 立刻再次推送（应被 backoff）
	server.PushDeltaResources(event)
	t.Logf("Nack after ClientState:%+v PendingPush:%+v\n", state, state.PendingPush)
	select {
	case <-client.SendCh:
		t.Fatal("should not push during backoff window")
	default:
		// ok
	}
}
func TestBackoffExpires_AllowsPush(t *testing.T) {
	server, client := newTestServerWithClient()

	event := llmrouterxds.XDSPushEvent{
		Type:             llmrouterxds.EDSType,
		AffectedServices: []string{"svc-a"},
	}

	state := &llmrouterxds.ClientTypeState{
		NackCount:  1,
		LastNackAt: time.Now().Add(-3 * time.Second),
	}
	client.Nonce[string(llmrouterxds.EDSType)] = state

	server.PushDeltaResources(event)

	select {
	case <-client.SendCh:
		// ok
	default:
		t.Fatal("expected push after backoff expires")
	}
}
