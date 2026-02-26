package utils

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	llmrouterxds "github.com/duuuuu17/llm-router-operator/internal/llmrouter-xds"
)

func TestConcurrentPushAndACK_RaceFree(t *testing.T) {
	server, client := newTestServerWithClient()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 模拟 ACK goroutine
	go func() {
		for {
			select {
			case resp := <-client.SendCh:
				client.Mu.Lock()
				state := client.Nonce[resp.TypeUrl]
				if state != nil && state.LastSentNonce == resp.Nonce {
					state.InFlight = false
					state.LastAckNonce = resp.Nonce
					state.LastAckVersion = resp.VersionInfo
					t.Log("successful sync status")
					if state.PendingPush != nil {
						t.Log("Push by self after ACK client req send")
						p := *state.PendingPush
						state.PendingPush = nil
						client.Mu.Unlock()
						go server.PushDeltaResources(p)
						continue
					}
				}
				client.Mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()

	// 并发 Delta Push
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event := llmrouterxds.XDSPushEvent{
				Type:             llmrouterxds.EDSType,
				AffectedServices: []string{fmt.Sprintf("svc-%d", i)},
			}
			_ = server.PushDeltaResources(event)
		}(i)
	}

	// 并发 SotW Push
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = server.PushResourcesSotW()
	}()

	wg.Wait()

	// 给 ACK goroutine 一点时间 drain
	time.Sleep(200 * time.Millisecond)
}
