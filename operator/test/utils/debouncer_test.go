/*
Copyright 2026 duuuuu17.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	llmrouterxds "github.com/duuuuu17/llm-router-operator/internal/llmrouter-xds"
)

func TestDebouncerInstance(t *testing.T) {
	debouncer := llmrouterxds.NewChanDebouncer(500 * time.Millisecond)
	reconcilerEvents := make([]llmrouterxds.ReconcilerPushEvent, 0, 5)
	for i := range 5 {
		reconcilerEvents = append(reconcilerEvents, llmrouterxds.ReconcilerPushEvent{
			Type:             llmrouterxds.CDSType,
			AffectedServices: []string{fmt.Sprintf("svc-%d", i)},
		})
	}
	mux := sync.Mutex{}
	grpcServerEvents := make([]llmrouterxds.XDSPushEvent, 0, 5)
	debouncer.Start()
	ctx, cancel := context.WithCancel(context.TODO())
	//  模拟reconciler入队
	go func() {
		for _, ev := range reconcilerEvents {
			debouncer.Enqueue(ev)
		}
	}()
	// 模拟grpc Server获取事件
	go func(ctx context.Context) {
		for {
			select {
			case ev, _ := <-debouncer.Events():
				mux.Lock()
				grpcServerEvents = append(grpcServerEvents, ev)
				mux.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}(ctx)
	// 检测是否能够获取
	// 结果
	timeout := time.NewTimer(5 * time.Second)
	for {
		mux.Lock()
		if len(grpcServerEvents) == 5 {
			mux.Unlock()
			break
		}
		mux.Unlock()
		select {
		case <-timeout.C:
			timeout.Stop()
			cancel()
			debouncer.Stop()
			return
		default:
		}
	}
	t.Log("Debouncer Successful")
	cancel()
	debouncer.Stop()
	timeout.Stop()
}
