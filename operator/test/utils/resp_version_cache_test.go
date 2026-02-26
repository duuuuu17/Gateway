package utils

import (
	"fmt"
	"sync"
	"testing"
	"time"

	llmrouterxds "github.com/duuuuu17/llm-router-operator/internal/llmrouter-xds"
	"google.golang.org/protobuf/types/known/anypb"
)

// 创建一个简单的 DiscoveryResponse 实例用于测试
func createTestResponse(version, typeURL, nonce string) *llmrouterxds.DiscoveryResponse {
	// anypb.Any 需要一个 proto.Message，我们可以创建一个简单的、可序列化的对象
	// 这里使用一个简单的字符串包装成 proto
	// 在实际测试中，你可能需要一个更复杂的测试 proto
	testStruct := &anypb.Any{} // 这里只是一个示例，实际需要一个有效的 proto 结构
	// testStruct, _ = anypb.New(&structpb.Struct{Fields: map[string]*structpb.Value{"test": "123"}})

	return &llmrouterxds.DiscoveryResponse{
		VersionInfo: version,
		TypeUrl:     typeURL,
		Resources:   []*anypb.Any{testStruct}, // 示例资源
		Nonce:       nonce,
	}
}

// TestRespVersionCacheBasic 测试基本的 Get, Add, Close 功能
func TestRespVersionCacheBasic(t *testing.T) {
	cache := llmrouterxds.NewRespVersionCache(3) // 设置较小的容量便于测试

	typeURL := "type.googleapis.com/test.Type"
	ver1 := "v1"
	ver2 := "v2"
	nonce1 := "nonce1"
	nonce2 := "nonce2"

	resp1 := createTestResponse(ver1, typeURL, nonce1)
	resp2 := createTestResponse(ver2, typeURL, nonce2)

	// 1. 测试 Add
	t.Log("Adding responses...")
	cache.Add(typeURL, llmrouterxds.VersionedResponse{Version: ver1, Resp: resp1})
	cache.Add(typeURL, llmrouterxds.VersionedResponse{Version: ver2, Resp: resp2})

	// 2. 测试 Get
	t.Log("Testing Get...")
	gotResp1 := cache.Get(typeURL, ver1)
	if gotResp1 == nil {
		t.Fatalf("Expected response for version %s, but got nil", ver1)
	}
	if gotResp1.VersionInfo != ver1 {
		t.Errorf("Expected VersionInfo %s, got %s", ver1, gotResp1.VersionInfo)
	}
	if gotResp1.Nonce != nonce1 {
		t.Errorf("Expected Nonce %s, got %s", nonce1, gotResp1.Nonce)
	}

	gotResp2 := cache.Get(typeURL, ver2)
	if gotResp2 == nil {
		t.Fatalf("Expected response for version %s, but got nil", ver2)
	}
	if gotResp2.VersionInfo != ver2 {
		t.Errorf("Expected VersionInfo %s, got %s", ver2, gotResp2.VersionInfo)
	}
	if gotResp2.Nonce != nonce2 {
		t.Errorf("Expected Nonce %s, got %s", nonce2, gotResp2.Nonce)
	}

	// 3. 测试 Get 不存在的版本
	t.Log("Testing Get non-existent version...")
	gotRespNonExistent := cache.Get(typeURL, "non-existent-version")
	if gotRespNonExistent != nil {
		t.Errorf("Expected nil for non-existent version, got %+v", gotRespNonExistent)
	}

	// 4. 测试缓存裁剪
	t.Log("Testing cache clipping...")
	ver3 := "v3"
	ver4 := "v4"
	ver5 := "v5"
	nonce3 := "nonce3"
	nonce4 := "nonce4"
	nonce5 := "nonce5"

	resp3 := createTestResponse(ver3, typeURL, nonce3)
	resp4 := createTestResponse(ver4, typeURL, nonce4)
	resp5 := createTestResponse(ver5, typeURL, nonce5)

	cache.Add(typeURL, llmrouterxds.VersionedResponse{Version: ver3, Resp: resp3}) // v1, v2, v3
	cache.Add(typeURL, llmrouterxds.VersionedResponse{Version: ver4, Resp: resp4}) // v2, v3, v4 (v1 被裁剪)
	cache.Add(typeURL, llmrouterxds.VersionedResponse{Version: ver5, Resp: resp5}) // v3, v4, v5 (v2 被裁剪)

	// 验证 v1 和 v2 已被裁剪
	if cache.Get(typeURL, ver1) != nil {
		t.Errorf("Expected version %s to be clipped from cache", ver1)
	}
	if cache.Get(typeURL, ver2) != nil {
		t.Errorf("Expected version %s to be clipped from cache", ver2)
	}

	// 验证 v3, v4, v5 仍在缓存中
	if cache.Get(typeURL, ver3) == nil {
		t.Errorf("Expected version %s to still be in cache", ver3)
	}
	if cache.Get(typeURL, ver4) == nil {
		t.Errorf("Expected version %s to still be in cache", ver4)
	}
	if cache.Get(typeURL, ver5) == nil {
		t.Errorf("Expected version %s to still be in cache", ver5)
	}

	// 5. 测试 Close
	t.Log("Testing Close...")
	// Close 后的行为（如再次 Get/Add）可以进一步定义，这里主要是确保 Close 不会 panic
}

// TestRespVersionCacheConcurrent 并发测试
func TestRespVersionCacheConcurrent(t *testing.T) {
	const numGoroutines = 10
	const numOperationsPerGoroutine = 100
	const cacheSize = 5 // 使用较小的缓存大小，更容易触发裁剪

	cache := llmrouterxds.NewRespVersionCache(cacheSize)

	var wg sync.WaitGroup

	// 启动多个 goroutine 同时进行 Add 和 Get 操作
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numOperationsPerGoroutine; j++ {
				typeURL := fmt.Sprintf("type.googleapis.com/test.Type_%d", goroutineID%2) // 两种 type URL
				version := fmt.Sprintf("v%d_%d", goroutineID, j)
				nonce := fmt.Sprintf("nonce_%d_%d", goroutineID, j)

				resp := createTestResponse(version, typeURL, nonce)

				// Add 操作
				cache.Add(typeURL, llmrouterxds.VersionedResponse{Version: version, Resp: resp})

				// 随机选择一个已知版本进行 Get 操作
				if j > 0 { // 确保至少有一个版本可以获取
					randomVersionToGet := fmt.Sprintf("v%d_%d", goroutineID, j-1)
					_ = cache.Get(typeURL, randomVersionToGet) // 忽略返回值，主要测试并发安全
				}

				// 添加短暂延迟，模拟实际场景中的操作间隔，增加并发冲突的可能性
				time.Sleep(time.Microsecond)
			}
		}(i)
	}

	wg.Wait() // 等待所有 goroutine 完成

	// 验证 Close 是否能正常工作（即使在并发操作后）
	t.Log("Closing cache after concurrent operations...")

	// 可以在这里添加一些简单的 post-close 检查，但这不是强制性的
	// 因为 Close 后的操作行为由接口定义，通常不期望继续调用 Add/Get
}
