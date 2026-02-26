package llmrouterxds

import (
	"slices"
	"sync"
)

type CacheStreamAggregateResponses interface {
	Get(typeURL, lastAckVersion string) *DiscoveryResponse
	Add(typeURL string, Response VersionedResponse)
}
type VersionedResponse struct {
	Version string
	Resp    *DiscoveryResponse
}
type RespVersionCache struct {
	mu      sync.RWMutex                   // 使用 RWMutex 提升读取性能
	m       map[string][]VersionedResponse // typeUrl -> versions
	maxSize int                            // 限制每个 typeURL 的最大缓存数量
}

func NewRespVersionCache(maxSize int) *RespVersionCache {
	if maxSize <= 0 {
		maxSize = 15 // 默认值
	}
	return &RespVersionCache{
		mu:      sync.RWMutex{}, // 使用 RWMutex
		m:       make(map[string][]VersionedResponse),
		maxSize: maxSize,
	}
}

func (rvc *RespVersionCache) Get(typeURL, lastAckVersion string) *DiscoveryResponse {
	rvc.mu.RLock()
	defer rvc.mu.RUnlock()
	versionedList := rvc.m[typeURL]
	foundAckedVersion := slices.IndexFunc(versionedList, func(vr VersionedResponse) bool {
		if vr.Version == lastAckVersion {
			return true
		}
		return false
	})
	if foundAckedVersion == -1 {
		return nil
	}
	return versionedList[foundAckedVersion].Resp
}
func (rvc *RespVersionCache) Add(typeURL string, Response VersionedResponse) {
	rvc.mu.Lock()
	defer rvc.mu.Unlock()
	versionedList := rvc.m[typeURL]
	foundAckedVersion := slices.IndexFunc(versionedList, func(vr VersionedResponse) bool {
		if vr.Version == Response.Version {
			return true
		}
		return false
	})
	// 裁剪，多余的本地缓存
	if foundAckedVersion == -1 {
		versionedList = append(versionedList, Response)
		if len(versionedList) > rvc.maxSize {
			versionedList = versionedList[len(versionedList)-rvc.maxSize:]
		}
		rvc.m[typeURL] = versionedList
	}
}
