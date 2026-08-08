package llmrouterxds

import (
	"maps"

	"google.golang.org/protobuf/proto"
)

type SnapshotResource[T any] struct {
	Resources map[string]T
}
type Snapshot struct {
	CDS SnapshotResource[*LLMRouterCluster]
	RDS SnapshotResource[*LLMRouterRouting]
	EDS SnapshotResource[*LLMRouterEndpointAssignment]
	TDS SnapshotResource[*LLMRouterTenantPipelineConfigs]
}

func (s *XDSController) BuildSnapshot() bool {

	// ref counter
	s.CDSController.rwMutex.RLock()
	defer s.CDSController.rwMutex.RUnlock()

	s.RDSController.rwMutex.RLock()
	defer s.RDSController.rwMutex.RUnlock()

	s.EDSController.rwMutex.RLock()
	defer s.EDSController.rwMutex.RUnlock()

	s.TDSController.rwMutex.RLock()
	defer s.TDSController.rwMutex.RUnlock()

	snap := &Snapshot{
		CDS: CloneMap(s.CDSController.Resources),
		RDS: CloneMap(s.RDSController.Resources),
		EDS: CloneMap(s.EDSController.Resources),
		TDS: CloneMap(s.TDSController.Resources),
		// CDS: CloneCDS(s.CDSController.Resources),
		// RDS: CloneRDS(s.RDSController.Resources),
		// EDS: CloneEDS(s.EDSController.Resources),
		// TDS: CloneTDS(s.TDSController.Resources),
	}
	s.snapshot.Store(snap)
	return true
}
func CloneMap[T any](src map[string]T) SnapshotResource[T] {
	dst := make(map[string]T, len(src))
	// XDSController缓存的设计是使用的指针保存，所以本身可以直接使用指针引用
	// 在go中，map delete键值对后，其指针value仍被snapshot引用可达
	// 属于GC中的黑色标记
	for k, v := range src {
		if msg, ok := any(v).(proto.Message); ok {
			cloned := proto.Clone(msg)
			dst[k] = any(cloned).(T)
		} else {
			dst[k] = v
		}
		// clone := reflect.TypeFor[T]().Elem()
		// dst[k] =
	}
	maps.Copy(dst, src)
	return SnapshotResource[T]{Resources: dst}
}

func CloneCDS(src map[string]*LLMRouterCluster) SnapshotResource[*LLMRouterCluster] {
	dst := make(map[string]*LLMRouterCluster, len(src))
	for k, v := range src {
		if v == nil {
			continue
		}
		// 必须复制底层数据！
		clone := LLMRouterCluster{
			LbStrategy: v.GetLbStrategy(),
			Name:       v.GetName(),
			Models:     v.GetModels(),
			Protocols:  v.GetProtocols(),
			Streaming:  v.GetStreaming(),
		} // 如果 LLMRouterCluster 内部还有切片或指针，需要继续深拷贝
		dst[k] = &clone
	}
	return SnapshotResource[*LLMRouterCluster]{Resources: dst}
}

func CloneRDS(src map[string]*LLMRouterRouting) SnapshotResource[*LLMRouterRouting] {
	dst := make(map[string]*LLMRouterRouting, len(src))
	for k, v := range src {
		if v == nil {
			continue
		}
		// 必须复制底层数据！
		clone := LLMRouterRouting{
			ClusterName: v.GetClusterName(),
			Selector:    v.GetSelector(),
			Region:      v.GetRegion(),
			Weight:      v.GetWeight(),
			Priority:    v.GetPriority(),
			CanaryRatio: v.GetCanaryRatio(),
		} // 如果 LLMRouterCluster 内部还有切片或指针，需要继续深拷贝
		dst[k] = &clone
	}
	return SnapshotResource[*LLMRouterRouting]{Resources: dst}
}

func CloneEDS(src map[string]*LLMRouterEndpointAssignment) SnapshotResource[*LLMRouterEndpointAssignment] {
	dst := make(map[string]*LLMRouterEndpointAssignment, len(src))
	for k, v := range src {
		if v == nil {
			continue
		}
		// 必须复制底层数据！
		clone := LLMRouterEndpointAssignment{
			ClusterName: v.GetClusterName(),
			Endpoints:   v.GetEndpoints(),
		} // 如果 LLMRouterCluster 内部还有切片或指针，需要继续深拷贝
		dst[k] = &clone
	}
	return SnapshotResource[*LLMRouterEndpointAssignment]{Resources: dst}
}

func CloneTDS(src map[string]*LLMRouterTenantPipelineConfigs) SnapshotResource[*LLMRouterTenantPipelineConfigs] {
	dst := make(map[string]*LLMRouterTenantPipelineConfigs, len(src))
	for k, v := range src {
		if v == nil {
			continue
		}
		// 必须复制底层数据！
		clone := LLMRouterTenantPipelineConfigs{
			TenantConfigId: v.GetTenantConfigId(),
			Tenants:        v.GetTenants(),
		} // 如果 LLMRouterCluster 内部还有切片或指针，需要继续深拷贝
		dst[k] = &clone
	}
	return SnapshotResource[*LLMRouterTenantPipelineConfigs]{Resources: dst}
}

func (s *XDSController) GetCDS(services ...string) ([]*LLMRouterCluster, []string) {
	s.CDSController.rwMutex.RLock()
	defer s.CDSController.rwMutex.RUnlock()
	cdss := make([]*LLMRouterCluster, 0, len(services))
	needRemoveServices := make([]string, 0)
	snapshot := s.snapshot.Load().(*Snapshot)
	for _, service := range services {
		cds, ok := snapshot.CDS.Resources[service]
		if !ok {
			needRemoveServices = append(needRemoveServices, service)
			continue
		}
		cdss = append(cdss, cds)
	}
	return cdss, needRemoveServices
}

func (s *XDSController) GetEDS(services ...string) ([]*LLMRouterEndpointAssignment, []string) {
	s.EDSController.rwMutex.RLock()
	defer s.EDSController.rwMutex.RUnlock()
	edss := make([]*LLMRouterEndpointAssignment, 0, len(services))
	needRemoveServices := make([]string, 0)
	snapshot := s.snapshot.Load().(*Snapshot)
	for _, service := range services {
		eds, ok := snapshot.EDS.Resources[service]
		if !ok {
			needRemoveServices = append(needRemoveServices, service)
			continue
		}
		edss = append(edss, eds)
	}
	return edss, needRemoveServices
}

func (s *XDSController) GetRDS(services ...string) ([]*LLMRouterRouting, []string) {
	s.RDSController.rwMutex.RLock()
	defer s.RDSController.rwMutex.RUnlock()
	rdss := make([]*LLMRouterRouting, 0, len(services))
	needRemoveServices := make([]string, 0)
	snapshot := s.snapshot.Load().(*Snapshot)
	for _, service := range services {
		rds, ok := snapshot.RDS.Resources[service]
		if !ok {
			needRemoveServices = append(needRemoveServices, service)
			continue
		}
		rdss = append(rdss, rds)
	}
	return rdss, needRemoveServices
}
func (s *XDSController) GetTDS(uids ...string) ([]*LLMRouterTenantPipelineConfigs, []string) {
	s.TDSController.rwMutex.RLock()
	defer s.TDSController.rwMutex.RUnlock()
	tdss := make([]*LLMRouterTenantPipelineConfigs, 0, len(uids))
	needRemoveServices := make([]string, 0)
	snapshot := s.snapshot.Load().(*Snapshot)
	for _, service := range uids {
		tds, ok := snapshot.TDS.Resources[service]
		if !ok {
			needRemoveServices = append(needRemoveServices, service)
			continue
		}
		tdss = append(tdss, tds)
	}
	return tdss, needRemoveServices
}
func (s *XDSController) GetCDSAll() []*LLMRouterCluster {
	s.CDSController.rwMutex.RLock()
	defer s.CDSController.rwMutex.RUnlock()
	clusters := make([]*LLMRouterCluster, 0)
	snapshot := s.snapshot.Load().(*Snapshot)
	for _, c := range snapshot.CDS.Resources {
		clusters = append(clusters, c)
	}
	return clusters
}

func (s *XDSController) GetEDSAll() []*LLMRouterEndpointAssignment {
	s.EDSController.rwMutex.RLock()
	defer s.EDSController.rwMutex.RUnlock()
	clusters := make([]*LLMRouterEndpointAssignment, 0)
	snapshot := s.snapshot.Load().(*Snapshot)
	for _, e := range snapshot.EDS.Resources {
		clusters = append(clusters, e)
	}
	return clusters
}

func (s *XDSController) GetRDSAll() []*LLMRouterRouting {
	s.RDSController.rwMutex.RLock()
	defer s.RDSController.rwMutex.RUnlock()
	clusters := make([]*LLMRouterRouting, 0)
	snapshot := s.snapshot.Load().(*Snapshot)
	for _, r := range snapshot.RDS.Resources {
		clusters = append(clusters, r)
	}
	return clusters
}
func (s *XDSController) GetTDSAll() []*LLMRouterTenantPipelineConfigs {
	s.TDSController.rwMutex.RLock()
	defer s.TDSController.rwMutex.RUnlock()
	clusters := make([]*LLMRouterTenantPipelineConfigs, 0)
	snapshot := s.snapshot.Load().(*Snapshot)
	for _, r := range snapshot.TDS.Resources {
		clusters = append(clusters, r)
	}
	return clusters
}
