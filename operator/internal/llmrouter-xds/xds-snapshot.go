package llmrouterxds

import "maps"

type SnapshotResource[T any] struct {
	Resources map[string]T
}
type Snapshot struct {
	CDS SnapshotResource[*LLMRouterCluster]
	RDS SnapshotResource[*LLMRouterRouting]
	EDS SnapshotResource[*LLMRouterEndpointAssignment]
}

func (s *XDSController) BuildSnapshot() bool {
	// deepcopy way
	// snap := &Snapshot{
	// 	CDS: SnapshotResource[*LLMRouterCluster]{make(map[string]*LLMRouterCluster, len(s.CDSController.Resources))},
	// 	RDS: SnapshotResource[*LLMRouterRouting]{make(map[string]*LLMRouterRouting, len(s.RDSController.Resources))},
	// 	EDS: SnapshotResource[*LLMRouterEndpointAssignment]{make(map[string]*LLMRouterEndpointAssignment, len(s.EDSController.Resources))},
	// }
	// var wg sync.WaitGroup
	// wg.Add(3)
	// go func() {
	// 	defer wg.Done()
	// 	s.CDSController.rwMutex.Lock()
	// 	for k, v := range s.CDSController.Resources {
	// 		// deepcopy
	// 		val, ok := proto.Clone(v).(*LLMRouterCluster)
	// 		if !ok {
	// 			continue
	// 		}
	// 		snap.CDS.Resources[k] = val
	// 	}
	// 	s.CDSController.rwMutex.Unlock()
	// }()
	// go func() {
	// 	defer wg.Done()
	// 	s.RDSController.rwMutex.Lock()
	// 	for k, v := range s.RDSController.Resources {
	// 		val, ok := proto.Clone(v).(*LLMRouterRouting)
	// 		if !ok {
	// 			continue
	// 		}
	// 		snap.RDS.Resources[k] = val
	// 	}
	// 	s.RDSController.rwMutex.Unlock()
	// }()
	// go func() {
	// 	defer wg.Done()
	// 	s.EDSController.rwMutex.Lock()
	// 	for k, v := range s.EDSController.Resources {
	// 		val, ok := proto.Clone(v).(*LLMRouterEndpointAssignment)
	// 		if !ok {
	// 			continue
	// 		}
	// 		snap.EDS.Resources[k] = val
	// 	}
	// 	s.EDSController.rwMutex.Unlock()
	// }()
	// wg.Wait()

	// ref counter
	s.CDSController.rwMutex.RLock()
	defer s.CDSController.rwMutex.RUnlock()

	s.RDSController.rwMutex.RLock()
	defer s.RDSController.rwMutex.RUnlock()

	s.EDSController.rwMutex.RLock()
	defer s.EDSController.rwMutex.RUnlock()
	snap := &Snapshot{
		CDS: CloneMap(s.CDSController.Resources),
		RDS: CloneMap(s.RDSController.Resources),
		EDS: CloneMap(s.EDSController.Resources),
	}
	s.snapshot.Store(snap)
	return true
}
func CloneMap[T any](src map[string]T) SnapshotResource[T] {
	dst := make(map[string]T, len(src))
	// XDSController缓存的设计是使用的指针保存，所以本身可以直接使用指针引用
	// 在go中，map delete键值对后，期指针value仍被snapshot引用可达
	// 属于GC中的黑色标记
	maps.Copy(dst, src)
	return SnapshotResource[T]{Resources: dst}
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
	return cdss, services
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
