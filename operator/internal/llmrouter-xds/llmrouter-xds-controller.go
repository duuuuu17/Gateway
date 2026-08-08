package llmrouterxds

import (
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
)

// reconciler call update
type XDSManager interface {
	// update serviceConfig
	UpdateOrCreateServiceConfig(serviceName string, servicePort *ServiceConfig) bool
	// update RDS
	UpdateOrCreateRDS(serviceName string, routes *LLMRouterRouting, servicePort *ServiceConfig) bool
	// update CDS
	UpdateOrCreateCDS(serviceName string, cluster *LLMRouterCluster, servicePort *ServiceConfig) bool
	// update EDS
	// 不需要ServiceConfig
	// 因为是使用了K8s原生资源EndpointSlices，其Reconciler获取资源事件是必在已知被监听的Service下执行过滤后得到
	UpdateOrCreateEDS(serviceName string, endpoints []*LLMRouterEndpoint) bool
	// Tenants's UID & peer tenantID updated
	// Coarse-grained update
	UpsertTDSByUID(tenants *LLMRouterTenantPipelineConfigs) bool
	IsWatchedService(serviceName string) bool
	GetServiceConfig(serviceName string) (ServiceConfig, bool)
	NeedRemovedService(serviceName ...string) []string
	DeleteAllXDSs() bool
	DeleteXDSs(serviceNames ...string) bool
	DeleteTDSCache(tenantID string)
}

// grpc push using
type XDSStore interface {
	GetCDS(services ...string) ([]*LLMRouterCluster, []string)
	GetEDS(services ...string) ([]*LLMRouterEndpointAssignment, []string)
	GetRDS(services ...string) ([]*LLMRouterRouting, []string)
	GetTDS(uids ...string) ([]*LLMRouterTenantPipelineConfigs, []string)
	GetCDSAll() []*LLMRouterCluster
	GetEDSAll() []*LLMRouterEndpointAssignment
	GetRDSAll() []*LLMRouterRouting
	GetTDSAll() []*LLMRouterTenantPipelineConfigs
	RefreshSnapshot() error
}

// snapshot cache
type XDSController struct {
	watchMu       sync.RWMutex
	watched       map[string]ServiceConfig
	CDSController *ResourcesController[*LLMRouterCluster]               // key: servicename
	RDSController *ResourcesController[*LLMRouterRouting]               // key: servicename
	EDSController *ResourcesController[*LLMRouterEndpointAssignment]    // key: ServiceName
	TDSController *ResourcesController[*LLMRouterTenantPipelineConfigs] // key: TDS's namespace/name/UID

	snapshot atomic.Value // *Snapshot
}
type ResourcesController[T any] struct {
	rwMutex   sync.RWMutex
	Resources map[string]T
}

// CR指定的service Port
type ServiceConfig struct {
	TargetPort     *int32
	TargetPortName string
}

func NewXDSStorage() *XDSController {
	xdsc := &XDSController{
		watchMu: sync.RWMutex{},
		watched: map[string]ServiceConfig{},
		CDSController: &ResourcesController[*LLMRouterCluster]{
			rwMutex:   sync.RWMutex{},
			Resources: make(map[string]*LLMRouterCluster),
		},
		RDSController: &ResourcesController[*LLMRouterRouting]{
			rwMutex:   sync.RWMutex{},
			Resources: make(map[string]*LLMRouterRouting),
		},
		EDSController: &ResourcesController[*LLMRouterEndpointAssignment]{
			rwMutex:   sync.RWMutex{},
			Resources: make(map[string]*LLMRouterEndpointAssignment),
		},
		TDSController: &ResourcesController[*LLMRouterTenantPipelineConfigs]{
			rwMutex:   sync.RWMutex{},
			Resources: make(map[string]*LLMRouterTenantPipelineConfigs),
		},
		snapshot: atomic.Value{},
	}
	xdsc.snapshot.Store(&Snapshot{})
	return xdsc
}
func (s *XDSController) RefreshSnapshot() error {
	if s.BuildSnapshot() {
		return nil
	}
	return fmt.Errorf("couldn't build new snapshot")
}

// func (s *XDSController) GetCDS(services ...string) ([]*LLMRouterCluster, []string) {
// 	s.CDSController.rwMutex.RLock()
// 	defer s.CDSController.rwMutex.RUnlock()
// 	cdss := make([]*LLMRouterCluster, 0, len(services))
// 	needRemoveServices := make([]string, 0)
// 	for _, service := range services {
// 		if !s.IsWatchedService(service) {
// 			needRemoveServices = append(needRemoveServices, service)
// 			continue
// 		}
// 		cdss = append(cdss, s.CDSController.Resources[service])
// 	}
// 	return cdss, services
// }
// func (s *XDSController) GetEDS(services ...string) ([]*LLMRouterEndpointAssignment, []string) {
// 	s.EDSController.rwMutex.RLock()
// 	defer s.EDSController.rwMutex.RUnlock()
// 	edss := make([]*LLMRouterEndpointAssignment, 0, len(services))
// 	needRemoveServices := make([]string, 0)
// 	for _, service := range services {
// 		if !s.IsWatchedService(service) {
// 			needRemoveServices = append(needRemoveServices, service)
// 			continue
// 		}
// 		edss = append(edss, s.EDSController.Resources[service])
// 	}
// 	return edss, needRemoveServices
// }
// func (s *XDSController) GetRDS(services ...string) ([]*LLMRouterRouting, []string) {
// 	s.RDSController.rwMutex.RLock()
// 	defer s.RDSController.rwMutex.RUnlock()
// 	rdss := make([]*LLMRouterRouting, 0, len(services))
// 	needRemoveServices := make([]string, 0)
// 	for _, service := range services {
// 		if !s.IsWatchedService(service) {
// 			needRemoveServices = append(needRemoveServices, service)
// 			continue
// 		}
// 		rdss = append(rdss, s.RDSController.Resources[service])
// 	}
// 	return rdss, needRemoveServices
// }
// func (s *XDSController) GetCDSAll() []*LLMRouterCluster {
// 	s.CDSController.rwMutex.RLock()
// 	defer s.CDSController.rwMutex.RUnlock()
// 	clusters := make([]*LLMRouterCluster, 0)
// 	for _, c := range s.CDSController.Resources {
// 		clusters = append(clusters, c)
// 	}
// 	return clusters
// }
// func (s *XDSController) GetEDSAll() []*LLMRouterEndpointAssignment {
// 	s.EDSController.rwMutex.RLock()
// 	defer s.EDSController.rwMutex.RUnlock()
// 	clusters := make([]*LLMRouterEndpointAssignment, 0)
// 	for _, e := range s.EDSController.Resources {
// 		clusters = append(clusters, e)
// 	}
// 	return clusters
// }
// func (s *XDSController) GetRDSAll() []*LLMRouterRouting {
// 	s.RDSController.rwMutex.RLock()
// 	defer s.RDSController.rwMutex.RUnlock()
// 	clusters := make([]*LLMRouterRouting, 0)
// 	for _, r := range s.RDSController.Resources {
// 		clusters = append(clusters, r)
// 	}
// 	return clusters
// }

// update RDS
func (s *XDSController) UpdateOrCreateRDS(serviceName string, routes *LLMRouterRouting, servicePort *ServiceConfig) bool {
	s.RDSController.rwMutex.Lock()
	defer s.RDSController.rwMutex.Unlock()
	// 获取旧值
	oldRoutes, exists := s.RDSController.Resources[serviceName]
	// 利用protobuf库的等值检查是否需要更新
	if exists && proto.Equal(oldRoutes, routes) {
		return false
	}
	s.RDSController.Resources[serviceName] = routes
	return true
}

// update CDS
func (s *XDSController) UpdateOrCreateCDS(serviceName string, cluster *LLMRouterCluster, servicePort *ServiceConfig) bool {
	s.CDSController.rwMutex.Lock()
	defer s.CDSController.rwMutex.Unlock()
	oldCluster, exists := s.CDSController.Resources[serviceName]
	if exists && proto.Equal(oldCluster, cluster) {
		return false
	}
	s.CDSController.Resources[serviceName] = cluster
	return true
}

// update EDS
func (s *XDSController) UpdateOrCreateEDS(serviceName string, endpoints []*LLMRouterEndpoint) bool {
	es := make([]*LLMRouterEndpoint, 0, len(endpoints))
	es = append(es, endpoints...)
	s.EDSController.rwMutex.Lock()
	defer s.EDSController.rwMutex.Unlock()

	eds := &LLMRouterEndpointAssignment{
		ClusterName: serviceName,
		Endpoints:   es,
	}
	oldEndpoints, exists := s.EDSController.Resources[serviceName]
	if exists && proto.Equal(oldEndpoints, eds) {
		return false
	}
	s.EDSController.Resources[serviceName] = eds
	return true
}
func (s *XDSController) UpsertTDSByUID(newTenants *LLMRouterTenantPipelineConfigs) bool {
	s.TDSController.rwMutex.Lock()
	defer s.TDSController.rwMutex.Unlock()

	oldTenants, ok := s.TDSController.Resources[newTenants.GetTenantConfigId()]
	if ok && proto.Equal(oldTenants, newTenants) {
		return false
	}
	s.TDSController.Resources[newTenants.GetTenantConfigId()] = newTenants
	return true
}
func (s *XDSController) IsWatchedService(serviceName string) bool {
	s.watchMu.RLock()
	defer s.watchMu.RUnlock()
	_, ok := s.watched[serviceName]
	return ok
}
func (s *XDSController) UpdateOrCreateServiceConfig(serviceName string, servicePort *ServiceConfig) bool {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	oldCfg, exists := s.watched[serviceName]
	if exists && ServiceConfigEqual(&oldCfg, servicePort) {
		return false
	}
	s.watched[serviceName] = *servicePort
	return true
}
func ServiceConfigEqual(old, new *ServiceConfig) bool {
	if old.TargetPort != nil && *old.TargetPort == *new.TargetPort {
		return true
	}
	if old.TargetPortName != "" && old.TargetPortName == new.TargetPortName {
		return true
	}
	return false
}
func (s *XDSController) GetServiceConfig(serviceName string) (ServiceConfig, bool) {
	s.watchMu.RLock()
	defer s.watchMu.RUnlock()
	serviceConfig, ok := s.watched[serviceName]
	return serviceConfig, ok
}
func (s *XDSController) NeedRemovedService(serviceNames ...string) []string {
	s.watchMu.Lock()
	desireSet := make(map[string]bool, len(serviceNames))
	for _, key := range serviceNames {
		desireSet[key] = true
	}
	Removekeys := make([]string, 0, len(s.watched)-len(serviceNames))
	for k := range s.watched {
		// diff get removeServices: current - desire
		if !desireSet[k] {
			Removekeys = append(Removekeys, k)
			delete(s.watched, k)
		}
	}
	s.watchMu.Unlock()
	s.DeleteXDSs(Removekeys...)
	return Removekeys
}
func (s *XDSController) DeleteXDSCache(key string) {
	s.DeleteCDSCache(key)
	s.DeleteEDSCache(key)
	s.DeleteRDSCache(key)
}
func (s *XDSController) DeleteCDSCache(key string) {
	s.CDSController.rwMutex.Lock()
	delete(s.CDSController.Resources, key)
	s.CDSController.rwMutex.Unlock()
}
func (s *XDSController) DeleteEDSCache(key string) {
	s.EDSController.rwMutex.Lock()
	delete(s.EDSController.Resources, key)
	s.EDSController.rwMutex.Unlock()
}
func (s *XDSController) DeleteRDSCache(key string) {
	s.RDSController.rwMutex.Lock()
	delete(s.RDSController.Resources, key)
	s.RDSController.rwMutex.Unlock()
}

// nolint
func getAllTenantIDs(tenants []*LLMRouterTenantPipelineConfig) []string {
	removeTenantIDs := make([]string, 0, len(tenants))
	for _, tenant := range tenants {
		removeTenantIDs = append(removeTenantIDs, tenant.TenantId)
	}
	return removeTenantIDs
}
func (s *XDSController) DeleteTDSCache(uid string) {
	s.TDSController.rwMutex.Lock()
	delete(s.TDSController.Resources, uid)
	s.TDSController.rwMutex.Unlock()
}
func (s *XDSController) DeleteAllXDSs() bool {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	for key := range s.watched {
		delete(s.watched, key)
		s.DeleteCDSCache(key)
		s.DeleteEDSCache(key)
		s.DeleteRDSCache(key)
	}
	return true
}
func (s *XDSController) DeleteXDSs(serviceNames ...string) bool {
	s.watchMu.Lock()
	defer s.watchMu.Unlock()
	for _, svcName := range serviceNames {
		delete(s.watched, svcName)
		s.DeleteCDSCache(svcName)
		s.DeleteEDSCache(svcName)
		s.DeleteRDSCache(svcName)
	}
	return true
}
func CDSsTransformerToProtoMessages(cdss []*LLMRouterCluster) []proto.Message {
	resources := make([]proto.Message, 0, len(cdss))
	for _, cds := range cdss {
		resources = append(resources, cds)
	}
	return resources
}
func EDSsTransformerToProtoMessages(edss []*LLMRouterEndpointAssignment) []proto.Message {
	resources := make([]proto.Message, 0, len(edss))
	for _, eds := range edss {
		resources = append(resources, eds)
	}
	return resources
}
func RDSsTransformerToProtoMessages(rdss []*LLMRouterRouting) []proto.Message {
	resources := make([]proto.Message, 0, len(rdss))
	for _, rds := range rdss {
		resources = append(resources, rds)
	}
	return resources
}
func TDSsTransformerToProtoMessages(tdss []*LLMRouterTenantPipelineConfigs) []proto.Message {
	resources := make([]proto.Message, 0, len(tdss))
	for _, tds := range tdss {
		resources = append(resources, tds)
	}
	return resources
}
