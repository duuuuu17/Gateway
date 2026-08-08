package llmrouterxds

import (
	"time"
)

type ReconcilerPushEvent struct {
	Type             XDSType  // xDsType
	AffectedServices []string // 受影响的ServiceName的配置
	RemoveDService   []string // 需要删除的对象
	Reason           string   // optional: CRUpdate / EndpointSliceChange
}

func NewEvent(typ, service string, deleted bool) ReconcilerPushEvent {
	ret := ReconcilerPushEvent{}
	if len(service) == 0 {
		return ReconcilerPushEvent{}
	}
	switch typ {
	case "cds":
		ret.Type = CDSType
	case "eds":
		ret.Type = EDSType
	case "rds":
		ret.Type = RDSType
	case "tds":
		ret.Type = TDSType
	default:
		return ReconcilerPushEvent{}
	}
	if !deleted {
		ret.AffectedServices = []string{service}
	} else {
		ret.RemoveDService = []string{service}
	}
	return ret
}

type XDSPushEvent struct {
	Type             XDSType  // xDsType
	AffectedServices []string // 受影响的ServiceName的配置
	RemoveDService   []string // 需要删除的对象
	Full             bool     // 是否全量推送
	Reason           string   // optional: CRUpdate / EndpointSliceChange
}

const (
	Deleted = "Deleted"
	Updated = "Updated"
)

type DirtySet map[XDSType]map[string]struct{}

func NewDirtySet() DirtySet {
	return make(DirtySet)
}
func (d DirtySet) Mark(ty XDSType, serviceName string) {
	if _, ok := d[ty]; !ok {
		d[ty] = make(map[string]struct{})
	}
	d[ty][serviceName] = struct{}{}
}

const (
	DebounceMin = 100 * time.Millisecond // 最小等待时间
	DebounceMax = 500 * time.Millisecond // 最大延迟时间(防止事件一致防抖动收集不推送)
)

// MergeXDSPushEvent merges newEvent into base (in-place)
func MergeXDSPushEvent(base *XDSPushEvent, newEvent XDSPushEvent) {
	if base == nil {
		return
	}

	upd := make(map[string]struct{})
	del := make(map[string]struct{})

	for _, s := range base.AffectedServices {
		upd[s] = struct{}{}
	}
	for _, s := range base.RemoveDService {
		del[s] = struct{}{}
	}

	// 先处理删除（删除优先）
	for _, s := range newEvent.RemoveDService {
		delete(upd, s)
		del[s] = struct{}{}
	}

	// 再处理更新
	for _, s := range newEvent.AffectedServices {
		delete(del, s)
		upd[s] = struct{}{}
	}

	base.AffectedServices = base.AffectedServices[:0]
	for s := range upd {
		base.AffectedServices = append(base.AffectedServices, s)
	}

	base.RemoveDService = base.RemoveDService[:0]
	for s := range del {
		base.RemoveDService = append(base.RemoveDService, s)
	}
	base.Full = newEvent.Full
	base.Reason = base.Reason + "|" + newEvent.Reason
}
