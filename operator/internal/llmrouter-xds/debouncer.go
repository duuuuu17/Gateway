package llmrouterxds

import (
	"time"
)

type Debouncer interface {
	Enqueue(event ReconcilerPushEvent)
	Events() <-chan XDSPushEvent
	Start()
	Stop()
}
type ChanDebouncer struct {
	in     chan ReconcilerPushEvent
	out    chan XDSPushEvent
	stop   chan struct{}
	window time.Duration
}

func NewChanDebouncer(window time.Duration) *ChanDebouncer {
	return &ChanDebouncer{
		in:     make(chan ReconcilerPushEvent, 100),
		out:    make(chan XDSPushEvent, 10),
		stop:   make(chan struct{}),
		window: window,
	}
}
func (d *ChanDebouncer) Events() <-chan XDSPushEvent {
	return d.out
}
func (d *ChanDebouncer) Enqueue(event ReconcilerPushEvent) {
	select {
	case d.in <- event:
	default:
		// metrics: discrad , record
	}
}

func (d *ChanDebouncer) Start() {
	go func() {
		ticker := time.NewTicker(d.window)
		defer ticker.Stop()
		needUpdate := make(map[XDSType]map[string]struct{})
		needRemove := make(map[XDSType]map[string]struct{})
		for {
			select {
			case ev := <-d.in:
				if _, ok := needUpdate[ev.Type]; !ok {
					needUpdate[ev.Type] = make(map[string]struct{})
				}
				if _, ok := needRemove[ev.Type]; !ok {
					needRemove[ev.Type] = make(map[string]struct{})
				}
				// 先组合删除
				for _, es := range ev.RemoveDService {
					delete(needUpdate[ev.Type], es)
					needRemove[ev.Type][es] = struct{}{}
				}
				// 后组合更新
				for _, es := range ev.AffectedServices {
					delete(needRemove[ev.Type], es)
					needUpdate[ev.Type][es] = struct{}{}
				}
			case <-ticker.C:
				d.push(needUpdate, needRemove)
				needUpdate = make(map[XDSType]map[string]struct{})
				needRemove = make(map[XDSType]map[string]struct{})
			case <-d.stop:
				return
			}
		}
	}()
}

func (d *ChanDebouncer) Stop() {
	close(d.stop)
}
func (d *ChanDebouncer) push(needUpdate map[XDSType]map[string]struct{}, needRemove map[XDSType]map[string]struct{}) {
	for typ := range needUpdate {
		upd := needUpdate[typ]
		rem := needRemove[typ]

		updates := setToKeysSlice(upd)
		removes := setToKeysSlice(rem)
		d.out <- XDSPushEvent{
			Type:             XDSType(typ),
			AffectedServices: updates,
			RemoveDService:   removes,
			Full:             false, // delta push
		}
	}
}
