package eventbus

import "sync"

type Bus struct {
	listeners map[string][]chan any // event -> [] subscriber's channel list
	mu        sync.RWMutex
}

func NewBus() *Bus {
	return &Bus{
		listeners: make(map[string][]chan any),
	}
}
func (b *Bus) Subscriber(eventType string) <-chan any {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan any, 1)
	b.listeners[eventType] = append(b.listeners[eventType], ch)
	return ch
}

func (b *Bus) Publish(eventType string, data any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.listeners[eventType] {
		select {
		case ch <- data:
		default:
			// 订阅者处理较慢，当前设计默认discard
		}
	}
}
