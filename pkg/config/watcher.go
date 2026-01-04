package config

import (
	"context"
	"log"
	"time"

	"github.com/fsnotify/fsnotify"
)

func Initialization(path string) (*AtomicConfigStore, error) {
	yamlLoader := NewYAMLLoader(path)
	routerConfig, err := yamlLoader.Load()
	if err != nil {
		return nil, err
	}
	return NewAtomicConfigStore(routerConfig), nil

}
func WatchConfig(ctx context.Context, filepath string, p *AtomicConfigStore, load ConfigLoader) {
	watcher, _ := fsnotify.NewWatcher()
	watcher.Add(filepath)
	var lastTIme time.Time
	const fixedDelayTime = time.Millisecond * 500
	for {
		select {
		case <-ctx.Done():
			return
		case events := <-watcher.Events:
			if events.Op&fsnotify.Write == fsnotify.Write {
				if time.Since(lastTIme) > fixedDelayTime {
					lastTIme = time.Now()
					cfg, err := load.Load()
					if err == nil {
						p.update(cfg)
						// todo log print
						// fmt.Printf("%+v", p.GetConfig())
						log.Println("config reloaded")
					}
				}
			}

		}
	}
}
