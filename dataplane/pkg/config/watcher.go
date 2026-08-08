package config

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/fsnotify/fsnotify"
)

func Initialization(ctx context.Context, path string) (*MultiConfigStore, error) {
	yamlLoader := NewMultiBackendLoader(path)
	routerConfig, err := yamlLoader.Load()
	if err != nil {
		return nil, err
	}
	storage := NewMultiConfigStore(routerConfig)
	go WatchConfig(ctx, path, storage, yamlLoader)

	return storage, nil
}

// 采用Hash ConfigMapFile方法
func WatchConfig(ctx context.Context, filepath string, p *MultiConfigStore, load ConfigLoader) {
	watcher, _ := fsnotify.NewWatcher()
	if err := watcher.Add(filepath); err != nil {
		slog.Warn("watch file failure")
		return
	}
	var lastTIme time.Time
	const fixedDelayTime = time.Millisecond * 500
	slog.Info("YAML file watcher 已启动")
	for {
		select {
		case <-ctx.Done():
			slog.Info("YAML file watcher 已停止监听")
			return
		case events := <-watcher.Events:
			if events.Op&fsnotify.Write == fsnotify.Write {
				if time.Since(lastTIme) > fixedDelayTime {
					lastTIme = time.Now()
					cfgs, err := load.Load()
					if err == nil {
						p.update(cfgs)
						// todo log print
						// fmt.Printf("%+v", p.GetConfig())
						log.Println("config reloaded")
					}
				}
			}
		}
	}
}
