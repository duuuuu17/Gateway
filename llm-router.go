package main

import (
	"context"
	"control-plane-model-test/pkg/config"
	"control-plane-model-test/pkg/router/handler"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

var (
	models           []string
	Endpoints        []string
	canaryRatio      float64
	enabledStreaming bool
	uriSuffix        = "/openai/v1/chat/completions"
)

// func init() {
// }
func serverParameters(r []config.ConfigReader) {
	models = r[0].GetConfig().Models
	Endpoints = r[0].GetConfig().Endpoints
	canaryRatio = r[0].GetConfig().CanaryRatio
	enabledStreaming = r[0].GetConfig().EnabledStreaming
}

func pickBackend(r *rand.Rand) string {

	if r.Float64() < canaryRatio {
		fmt.Println("choose secondary model!")
		return Endpoints[0]
	}
	fmt.Println("choose primary model!")
	return Endpoints[1]
}
func chatHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("got the request")
	randObj := rand.New(rand.NewSource(time.Now().UnixNano()))
	tartgetURI := pickBackend(randObj) + uriSuffix
	// create request to real model server
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		tartgetURI,
		r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	// headers passthrough
	for k, v := range r.Header {
		req.Header[k] = v
	}
	client := &http.Client{Timeout: 0} // NOTE: Streaming of the reply way can't setting timeout
	resp, err := client.Do(req)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	defer resp.Body.Close()
	// setting response headers
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	// write the response's StatusCode
	w.WriteHeader(resp.StatusCode)
	if enabledStreaming && strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", 500)
			return
		}
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				_, writeErr := w.Write(buf[:n])
				if writeErr != nil {
					fmt.Println("write stream error:", writeErr)
					break
				}
				flusher.Flush()
			}
			if err != nil {
				if err != io.EOF {
					fmt.Println("stream got error", err)
				}
				break
			}
		}
		return
	}
	io.Copy(w, resp.Body)
}

func main() {
	path := "./config.yaml"
	storage, err := config.Initialization(path)
	if err != nil {
		panic(err)
	}

	// 调用config模块进行主动初始化
	loader := config.NewYAMLLoader(path)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// 启用监听
	go config.WatchConfig(ctx, path, storage, loader)
	storages := []config.ConfigReader{storage}
	r := handler.InitialRouterModel(storages)
	// 调试代码：start
	serverParameters(r.GetConfigs())
	fmt.Printf("load backend configuration: %+v", r.GetConfigs()[0].GetConfig())
	// 调试代码：end
	http.HandleFunc("/openai/v1/chat/completions", chatHandler)
	// log.Println("LLM Router listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
