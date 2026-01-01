package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// var (
//
//	primaryBackend   = os.Getenv("PRIMARY_BACKEND")
//	secondaryBackend = os.Getenv("SECONDARY_BACKEND")
//	canaryRatio      = os.Getenv("CANARU_RATIO")
//	enabledStreaming = os.Getenv("ENABLE_STREAMING") == "true"
//	uriSuffix        = "/openai/v1/chat/completions"
//
// )
var (
	uriSuffix        = "/openai/v1/chat/completions"
	primaryBackend   = "http://172.18.0.4:32337/"
	secondaryBackend = "http://172.18.0.4:32337/"
	canaryRatio      = "0.5"
	enabledStreaming = true //false
)

func checkEnvs() bool {
	if primaryBackend == "" {
		fmt.Fprint(os.Stderr, "Env PRIMARY_BACKEND is null!\n And checks others env variables!")
		return false
	}
	if secondaryBackend == "" {
		fmt.Fprint(os.Stderr, "Env SECONDARY_BACKEND is null!\n And checks others env variables!")
		return false
	}
	if canaryRatio == "" {
		fmt.Fprint(os.Stderr, "Env CANARU_RATIO is null!\n And checks others env variables!")
		return false
	}
	return true
}

func pickBackend(r *rand.Rand) string {
	ratio, err := strconv.ParseFloat(canaryRatio, 32)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Parse ratio to float error: %w", err)
	}
	if r.Float64() < ratio {
		fmt.Println("choose secondary model!")
		return secondaryBackend
	}
	fmt.Println("choose primary model!")
	return primaryBackend
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
	http.HandleFunc("/openai/v1/chat/completions", chatHandler)
	log.Println("LLM Router listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
