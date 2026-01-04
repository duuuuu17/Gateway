package outbound

import (
	"bytes"
	"context"
	"control-plane-model-test/pkg/config"
	"control-plane-model-test/pkg/router/core"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// DTO
type OpenAIChatCompletionBody struct {
	Model       string          `json:"model"`
	Messages    []openAIMessage `json:"messages"`
	Stream      *bool           `json:"stream,omitempty"`
	Temperature *float64        `json:"temperature,omitempty"`
}
type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func buildOpenAIBody(req *core.LLMRequest) (*OpenAIChatCompletionBody, error) {
	msgs := make([]openAIMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		msgs = append(msgs, openAIMessage{
			Role:    m.Role,
			Content: m.Role,
		})
	}
	body := &OpenAIChatCompletionBody{
		Model:    req.Model,
		Stream:   req.Stream,
		Messages: msgs,
	}
	if v, ok := req.Parameters["temperature"].(float64); ok {
		body.Temperature = &v
	}
	return body, nil
}

type OpenAIOutBoundAdapter struct{}

func NewOpenAIOutBoundAdapter() OutboundAdapter {
	return &OpenAIOutBoundAdapter{}
}
func (od *OpenAIOutBoundAdapter) BuildHTTPRequest(ctx context.Context, req *core.LLMRequest, cfg config.ConfigReader) (*http.Request, error) {

	// Got backend uri
	useURI := core.CanaryPickOne(cfg.GetConfig()) + "/openai/v1/chat/completions"
	// got openai protocol request body
	openAIbody, err := buildOpenAIBody(req)
	if err != nil {
		return nil, err
	}
	forwardBodyBytes, err := json.Marshal(openAIbody)
	if err != nil {
		return nil, fmt.Errorf("can't mrashal the json bytes!")
	}
	// NOTE: here using ctx from client request. And after occure interruption, the connection that route connect to Pod can be cancel
	forwardReq, err := http.NewRequestWithContext(ctx, http.MethodPost, useURI, bytes.NewReader(forwardBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("can't create openai request!")
	}
	// Headers content-type specify body format
	req.Headers.Set("Content-Type", "application/json")
	// header passthrough
	for k, v := range req.Headers {
		req.Headers[k] = v
	}
	// stream way need handler
	if req.Stream != nil && *req.Stream {
		req.Headers.Set("Accept", "text/event-stream")
	}

	return forwardReq, nil
}
func (od *OpenAIOutBoundAdapter) HandleResponse(ctx context.Context, w http.ResponseWriter, resp *http.Response) {
	defer resp.Body.Close()
	// write header to response client
	for k, v := range resp.Header {
		if strings.EqualFold(k, "Transfer-Encoding") {
			continue
		}
		if strings.EqualFold(k, "Connection") {
			continue
		}
		if strings.EqualFold(k, "Keep-Alive") {
			continue
		}
		w.Header()[k] = v
	}
	// return status code
	w.WriteHeader(resp.StatusCode)
	if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupport!", 500)
			return
		}
		buf := make([]byte, 4096)
		for {
			// avert client request interruptions,need using the request context
			select {
			case <-ctx.Done():
				return
			default:
			}
			n, err := resp.Body.Read(buf) // 读取body
			if n > 0 {
				_, writeErr := w.Write(buf[:n])
				if writeErr != nil {
					fmt.Println("write stream got error: ", writeErr.Error())
					break
				}
				flusher.Flush() // 写完立即刷新缓冲区
			}
			if err != nil {
				if err != io.EOF {
					fmt.Println("streamng got err: ", err.Error())
				}
				break
			}
		}
		return
	}
	io.Copy(w, resp.Body)
}
