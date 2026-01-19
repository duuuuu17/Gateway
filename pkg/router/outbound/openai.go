package outbound

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/metrics"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	if req.Prompt == "" && len(req.Messages) == 0 {
		return nil, core.ErrInvalidRequest
	}
	msgs := make([]openAIMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		msgs = append(msgs, openAIMessage{
			Role:    m.Role,
			Content: m.Content,
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
func (od *OpenAIOutBoundAdapter) BuildHTTPRequest(ctx context.Context, req *core.LLMRequest, cfg *config.RuntimeBackend) (*http.Request, error) {

	// Got backend uri
	e, err := cfg.EndpointSelector.Select(cfg.Capabilty.Endpoints)
	if err != nil {
		return nil, err
	}
	useURI := e.Address + "/openai/v1/chat/completions"
	// got openai protocol request body
	openAIbody, err := buildOpenAIBody(req)
	if err != nil {
		return nil, err
	}
	forwardBodyBytes, err := json.Marshal(openAIbody)
	if err != nil {
		return nil, err
	}
	// NOTE: here using ctx from client request. And after occure interruption, the connection that route connect to Pod can be cancel
	forwardReq, err := http.NewRequestWithContext(ctx, http.MethodPost, useURI, bytes.NewReader(forwardBodyBytes))
	if err != nil {
		return nil, err
	}
	// Headers content-type specify body format
	forwardReq.Header.Set("Content-Type", "application/json")
	// header passthrough
	for k, v := range req.Headers {
		forwardReq.Header[k] = v
	}
	// stream way need handler
	if req.Stream != nil && *req.Stream {
		forwardReq.Header.Set("Accept", "text/event-stream")
	}

	return forwardReq, nil
}

func (od *OpenAIOutBoundAdapter) HandleResponse(ctx context.Context, w http.ResponseWriter, resp *http.Response) error {
	select {
	case <-ctx.Done():
		return core.ErrClientCancel
	default:
	}
	ctx, span := otel.Tracer("outbound").Start(ctx, "outbound.handleResponse", trace.WithAttributes(attribute.Int("statusCode", resp.StatusCode)))
	defer span.End()

	if resp.StatusCode >= 500 {
		http.Error(w, "the model can't handle, waitting minutes!", 500)
		metrics.BackendErrorsTotal.WithLabelValues(ctx.Value("x-model").(string), resp.Status).Inc()

		return core.ErrBackend5xx
	}
	if resp.StatusCode >= 400 {
		http.Error(w, "the model can't handle, waitting minutes!", 400)
		metrics.BackendErrorsTotal.WithLabelValues(ctx.Value("x-model").(string), resp.Status).Inc()
		return core.ErrBackend4xx
	}
	defer resp.Body.Close()
	// write header to response client
	for k, v := range resp.Header {
		switch strings.ToLower(k) {
		case "transfer-encoding", "connection", "keep-alive":
			continue
		default:
			w.Header()[k] = v
		}
	}
	w.WriteHeader(resp.StatusCode)
	if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		metrics.LLMStreamActiveConnections.WithLabelValues(resp.Request.Host, ctx.Value("x-model").(string)).Inc()
		defer metrics.LLMStreamActiveConnections.WithLabelValues(resp.Request.Host, ctx.Value("x-model").(string)).Dec()
		span.AddEvent("streaming response")
		return od.streamResponse(ctx, w, resp)
	}
	// return status code
	_, err := io.Copy(w, resp.Body)
	return err
}

func (od *OpenAIOutBoundAdapter) streamResponse(ctx context.Context, w http.ResponseWriter, resp *http.Response) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return core.ErrStreamUnsupport
	}
	buf := make([]byte, 4096)
	for {
		// avert client request interruptions,need using the request context
		select {
		case <-ctx.Done():
			return core.ErrClientCancel
		default:
		}
		n, err := resp.Body.Read(buf) // 读取body
		if n > 0 {
			metrics.LLMStreamChunksTotal.WithLabelValues(resp.Request.Host, ctx.Value("x-model").(string)).Inc() // 添加指标
			_, writeErr := w.Write(buf[:n])
			if writeErr != nil {
				slog.Warn("write stream got error: ", "err", writeErr)
				break
			}
			flusher.Flush() // 写完立即刷新缓冲区
		}
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
	}
	return nil
}
