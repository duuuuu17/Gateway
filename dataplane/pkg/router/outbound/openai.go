package outbound

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"strings"

	"github.com/duuuuu17/llm-router-operator/pkg/config"
	"github.com/duuuuu17/llm-router-operator/pkg/metrics"
	"github.com/duuuuu17/llm-router-operator/pkg/router/common"
	"github.com/duuuuu17/llm-router-operator/pkg/router/core"
	"github.com/duuuuu17/llm-router-operator/pkg/router/errs"
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
		return nil, errs.ErrInvalidRequest
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
	// slog.Info("candidates.GetEndpoints", "[candidates.GetEndpoints]", cfg.GetEndpoints())
	// Got backend uri
	e, err := cfg.EndpointSelector.Select(cfg.GetEndpoints())
	if err != nil {
		return nil, err
	}
	// address := ""
	// if e.Address == "10.244.1.24:8080" {
	// 	address = "localhost:8000"
	// }
	// useURI := "http://" + address + "/openai/v1/chat/completions"
	useURI := "http://" + e.Address + "/openai/v1/chat/completions"
	// got openai protocol request body
	// slog.Info("buildOpenAIBody")
	openAIbody, err := buildOpenAIBody(req)
	if err != nil {
		return nil, err
	}
	// slog.Info("forwardBodyBytes")
	forwardBodyBytes, err := json.Marshal(openAIbody)
	if err != nil {
		return nil, err
	}
	// slog.Info("http.NewRequestWithContext")
	// NOTE: here using ctx from client request. And after occure interruption, the connection that route connect to Pod can be cancel
	forwardReq, err := http.NewRequestWithContext(ctx, http.MethodPost, useURI, bytes.NewReader(forwardBodyBytes))
	if err != nil {
		return nil, err
	}
	// Headers content-type specify body format
	forwardReq.Header.Set("Content-Type", "application/json")
	// header passthrough
	// todo: some header need filter
	maps.Copy(forwardReq.Header, req.Headers)
	// stream way need handler
	if req.Stream != nil && *req.Stream {
		forwardReq.Header.Set("Accept", "text/event-stream")
	}

	return forwardReq, nil
}

func (od *OpenAIOutBoundAdapter) HandleResponse(ctx context.Context, w http.ResponseWriter, resp *http.Response) error {
	select {
	case <-ctx.Done():
		return context.Canceled
	default:
	}
	span := trace.SpanFromContext(ctx)
	if resp.StatusCode >= 500 {
		metrics.BackendErrorsTotal.WithLabelValues(resp.Request.Host, resp.Status).Inc()

		return errs.ErrBackendCantUse
	}
	if resp.StatusCode >= 400 {
		metrics.BackendErrorsTotal.WithLabelValues(resp.Request.Host, resp.Status).Inc()
		return errs.ErrBackendCantUse
	}
	defer resp.Body.Close() //nolint:errcheck
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
		metrics.LLMStreamActiveConnections.WithLabelValues(resp.Request.Host, common.GetModelNameRetStr(ctx)).Inc()
		defer metrics.LLMStreamActiveConnections.WithLabelValues(resp.Request.Host, common.GetModelNameRetStr(ctx)).Dec()
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
		return errs.ErrStreamUnsupport
	}
	buf := make([]byte, 4096)
	for {
		// avert client request interruptions,need using the request context
		select {
		case <-ctx.Done():
			// 此时可以获取err，然后判断错误类型并打印
			err := ctx.Err()
			if errors.Is(err, context.Canceled) {
				slog.WarnContext(ctx, "client cancel the request")
			} else if errors.Is(err, context.DeadlineExceeded) {
				slog.WarnContext(ctx, "request deadline exceeded")
			} else {
				slog.WarnContext(ctx, "client connection closed", "err", err.Error())
			}
			return errs.ErrClientCancel
		default:
		}
		n, err := resp.Body.Read(buf) // 读取body
		if n > 0 {
			metrics.LLMStreamChunksTotal.WithLabelValues(resp.Request.Host, common.GetModelNameRetStr(ctx)).Inc() // 添加指标
			_, writeErr := w.Write(buf[:n])
			if writeErr != nil {
				slog.WarnContext(ctx, "write stream got error: ", "err", writeErr)
				break
			}
			flusher.Flush() // 写完立即刷新缓冲区
		}
		if err != nil {
			if err != io.EOF {
				return err
			}
			break // io.EOF
		}
	}
	return nil
}
