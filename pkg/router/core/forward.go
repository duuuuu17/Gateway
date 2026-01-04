package core

import (
	"context"
	"net/http"
)

type Forward interface {
	Do(context.Context, *http.Request) (*http.Response, error)
}
type HTTPForward struct {
	http.Client
}

func (hf *HTTPForward) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	// todo: using ctx print log/span
	resp, err := hf.Client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
