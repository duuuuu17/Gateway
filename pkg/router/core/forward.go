package core

import (
	"fmt"
	"net/http"
	"time"
)

type Forward interface {
	Do(*http.Request) (*http.Response, error)
}
type HTTPForward struct {
	http.Client
}

func NewHTTPForward() *HTTPForward {
	// 传输层配置信息，提高复用
	transport := http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // 当没有使用gzip时，阻止自动压缩
	}
	return &HTTPForward{
		Client: http.Client{
			Transport: &transport,
			Timeout:   0},
	}
}
func (hf *HTTPForward) Do(req *http.Request) (*http.Response, error) {
	// todo: using ctx print log/span
	return hf.Client.Do(req)
}

func (hf *HTTPForward) TestDo(req *http.Request) (*http.Response, error) {
	// todo: using ctx print log/span
	fmt.Printf("HTTP Rquest:%+v ", req)
	return nil, nil
}
