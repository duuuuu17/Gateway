package router

import (
	"net/http"
	"net/http/httptest"
)

func FakeBackendServer() *httptest.Server {

	fakeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write([]byte(`{"content":"test success"}`))
	}))

	// fakeServer := &httptest.Server{Config: &http.Server{Addr: ":8080", Handler: backendHandler}}
	return fakeServer
}
