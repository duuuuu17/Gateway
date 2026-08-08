package metrics

import (
	"net/http"
)

func Healthz(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("ok"))
}
func Readyz(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte("readyz"))
}
