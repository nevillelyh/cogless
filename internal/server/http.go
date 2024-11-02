package server

import (
	"net/http"
)

func NewServer(addr string, runner *Runner) *http.Server {
	handler := Handler{runner: runner}
	serveMux := http.NewServeMux()
	serveMux.HandleFunc("/", handler.Root)
	serveMux.HandleFunc("/health-check", handler.HealthCheck)
	serveMux.HandleFunc("/openapi.json", handler.OpenApi)
	// POST /predictions
	serveMux.HandleFunc("/predictions", handler.Predict)
	// POST /predictions       Prefer: respond-async
	// PUT  /predictions/<pid>
	// PUT  /predictions/<pid> Prefer: respond-async
	// POST /predictions/<pid>/cancel
	// POST /shutdown
	serveMux.HandleFunc("/shutdown", handler.Shutdown)

	return &http.Server{
		Addr:    addr,
		Handler: serveMux,
	}
}
