package server

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/replicate/go/must"

	"github.com/replicate/go/logging"
)

var logger = logging.New("cog-http-server")

type Handler struct {
	runner *Runner
}

func (h *Handler) Root(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	hc := HealthCheck{
		Status: h.runner.status.String(),
		Setup:  h.runner.setupResult,
	}

	if bs, err := json.Marshal(hc); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	} else {
		w.WriteHeader(http.StatusOK)
		must.Get(w.Write(bs))
	}
}

func (h *Handler) OpenApi(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if h.runner.schema == "" {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
		must.Get(w.Write([]byte(h.runner.schema)))
	}
}

func (h *Handler) Shutdown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := h.runner.Shutdown(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (h *Handler) Predict(w http.ResponseWriter, r *http.Request) {
	var req PredictionRequest
	if err := json.Unmarshal(must.Get(io.ReadAll(r.Body)), &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	if c, err := h.runner.predict(req); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		resp := <-c
		w.WriteHeader(http.StatusOK)
		must.Get(w.Write(must.Get(json.Marshal(resp))))
	}
}
