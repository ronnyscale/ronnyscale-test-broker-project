// HTTP-слой: PUT кладёт, GET забирает, timeout по желанию.
package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ronnyscale/ronnyscale-test-broker-project/testproject/internal/broker"
)

type Server struct {
	b *broker.Broker
}

func New(b *broker.Broker) http.Handler { return &Server{b: b} }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(strings.TrimSpace(r.URL.Path), "/")
	if name == "" || strings.Contains(name, "/") {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.put(w, r, name)
	case http.MethodGet:
		s.get(w, r, name)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) put(w http.ResponseWriter, r *http.Request, queue string) {
	v := r.URL.Query().Get("v")
	if v == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := s.b.Push(r.Context(), queue, v); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request, queue string) {
	var wait time.Duration
	if raw := r.URL.Query().Get("timeout"); raw != "" {
		sec, err := strconv.Atoi(raw)
		if err != nil || sec < 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		wait = time.Duration(sec) * time.Second
	}

	msg, ok, err := s.b.Pop(r.Context(), queue, wait)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// Клиент съехал или сервак гасим - ответ уже не катит.
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte(msg))
}
