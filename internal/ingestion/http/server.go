// internal/ingestion/http/server.go
package http

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/ivov/n8n-tracer/internal/config"
	"github.com/ivov/n8n-tracer/internal/core"
)

type HTTPIngestorServer struct {
	server    *http.Server
	parser    *core.Parser
	eventCh   chan interface{}
	errCh     chan error
	stopCh    chan struct{}
	stopOnce  sync.Once
	closeOnce sync.Once
}

func NewHTTPIngestorServer(cfg config.HTTPIngestorConfig) *HTTPIngestorServer {
	server := &HTTPIngestorServer{
		parser:  core.NewParser(),
		eventCh: make(chan interface{}, 100),
		errCh:   make(chan error, 10),
		stopCh:  make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", server.handleIngest)

	server.server = &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return server
}

func (s *HTTPIngestorServer) Start(ctx context.Context) (<-chan interface{}, <-chan error) {
	go func() {
		log.Printf("Starting HTTP ingestion server on port %s", s.server.Addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.errCh <- fmt.Errorf("HTTP ingestion server failed: %w", err)
		}
	}()

	go func() {
		<-ctx.Done()
		s.Stop()
	}()

	return s.eventCh, s.errCh
}

func (s *HTTPIngestorServer) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.server.Shutdown(ctx); err != nil {
			log.Printf("HTTP ingestion server shutdown error: %v", err)
		}

		s.closeOnce.Do(func() {
			close(s.eventCh)
			close(s.errCh)
		})
	})
}

func (s *HTTPIngestorServer) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	event, err := s.parser.ToEvent(body)
	if err != nil {
		http.Error(w, "Invalid event format", http.StatusBadRequest)
		return
	}

	if event != nil {
		select {
		case s.eventCh <- event:
			w.WriteHeader(http.StatusOK)
		case <-s.stopCh:
			http.Error(w, "Server shutting down", http.StatusServiceUnavailable)
		default:
			http.Error(w, "Event channel full", http.StatusServiceUnavailable)
		}
	} else {
		w.WriteHeader(http.StatusOK)
	}
}
