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
	srv := &HTTPIngestorServer{
		parser:  core.NewParser(),
		eventCh: make(chan interface{}, 10_000), // TODO: Adjust buffer based on real-world usage
		errCh:   make(chan error, 100),
		stopCh:  make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", srv.handleIngest)

	srv.server = &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return srv
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

	if event == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	select {
	case s.eventCh <- event:
		w.WriteHeader(http.StatusOK)
	case <-time.After(30 * time.Second):
		// Time out after 30s if event channel is full - prevents indefinite blocking
		// while giving the tracer time to catch up with events backlog
		http.Error(w, "Processing timeout - too many events", http.StatusRequestTimeout)
	case <-s.stopCh:
		http.Error(w, "Server shutting down", http.StatusServiceUnavailable)
	}
}
