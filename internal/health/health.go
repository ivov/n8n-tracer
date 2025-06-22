package health

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

const (
	healthCheckPath = "/health"
	readTimeout     = 1 * time.Second
	writeTimeout    = 1 * time.Second
)

type MetricsProvider interface {
	GetMetrics() (eventsProcessed int64, lastEventAt time.Time, execsInMemory int)
}

func NewHealthCheckServer(port string, provider MetricsProvider) *http.Server {
	mux := http.NewServeMux()
	handler := makeHandler(provider)
	mux.HandleFunc(healthCheckPath, handler)

	return &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}
}

func makeHandler(provider MetricsProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		eventsProcessed, lastAt, execsInMemory := provider.GetMetrics()

		res := struct {
			Status               string `json:"status"`
			LastEventProcessedAt string `json:"last_event_processed_at,omitempty"`
			EventsProcessed      int64  `json:"events_processed_since_last_startup"`
			ExecsInMemory        int    `json:"executions_in_memory"`
		}{
			Status:          "ok",
			EventsProcessed: eventsProcessed,
			ExecsInMemory:   execsInMemory,
		}

		if !lastAt.IsZero() {
			res.LastEventProcessedAt = lastAt.Format(time.RFC3339)
		}

		if err := json.NewEncoder(w).Encode(res); err != nil {
			log.Printf("Failed to encode health check response: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}
