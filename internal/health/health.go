package health

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"
)

const (
	healthCheckPath = "/health"
	readTimeout     = 1 * time.Second
	writeTimeout    = 1 * time.Second
)

// InitHealthCheckServer creates and starts the tracer's health check server.
func InitHealthCheckServer(port string) *http.Server {
	srv := newHealthCheckServer(port)
	go func() {
		log.Printf("Starting health check server at port %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errMsg := "Health check server failed to start"
			if opErr, ok := err.(*net.OpError); ok && opErr.Op == "listen" {
				errMsg = fmt.Sprintf("%s: Port %s is already in use", errMsg, srv.Addr)
			} else {
				errMsg = fmt.Sprintf("%s: %s", errMsg, err)
			}
			log.Print(errMsg)
			return
		}
	}()

	return srv
}

func newHealthCheckServer(port string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc(healthCheckPath, handleHealthCheck)

	return &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      mux,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}
}

func handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	res := struct {
		Status string `json:"status"`
	}{Status: "ok"}

	if err := json.NewEncoder(w).Encode(res); err != nil {
		log.Printf("Failed to encode health check response: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
