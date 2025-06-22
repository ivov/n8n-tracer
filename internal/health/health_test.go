package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMetricsProvider struct {
	eventsProcessed int64
	lastEventAt     time.Time
	execsInMemory   int
}

func (m *mockMetricsProvider) GetMetrics() (int64, time.Time, int) {
	return m.eventsProcessed, m.lastEventAt, m.execsInMemory
}

func TestHealthCheckHandler(t *testing.T) {
	provider := &mockMetricsProvider{
		eventsProcessed: 42,
		lastEventAt:     time.Now().UTC(),
		execsInMemory:   3,
	}

	tests := []struct {
		name           string
		method         string
		expectedStatus int
		wantBody       bool
	}{
		{
			name:           "GET request returns 200 and status ok",
			method:         http.MethodGet,
			expectedStatus: http.StatusOK,
			wantBody:       true,
		},
		{
			name:           "POST request returns 405 and status not allowed",
			method:         http.MethodPost,
			expectedStatus: http.StatusMethodNotAllowed,
			wantBody:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/healthz", nil)
			w := httptest.NewRecorder()

			handler := makeHealthCheckHandler(provider)
			handler(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code, "unexpected status code")

			if tt.wantBody {
				var response struct {
					Status               string `json:"status"`
					LastEventProcessedAt string `json:"last_event_processed_at"`
					EventsProcessed      int64  `json:"events_processed_since_last_startup"`
					ExecsInMemory        int    `json:"executions_in_memory"`
				}

				err := json.NewDecoder(w.Body).Decode(&response)
				require.NoError(t, err, "failed to decode response body")

				assert.Equal(t, "ok", response.Status, "unexpected status in response")
				assert.Equal(t, int64(42), response.EventsProcessed)
				assert.Equal(t, 3, response.ExecsInMemory)
				assert.NotEmpty(t, response.LastEventProcessedAt)
				assert.Equal(t, "application/json", w.Header().Get("Content-Type"), "unexpected Content-Type header")
			}
		})
	}
}

func TestHealthCheckHandlerEncodingError(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	mockProvider := &mockMetricsProvider{}
	failingWriter := &failingWriter{
		headers: http.Header{},
	}

	handler := makeHealthCheckHandler(mockProvider)
	handler(failingWriter, req)

	assert.Equal(t, http.StatusInternalServerError, failingWriter.statusCode,
		"unexpected status code for encoding error")
}

type failingWriter struct {
	statusCode int
	headers    http.Header
}

func (w *failingWriter) Header() http.Header {
	return w.headers
}

func (w *failingWriter) Write([]byte) (int, error) {
	return 0, fmt.Errorf("encoding error")
}

func (w *failingWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}

func TestNewHealthCheckServer(t *testing.T) {
	provider := &mockMetricsProvider{}
	server := newHealthCheckServer("5680", provider)

	require.NotNil(t, server, "server should not be nil")

	assert.Equal(t, ":5680", server.Addr, "unexpected server address")
	assert.Equal(t, readTimeout, server.ReadTimeout, "unexpected read timeout")
	assert.Equal(t, writeTimeout, server.WriteTimeout, "unexpected write timeout")
}

func TestInitHealthCheckServer(t *testing.T) {
	port := "0" // let the OS choose an available port
	provider := &mockMetricsProvider{}

	InitHealthCheckServer(port, provider)

	time.Sleep(10 * time.Millisecond)

	// The test verifies that InitHealthCheckServer does not panic
	// and returns immediately, since it starts the server in a goroutine.
}

func TestHealthCheckHandler_NoEventsProcessed(t *testing.T) {
	provider := &mockMetricsProvider{
		eventsProcessed: 0,
		lastEventAt:     time.Time{}, // zero time
		execsInMemory:   0,
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	handler := makeHealthCheckHandler(provider)
	handler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&response)
	require.NoError(t, err)

	assert.Equal(t, "ok", response["status"])
	assert.Equal(t, float64(0), response["events_processed_since_last_startup"])
	assert.Equal(t, float64(0), response["executions_in_memory"])
	assert.Nil(t, response["last_event_processed_at"], "should not include timestamp when zero")
}
