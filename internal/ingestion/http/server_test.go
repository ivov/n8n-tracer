package http_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/ivov/n8n-tracer/internal/config"
	httpingestion "github.com/ivov/n8n-tracer/internal/ingestion/http"
	"github.com/ivov/n8n-tracer/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getFreePort() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer listener.Close()
	addr := listener.Addr().(*net.TCPAddr)
	return fmt.Sprintf("%d", addr.Port), nil
}

func Test_HTTPIngestionServer_SingleEvent(t *testing.T) {
	port, err := getFreePort()
	require.NoError(t, err)

	cfg := config.HTTPIngestorConfig{Port: port}
	server := httpingestion.NewHTTPIngestorServer(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventCh, errCh := server.Start(ctx)
	defer server.Stop()

	// Wait for server to start
	time.Sleep(200 * time.Millisecond)

	// Send a test request
	eventJSON := `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}`

	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%s/ingest", port), "application/json", bytes.NewBufferString(eventJSON))

	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Check if event was received
	select {
	case event := <-eventCh:
		jobEvent, ok := event.(models.JobEnqueuedEvent)
		assert.True(t, ok)
		assert.Equal(t, "exec-123", jobEvent.Payload.ExecutionID)
	case err := <-errCh:
		t.Fatalf("Unexpected error: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for event")
	}
}

func Test_HTTPIngestionServer_InvalidMethod(t *testing.T) {
	port, err := getFreePort()
	require.NoError(t, err)

	cfg := config.HTTPIngestorConfig{Port: port}
	server := httpingestion.NewHTTPIngestorServer(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = server.Start(ctx)
	defer server.Stop()

	time.Sleep(200 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/ingest", port))

	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func Test_HTTPIngestionServer_InvalidJSON(t *testing.T) {
	port, err := getFreePort()
	require.NoError(t, err)

	cfg := config.HTTPIngestorConfig{Port: port}
	server := httpingestion.NewHTTPIngestorServer(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = server.Start(ctx)
	defer server.Stop()

	time.Sleep(200 * time.Millisecond)

	resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%s/ingest", port), "application/json", bytes.NewBufferString("invalid json"))

	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func Test_HTTPIngestionServer_Stop(t *testing.T) {
	port, err := getFreePort()
	require.NoError(t, err)

	cfg := config.HTTPIngestorConfig{Port: port}
	server := httpingestion.NewHTTPIngestorServer(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	eventCh, errCh := server.Start(ctx)

	time.Sleep(200 * time.Millisecond)

	server.Stop()

	// Check channels are closed
	select {
	case _, ok := <-eventCh:
		assert.False(t, ok, "Event channel should be closed")
	case <-time.After(1 * time.Second):
		t.Fatal("Event channel should be closed")
	}

	select {
	case _, ok := <-errCh:
		assert.False(t, ok, "Error channel should be closed")
	case <-time.After(1 * time.Second):
		t.Fatal("Error channel should be closed")
	}
}
