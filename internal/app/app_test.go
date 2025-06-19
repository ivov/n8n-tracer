package app_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ivov/n8n-tracer/internal/app"
	"github.com/ivov/n8n-tracer/internal/config"
	"github.com/ivov/n8n-tracer/internal/harness"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestConfig(t *testing.T) config.Config {
	t.Helper()
	tempDir := t.TempDir()

	return config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    filepath.Join(tempDir, "n8nEventLog.log"),
			StateFilePath:    filepath.Join(tempDir, "state.json"),
			DebounceDuration: 50 * time.Millisecond,
		},
		N8N: config.N8NConfig{
			DeploymentMode: "regular",
			Version:        "test",
		},
		Exporter: config.ExporterConfig{
			Endpoint: "http://localhost:4318",
		},
		Health: config.HealthConfig{
			Port:               "0", // Use a random free port
			StaleSpanThreshold: 1 * time.Minute,
			SpanGCInterval:     1 * time.Minute,
		},
	}
}

func TestApp_New_Success(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	cfg := setupTestConfig(t)
	application, err := app.New(cfg, h.Tracer)

	require.NoError(t, err)
	require.NotNil(t, application)
}

func TestApp_New_StateManagerFailure(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	cfg := setupTestConfig(t)
	readOnlyDir := t.TempDir()
	err := os.Chmod(readOnlyDir, 0444)
	require.NoError(t, err)
	defer func() {
		err := os.Chmod(readOnlyDir, 0755)
		require.NoError(t, err, "failed to restore permissions on cleanup")
	}()

	cfg.LogfileIngestor.StateFilePath = filepath.Join(readOnlyDir, "state.json")

	application, err := app.New(cfg, h.Tracer)

	require.Error(t, err)
	assert.Nil(t, application)
	assert.Contains(t, err.Error(), "failed to initialize state manager")
}

func TestApp_Run_ProcessesEventAndShutsDown(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	cfg := setupTestConfig(t)

	logContent := `{"ts":"2025-06-09T20:01:00.000Z","eventName":"n8n.workflow.started","payload":{"executionId":"exec-test-run","workflowId":"wf-test-run","hostId":"host-test"}}` + "\n"
	err := os.WriteFile(cfg.LogfileIngestor.WatchFilePath, []byte(logContent), 0600)
	require.NoError(t, err)

	application, err := app.New(cfg, h.Tracer)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		runErr := application.Run(ctx)
		assert.NoError(t, runErr, "app.Run should return no error on graceful shutdown")
	}()

	require.Eventually(t, func() bool {
		return h.Tracer.ExecutionStatesInMemory() == 1
	}, 2*time.Second, 100*time.Millisecond, "expected one span to be created in tracer memory")

	cancel()
	wg.Wait()

	// The unterminated span will not be in the "ended" list, which is correct behavior
	endedSpans := h.SpanRecorder.Ended()
	assert.Empty(t, endedSpans, "No spans should be ended yet as the workflow did not complete")
}
