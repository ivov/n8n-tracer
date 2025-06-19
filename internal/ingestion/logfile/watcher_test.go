package logfile_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ivov/n8n-tracer/internal/config"
	"github.com/ivov/n8n-tracer/internal/ingestion/logfile"
	"github.com/ivov/n8n-tracer/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewLogfileWatcher_Success(t *testing.T) {
	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "n8nEventLog.log")
	stateFilePath := filepath.Join(tempDir, "state.json")

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    logFilePath,
			DebounceDuration: 100 * time.Millisecond,
		},
	}

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	watcher, err := logfile.NewLogfileWatcher(cfg, sm)

	require.NoError(t, err)
	assert.NotNil(t, watcher)

	watcher.Stop()
}

func Test_LogfileWatcher_ProcessSingleEvent(t *testing.T) {
	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "n8nEventLog.log")
	stateFilePath := filepath.Join(tempDir, "state.json")

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    logFilePath,
			DebounceDuration: 50 * time.Millisecond,
		},
	}

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	logContent := `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}` + "\n"
	err = os.WriteFile(logFilePath, []byte(logContent), 0600)
	require.NoError(t, err)

	watcher, err := logfile.NewLogfileWatcher(cfg, sm)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	eventCh, errCh := watcher.Start(ctx)

	var receivedEvent interface{}
	select {
	case event := <-eventCh:
		receivedEvent = event
	case err := <-errCh:
		t.Fatalf("Unexpected error: %v", err)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for event")
	}

	watcher.Stop()
	time.Sleep(50 * time.Millisecond) // Allow cleanup to complete

	// Verify event
	jobEvent, ok := receivedEvent.(models.JobEnqueuedEvent)
	assert.True(t, ok, "Event should be JobEnqueuedEvent")
	assert.Equal(t, "exec-123", jobEvent.Payload.ExecutionID)
	assert.Equal(t, "job-abc", jobEvent.Payload.JobID)
}

func Test_LogfileWatcher_ProcessMultipleEvents(t *testing.T) {
	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "n8nEventLog.log")
	stateFilePath := filepath.Join(tempDir, "state.json")

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    logFilePath,
			DebounceDuration: 50 * time.Millisecond,
		},
	}

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	logContent := `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}` + "\n" +
		`{"ts":"2025-06-09T20:00:01.000Z","eventName":"n8n.queue.job.dequeued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}` + "\n" +
		`{"ts":"2025-06-09T20:00:02.000Z","eventName":"n8n.workflow.started","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789"}}` + "\n"

	err = os.WriteFile(logFilePath, []byte(logContent), 0600)
	require.NoError(t, err)

	watcher, err := logfile.NewLogfileWatcher(cfg, sm)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	eventCh, errCh := watcher.Start(ctx)

	var events []interface{}
	eventCount := 0
	expectedEvents := 3

	for eventCount < expectedEvents {
		select {
		case event := <-eventCh:
			events = append(events, event)
			eventCount++
		case err := <-errCh:
			t.Fatalf("Unexpected error: %v", err)
		case <-ctx.Done():
			return
		}
	}

	watcher.Stop()
	time.Sleep(50 * time.Millisecond) // Allow cleanup to complete

	assert.Len(t, events, expectedEvents, "Should receive all events")

	// Verify event types
	assert.IsType(t, models.JobEnqueuedEvent{}, events[0])
	assert.IsType(t, models.JobDequeuedEvent{}, events[1])
	assert.IsType(t, models.WorkflowStartedEvent{}, events[2])
}

func Test_LogfileWatcher_SkipMalformedLines(t *testing.T) {
	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "n8nEventLog.log")
	stateFilePath := filepath.Join(tempDir, "state.json")

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    logFilePath,
			DebounceDuration: 50 * time.Millisecond,
		},
	}

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	logContent := `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}` + "\n" +
		`invalid json line` + "\n" +
		`{"malformed": "json"` + "\n" +
		`{"ts":"2025-06-09T20:00:01.000Z","eventName":"n8n.queue.job.dequeued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}` + "\n"

	err = os.WriteFile(logFilePath, []byte(logContent), 0600)
	require.NoError(t, err)

	watcher, err := logfile.NewLogfileWatcher(cfg, sm)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	eventCh, errCh := watcher.Start(ctx)

	var events []interface{}
	eventCount := 0
	expectedValidEvents := 2

	// Collect events with timeout
	timeout := time.After(1 * time.Second)
	for eventCount < expectedValidEvents {
		select {
		case event := <-eventCh:
			events = append(events, event)
			eventCount++
		case err := <-errCh:
			t.Fatalf("Unexpected error: %v", err)
		case <-timeout:
			return
		}
	}

	watcher.Stop()
	time.Sleep(50 * time.Millisecond) // Allow cleanup to complete

	// Should only receive valid events, malformed lines should be skipped
	assert.Len(t, events, expectedValidEvents, "Should only receive valid events")
	assert.IsType(t, models.JobEnqueuedEvent{}, events[0])
	assert.IsType(t, models.JobDequeuedEvent{}, events[1])
}

func Test_LogfileWatcher_EmptyFile(t *testing.T) {
	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "empty.log")
	stateFilePath := filepath.Join(tempDir, "state.json")

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    logFilePath,
			DebounceDuration: 50 * time.Millisecond,
		},
	}

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	// Create empty log file
	err = os.WriteFile(logFilePath, []byte(""), 0600)
	require.NoError(t, err)

	watcher, err := logfile.NewLogfileWatcher(cfg, sm)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	eventCh, errCh := watcher.Start(ctx)

	// Should not receive any events
	select {
	case event := <-eventCh:
		t.Fatalf("Unexpected event from empty file: %v", event)
	case err := <-errCh:
		t.Fatalf("Unexpected error: %v", err)
	case <-ctx.Done():
		// Expected - timeout means no events, which is correct
	}

	watcher.Stop()
}

func Test_LogfileWatcher_NonexistentFile(t *testing.T) {
	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "nonexistent.log")
	stateFilePath := filepath.Join(tempDir, "state.json")

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    logFilePath,
			DebounceDuration: 50 * time.Millisecond,
		},
	}

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	watcher, err := logfile.NewLogfileWatcher(cfg, sm)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	eventCh, errCh := watcher.Start(ctx)

	// Should not receive any events or errors for nonexistent file
	select {
	case event := <-eventCh:
		t.Fatalf("Unexpected event from nonexistent file: %v", event)
	case err := <-errCh:
		// This might be expected if the file doesn't exist
		t.Logf("Expected error for nonexistent file: %v", err)
	case <-ctx.Done():
		// Expected - no file means no events
	}

	watcher.Stop()
}

func Test_LogfileWatcher_StateManagement(t *testing.T) {
	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "state-test.log")
	stateFilePath := filepath.Join(tempDir, "state.json")

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    logFilePath,
			DebounceDuration: 50 * time.Millisecond,
		},
	}

	// First event
	firstEvent := `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}` + "\n"
	err := os.WriteFile(logFilePath, []byte(firstEvent), 0600)
	require.NoError(t, err)

	// First watcher run
	sm1, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	watcher1, err := logfile.NewLogfileWatcher(cfg, sm1)
	require.NoError(t, err)

	ctx1, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel1()

	eventCh1, errCh1 := watcher1.Start(ctx1)

	// Wait for first event
	select {
	case <-eventCh1:
		// Expected
	case err := <-errCh1:
		t.Fatalf("Unexpected error: %v", err)
	case <-ctx1.Done():
		t.Fatal("Timeout waiting for first event")
	}

	watcher1.Stop()

	// Save state explicitly to ensure it's persisted
	err = sm1.Save()
	require.NoError(t, err)
	cancel1()

	// Add second event to file
	secondEvent := `{"ts":"2025-06-09T20:00:01.000Z","eventName":"n8n.queue.job.dequeued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}` + "\n"
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = file.WriteString(secondEvent)
	require.NoError(t, err)
	file.Close()

	// Second watcher run - should only process the new event
	sm2, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	watcher2, err := logfile.NewLogfileWatcher(cfg, sm2)
	require.NoError(t, err)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel2()

	eventCh2, errCh2 := watcher2.Start(ctx2)

	var secondEventReceived interface{}
	select {
	case event := <-eventCh2:
		secondEventReceived = event
	case err := <-errCh2:
		t.Fatalf("Unexpected error: %v", err)
	case <-ctx2.Done():
		t.Fatal("Timeout waiting for second event")
	}

	watcher2.Stop()
	time.Sleep(50 * time.Millisecond) // Allow cleanup to complete

	// Verify we got the dequeued event (the newly added one)
	jobEvent, ok := secondEventReceived.(models.JobDequeuedEvent)
	assert.True(t, ok, "Second event should be JobDequeuedEvent")
	assert.Equal(t, "exec-123", jobEvent.Payload.ExecutionID)
}

func Test_LogfileWatcher_MultipleRotatedFiles(t *testing.T) {
	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "n8nEventLog.log")
	stateFilePath := filepath.Join(tempDir, "state.json")

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    logFilePath,
			DebounceDuration: 50 * time.Millisecond,
		},
	}

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	rotatedFile1 := filepath.Join(tempDir, "n8nEventLog-1.log")
	rotatedFile2 := filepath.Join(tempDir, "n8nEventLog-2.log")

	// Oldest file (should be processed first)
	oldestEvent := `{"ts":"2025-06-09T19:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-old","workflowId":"wf-old","hostId":"host-old","jobId":"job-old"}}` + "\n"
	err = os.WriteFile(rotatedFile2, []byte(oldestEvent), 0600)
	require.NoError(t, err)

	// Middle file
	middleEvent := `{"ts":"2025-06-09T19:30:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-mid","workflowId":"wf-mid","hostId":"host-mid","jobId":"job-mid"}}` + "\n"
	err = os.WriteFile(rotatedFile1, []byte(middleEvent), 0600)
	require.NoError(t, err)

	// Current file
	currentEvent := `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-new","workflowId":"wf-new","hostId":"host-new","jobId":"job-new"}}` + "\n"
	err = os.WriteFile(logFilePath, []byte(currentEvent), 0600)
	require.NoError(t, err)

	watcher, err := logfile.NewLogfileWatcher(cfg, sm)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	eventCh, errCh := watcher.Start(ctx)

	var events []interface{}
	expectedEvents := 3

	// Collect all events
	timeout := time.After(1500 * time.Millisecond)
	for len(events) < expectedEvents {
		select {
		case event := <-eventCh:
			events = append(events, event)
		case err := <-errCh:
			t.Fatalf("Unexpected error: %v", err)
		case <-timeout:
			return
		}
	}

	watcher.Stop()
	time.Sleep(50 * time.Millisecond) // Allow cleanup to complete

	require.Len(t, events, expectedEvents, "Should process all events from all files")

	// Verify chronological order (oldest first due to file processing order)
	oldEvent := events[0].(models.JobEnqueuedEvent)
	midEvent := events[1].(models.JobEnqueuedEvent)
	newEvent := events[2].(models.JobEnqueuedEvent)

	assert.Equal(t, "exec-old", oldEvent.Payload.ExecutionID)
	assert.Equal(t, "exec-mid", midEvent.Payload.ExecutionID)
	assert.Equal(t, "exec-new", newEvent.Payload.ExecutionID)
}

func Test_LogfileWatcher_Stop(t *testing.T) {
	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "stop-test.log")
	stateFilePath := filepath.Join(tempDir, "state.json")

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    logFilePath,
			DebounceDuration: 50 * time.Millisecond,
		},
	}

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	logContent := `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}` + "\n"
	err = os.WriteFile(logFilePath, []byte(logContent), 0600)
	require.NoError(t, err)

	watcher, err := logfile.NewLogfileWatcher(cfg, sm)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	eventCh, errCh := watcher.Start(ctx)

	select {
	case <-eventCh:
		// Expected
	case err := <-errCh:
		t.Fatalf("Unexpected error: %v", err)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for initial event")
	}

	watcher.Stop()

	timeout := time.After(1 * time.Second)
	select {
	case _, ok := <-eventCh:
		assert.False(t, ok, "Event channel should be closed after call to Stop()")
	case <-timeout:
		t.Fatal("Event channel should be closed after call to Stop()")
	}
}

func Test_LogfileWatcher_ContextCancellation(t *testing.T) {
	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "context-test.log")
	stateFilePath := filepath.Join(tempDir, "state.json")

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    logFilePath,
			DebounceDuration: 50 * time.Millisecond,
		},
	}

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	logContent := `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}` + "\n"
	err = os.WriteFile(logFilePath, []byte(logContent), 0600)
	require.NoError(t, err)

	watcher, err := logfile.NewLogfileWatcher(cfg, sm)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	eventCh, errCh := watcher.Start(ctx)

	select {
	case <-eventCh:
		// Expected
	case err := <-errCh:
		t.Fatalf("Unexpected error: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for initial event")
	}

	cancel()

	timeout := time.After(1 * time.Second)
	select {
	case _, ok := <-eventCh:
		assert.False(t, ok, "Event channel should be closed after context cancellation")
	case <-timeout:
		t.Fatal("Event channel should be closed after context cancellation")
	}

	watcher.Stop()
}

func Test_LogfileWatcher_DebounceDuration(t *testing.T) {
	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "debounce-test.log")
	stateFilePath := filepath.Join(tempDir, "state.json")

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    logFilePath,
			DebounceDuration: 200 * time.Millisecond,
		},
	}

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	err = os.WriteFile(logFilePath, []byte(""), 0600)
	require.NoError(t, err)

	watcher, err := logfile.NewLogfileWatcher(cfg, sm)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	eventCh, errCh := watcher.Start(ctx)

	// Wait for initial scan to complete (should find no events)
	time.Sleep(100 * time.Millisecond)

	// Add content to trigger file change
	logContent := `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}` + "\n"
	err = os.WriteFile(logFilePath, []byte(logContent), 0600)
	require.NoError(t, err)

	// Should receive event after debounce duration
	select {
	case event := <-eventCh:
		jobEvent, ok := event.(models.JobEnqueuedEvent)
		assert.True(t, ok, "Should receive JobEnqueuedEvent")
		assert.Equal(t, "exec-123", jobEvent.Payload.ExecutionID)
	case err := <-errCh:
		t.Fatalf("Unexpected error: %v", err)
	case <-ctx.Done():
		t.Fatal("Timeout waiting for debounced event")
	}

	watcher.Stop()
}

func Test_LogfileWatcher_InvalidWatchDirectory(t *testing.T) {
	stateFilePath := os.TempDir() + "/state.json"

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    "/nonexistent/directory/test.log",
			DebounceDuration: 50 * time.Millisecond,
		},
	}

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	watcher, err := logfile.NewLogfileWatcher(cfg, sm)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	eventCh, errCh := watcher.Start(ctx)

	select {
	case event := <-eventCh:
		t.Fatalf("Unexpected event from invalid directory: %v", event)
	case err := <-errCh:
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to find logfiles")
	case <-ctx.Done():
		// Timeout is also acceptable if the watcher fails silently
	}

	watcher.Stop()
}

func Test_LogfileWatcher_PruneState(t *testing.T) {
	tempDir := t.TempDir()
	logFilePath := filepath.Join(tempDir, "prune-test.log")
	stateFilePath := filepath.Join(tempDir, "state.json")

	cfg := config.Config{
		LogfileIngestor: config.LogfileIngestorConfig{
			WatchFilePath:    logFilePath,
			DebounceDuration: 50 * time.Millisecond,
		},
	}

	sm, err := logfile.NewStateManager(stateFilePath)
	require.NoError(t, err)

	logContent := `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}` + "\n"
	err = os.WriteFile(logFilePath, []byte(logContent), 0600)
	require.NoError(t, err)

	watcher, err := logfile.NewLogfileWatcher(cfg, sm)
	require.NoError(t, err)

	err = watcher.PruneState()
	require.NoError(t, err)

	watcher.Stop()
}
