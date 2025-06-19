package spans_test

import (
	"context"
	"testing"
	"time"

	"github.com/ivov/n8n-tracer/internal/harness"
	"github.com/ivov/n8n-tracer/internal/models"
	"github.com/ivov/n8n-tracer/internal/spans"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func Test_NewTaskSpan_WithParent(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, parentSpan := tracer.Start(ctx, "node.executing")

	event := models.RunnerTaskRequestedEvent{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Payload: models.RunnerTaskRequestedPayload{
			TaskID: "task-123",
			NodeID: "node-32",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-456",
				WorkflowID:  "wf-789",
				HostID:      "host-abc",
			},
		},
	}

	spanCtx := spans.SpanContext{
		Ctx:        ctx,
		Tracer:     tracer,
		ParentSpan: parentSpan,
		JobID:      "job-def",
	}
	_, taskSpan := spans.NewTaskSpan(spanCtx, event)
	taskSpan.End()
	parentSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 2, "Expected parent and task spans")

	taskSpanFound := harness.FindSpanByName(t, endedSpans, "task.executing")

	// Verify parent relationship
	assert.Equal(t, parentSpan.SpanContext().SpanID(), taskSpanFound.Parent().SpanID())

	attrs := harness.ToMap(taskSpanFound.Attributes())
	assert.Equal(t, "exec-456", attrs["execution.id"])
	assert.Equal(t, "wf-789", attrs["workflow.id"])
	assert.Equal(t, "host-abc", attrs["host.id"])
	assert.Equal(t, "task-123", attrs["task.id"])
	assert.Equal(t, "node-32", attrs["node.id"])
	assert.Equal(t, "job-def", attrs["job.id"])
}

func Test_NewTaskSpan_WithoutParent(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	event := models.RunnerTaskRequestedEvent{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Payload: models.RunnerTaskRequestedPayload{
			TaskID: "task-123",
			NodeID: "node-923",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-456",
				WorkflowID:  "wf-789",
				HostID:      "host-abc",
			},
		},
	}

	spanCtx := spans.SpanContext{
		Ctx:        ctx,
		Tracer:     tracer,
		ParentSpan: nil,
		JobID:      "",
	}
	// Create task span without parent (orphan case)
	_, taskSpan := spans.NewTaskSpan(spanCtx, event)
	taskSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1, "Expected only task span")

	taskSpanFound := harness.FindSpanByName(t, endedSpans, "task.executing")

	// Verify no parent
	assert.False(t, taskSpanFound.Parent().IsValid())

	attrs := harness.ToMap(taskSpanFound.Attributes())
	assert.Equal(t, "exec-456", attrs["execution.id"])
	assert.Equal(t, "wf-789", attrs["workflow.id"])
	assert.Equal(t, "host-abc", attrs["host.id"])
	assert.Equal(t, "task-123", attrs["task.id"])
	assert.Equal(t, "node-923", attrs["node.id"])
	assert.NotContains(t, attrs, "job.id", "job.id should not be present when empty")
}

func Test_EndTaskSpan(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	startTime := time.Now().UTC()
	endTime := startTime.Add(50 * time.Millisecond)

	_, span := tracer.Start(ctx, "task.executing", trace.WithTimestamp(startTime))

	spans.EndTaskSpan(span, endTime.Format(time.RFC3339Nano))

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	taskSpan := endedSpans[0]
	assert.Equal(t, startTime.Truncate(time.Nanosecond), taskSpan.StartTime().Truncate(time.Nanosecond))
	assert.Equal(t, endTime.Truncate(time.Nanosecond), taskSpan.EndTime().Truncate(time.Nanosecond))
}

func Test_NewTaskSpan_WithTimestamp(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	startTime := time.Now().UTC()

	event := models.RunnerTaskRequestedEvent{
		Timestamp: startTime.Format(time.RFC3339Nano),
		Payload: models.RunnerTaskRequestedPayload{
			TaskID: "task-456",
			NodeID: "TimedTask",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-789",
				WorkflowID:  "wf-123",
				HostID:      "host-def",
			},
		},
	}

	spanCtx := spans.SpanContext{
		Ctx:        ctx,
		Tracer:     tracer,
		ParentSpan: nil,
		JobID:      "",
	}
	_, taskSpan := spans.NewTaskSpan(spanCtx, event)
	taskSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	taskSpanFound := endedSpans[0]
	assert.Equal(t, startTime.Truncate(time.Nanosecond), taskSpanFound.StartTime().Truncate(time.Nanosecond))
}

func Test_NewTaskSpan_WithInvalidTimestamp(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	event := models.RunnerTaskRequestedEvent{
		Timestamp: "invalid-timestamp",
		Payload: models.RunnerTaskRequestedPayload{
			TaskID: "task-789",
			NodeID: "InvalidTimestampTask",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-123",
				WorkflowID:  "wf-456",
				HostID:      "host-789",
			},
		},
	}

	spanCtx := spans.SpanContext{
		Ctx:        ctx,
		Tracer:     tracer,
		ParentSpan: nil,
		JobID:      "",
	}
	// Should not panic with invalid timestamp
	_, taskSpan := spans.NewTaskSpan(spanCtx, event)
	taskSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)
}

func Test_NewTaskSpan_WithoutJobID(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	event := models.RunnerTaskRequestedEvent{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Payload: models.RunnerTaskRequestedPayload{
			TaskID: "task-no-job",
			NodeID: "node-712",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-regular",
				WorkflowID:  "wf-regular",
				HostID:      "host-regular",
			},
		},
	}

	spanCtx := spans.SpanContext{
		Ctx:        ctx,
		Tracer:     tracer,
		ParentSpan: nil,
		JobID:      "",
	}
	// Create task span without job ID (regular mode)
	_, taskSpan := spans.NewTaskSpan(spanCtx, event)
	taskSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	attrs := harness.ToMap(endedSpans[0].Attributes())
	assert.Equal(t, "exec-regular", attrs["execution.id"])
	assert.Equal(t, "wf-regular", attrs["workflow.id"])
	assert.Equal(t, "host-regular", attrs["host.id"])
	assert.Equal(t, "task-no-job", attrs["task.id"])
	assert.Equal(t, "node-712", attrs["node.id"])
	assert.NotContains(t, attrs, "job.id", "job.id should not be present when empty")
}
