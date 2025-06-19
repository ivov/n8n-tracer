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
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func Test_NewWorkflowSpan_WithParent(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, parentSpan := tracer.Start(ctx, "job.processing")

	event := models.WorkflowStartedEvent{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Payload: models.WorkflowStartedPayload{
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
		ParentSpan: parentSpan,
		JobID:      "job-abc",
	}
	_, workflowSpan := spans.NewWorkflowSpan(spanCtx, event)
	workflowSpan.End()
	parentSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 2, "Expected parent and workflow spans")

	workflowSpanFound := harness.FindSpanByName(t, endedSpans, "workflow.executing")

	assert.Equal(t, parentSpan.SpanContext().SpanID(), workflowSpanFound.Parent().SpanID())

	attrs := harness.ToMap(workflowSpanFound.Attributes())
	assert.Equal(t, "exec-123", attrs["execution.id"])
	assert.Equal(t, "wf-456", attrs["workflow.id"])
	assert.Equal(t, "host-789", attrs["host.id"])
	assert.Equal(t, "job-abc", attrs["job.id"])
}

func Test_NewWorkflowSpan_WithoutParent(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	event := models.WorkflowStartedEvent{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Payload: models.WorkflowStartedPayload{
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
	// Create workflow span without parent (regular mode)
	_, workflowSpan := spans.NewWorkflowSpan(spanCtx, event)
	workflowSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1, "Expected only workflow span")

	workflowSpanFound := harness.FindSpanByName(t, endedSpans, "workflow.executing")

	// Verify no parent
	assert.False(t, workflowSpanFound.Parent().IsValid())

	attrs := harness.ToMap(workflowSpanFound.Attributes())
	assert.Equal(t, "exec-123", attrs["execution.id"])
	assert.Equal(t, "wf-456", attrs["workflow.id"])
	assert.Equal(t, "host-789", attrs["host.id"])
	assert.NotContains(t, attrs, "job.id", "job.id should not be present when empty")
}

func Test_AddWorkflowSuccessAttributes(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, span := tracer.Start(ctx, "workflow.executing")

	event := models.WorkflowSuccessEvent{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Payload: models.WorkflowSuccessPayload{
			BasePayload: models.BasePayload{
				ExecutionID: "exec-123",
				WorkflowID:  "wf-456",
				HostID:      "host-789",
			},
		},
	}

	spans.AddWorkflowSuccessAttributes(span, event)
	span.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	attrs := harness.ToMap(endedSpans[0].Attributes())
	assert.Equal(t, "success", attrs["workflow.status"])
}

func Test_AddWorkflowFailureAttributes(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, span := tracer.Start(ctx, "workflow.executing")

	event := models.WorkflowFailedEvent{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Payload: models.WorkflowFailedPayload{
			LastNodeExecuted: "FailingNode",
			ErrorNodeType:    "n8n-nodes-base.httpRequest",
			ErrorMessage:     "Request failed with status 404",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-123",
				WorkflowID:  "wf-456",
				HostID:      "host-789",
			},
		},
	}

	spans.AddWorkflowFailureAttributes(span, event)
	span.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	attrs := harness.ToMap(endedSpans[0].Attributes())
	assert.Equal(t, "failed", attrs["workflow.status"])
	assert.Equal(t, "FailingNode", attrs["workflow.error.lastNodeExecuted"])
	assert.Equal(t, "n8n-nodes-base.httpRequest", attrs["workflow.error.node.type"])
	assert.Equal(t, "Request failed with status 404", attrs["workflow.error.errorMessage"])
}

func Test_EndWorkflowSpan(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	startTime := time.Now()
	endTime := startTime.Add(100 * time.Millisecond)

	_, span := tracer.Start(ctx, "workflow.executing", trace.WithTimestamp(startTime))

	spans.EndWorkflowSpan(span, endTime.Format(time.RFC3339Nano))

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	workflowSpan := endedSpans[0]
	assert.Equal(t, startTime.Truncate(time.Nanosecond), workflowSpan.StartTime().Truncate(time.Nanosecond))
	assert.Equal(t, endTime.Truncate(time.Nanosecond), workflowSpan.EndTime().Truncate(time.Nanosecond))
}

func Test_EndWorkflowSpanOnStall(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	startTime := time.Now()
	stallTime := startTime.Add(50 * time.Millisecond)

	_, span := tracer.Start(ctx, "workflow.executing", trace.WithTimestamp(startTime))

	event := models.JobStalledEvent{
		Timestamp: stallTime.Format(time.RFC3339Nano),
		Payload: models.JobStalledPayload{
			JobID: "job-123",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-123",
				WorkflowID:  "wf-456",
				HostID:      "host-789",
			},
		},
	}

	spans.EndWorkflowSpanOnStall(span, event)

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	workflowSpan := endedSpans[0]

	// Verify timing
	assert.Equal(t, startTime.Truncate(time.Nanosecond), workflowSpan.StartTime().Truncate(time.Nanosecond))
	assert.Equal(t, stallTime.Truncate(time.Nanosecond), workflowSpan.EndTime().Truncate(time.Nanosecond))

	// Verify error status
	assert.Equal(t, codes.Error, workflowSpan.Status().Code)
	assert.Equal(t, "The job stalled, terminating workflow execution.", workflowSpan.Status().Description)

	// Verify attributes
	attrs := harness.ToMap(workflowSpan.Attributes())
	assert.Equal(t, "ERROR_JOB_STALLED", attrs["otel.status_code"])
}

func Test_NewWorkflowSpan_WithTimestamp(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	startTime := time.Now()

	event := models.WorkflowStartedEvent{
		Timestamp: startTime.Format(time.RFC3339Nano),
		Payload: models.WorkflowStartedPayload{
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
	_, workflowSpan := spans.NewWorkflowSpan(spanCtx, event)
	workflowSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	workflowSpanFound := endedSpans[0]
	assert.Equal(t, startTime.Truncate(time.Nanosecond), workflowSpanFound.StartTime().Truncate(time.Nanosecond))
}

func Test_NewWorkflowSpan_WithInvalidTimestamp(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	event := models.WorkflowStartedEvent{
		Timestamp: "invalid-timestamp",
		Payload: models.WorkflowStartedPayload{
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
	_, workflowSpan := spans.NewWorkflowSpan(spanCtx, event)
	workflowSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)
}
