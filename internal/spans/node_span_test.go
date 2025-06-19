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

func Test_NewNodeSpan_WithParent(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, parentSpan := tracer.Start(ctx, "workflow.executing")

	event := models.NodeStartedEvent{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Payload: models.NodeStartedPayload{
			NodeID:   "node-4",
			NodeType: "n8n-nodes-base.httpRequest",
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
	_, nodeSpan := spans.NewNodeSpan(spanCtx, event)
	nodeSpan.End()
	parentSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 2, "Expected parent and node spans")

	nodeSpanFound := harness.FindSpanByName(t, endedSpans, "node.executing")

	// Verify parent relationship
	assert.Equal(t, parentSpan.SpanContext().SpanID(), nodeSpanFound.Parent().SpanID())

	attrs := harness.ToMap(nodeSpanFound.Attributes())
	assert.Equal(t, "exec-123", attrs["execution.id"])
	assert.Equal(t, "wf-456", attrs["workflow.id"])
	assert.Equal(t, "host-789", attrs["host.id"])
	assert.Equal(t, "node-4", attrs["node.id"])
	assert.Equal(t, "n8n-nodes-base.httpRequest", attrs["node.type"])
	assert.Equal(t, "job-abc", attrs["job.id"])
}

func Test_NewNodeSpan_WithoutParent(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	// Create node started event
	event := models.NodeStartedEvent{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Payload: models.NodeStartedPayload{
			NodeID:   "node-22",
			NodeType: "n8n-nodes-base.test",
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
	// Create node span without parent (orphan case)
	_, nodeSpan := spans.NewNodeSpan(spanCtx, event)
	nodeSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1, "Expected only node span")

	nodeSpanFound := harness.FindSpanByName(t, endedSpans, "node.executing")

	// Verify no parent
	assert.False(t, nodeSpanFound.Parent().IsValid())

	attrs := harness.ToMap(nodeSpanFound.Attributes())
	assert.Equal(t, "exec-123", attrs["execution.id"])
	assert.Equal(t, "wf-456", attrs["workflow.id"])
	assert.Equal(t, "host-789", attrs["host.id"])
	assert.Equal(t, "node-22", attrs["node.id"])
	assert.Equal(t, "n8n-nodes-base.test", attrs["node.type"])
	assert.NotContains(t, attrs, "job.id", "job.id should not be present when empty")
}

func Test_EndNodeSpan(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	startTime := time.Now()
	endTime := startTime.Add(75 * time.Millisecond)

	_, span := tracer.Start(ctx, "node.executing", trace.WithTimestamp(startTime))

	spans.EndNodeSpan(span, endTime.Format(time.RFC3339Nano))

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	nodeSpan := endedSpans[0]
	assert.Equal(t, startTime.Truncate(time.Nanosecond), nodeSpan.StartTime().Truncate(time.Nanosecond))
	assert.Equal(t, endTime.Truncate(time.Nanosecond), nodeSpan.EndTime().Truncate(time.Nanosecond))
}

func Test_EndNodeSpanOnStall(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	startTime := time.Now()
	stallTime := startTime.Add(30 * time.Millisecond)

	_, span := tracer.Start(ctx, "node.executing", trace.WithTimestamp(startTime))

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

	spans.EndNodeSpanOnStall(span, event)

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	nodeSpan := endedSpans[0]

	assert.Equal(t, startTime.Truncate(time.Nanosecond), nodeSpan.StartTime().Truncate(time.Nanosecond))
	assert.Equal(t, stallTime.Truncate(time.Nanosecond), nodeSpan.EndTime().Truncate(time.Nanosecond))

	assert.Equal(t, codes.Error, nodeSpan.Status().Code)
	assert.Equal(t, "The job stalled while this node was executing.", nodeSpan.Status().Description)

	attrs := harness.ToMap(nodeSpan.Attributes())
	assert.Equal(t, "stalled", attrs["node.status"])
	assert.Equal(t, "ERROR_JOB_STALLED", attrs["otel.status_code"])
}

func Test_NewNodeSpan_WithTimestamp(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	startTime := time.Now()

	event := models.NodeStartedEvent{
		Timestamp: startTime.Format(time.RFC3339Nano),
		Payload: models.NodeStartedPayload{
			NodeID:   "TimedNode",
			NodeType: "n8n-nodes-base.timer",
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
	_, nodeSpan := spans.NewNodeSpan(spanCtx, event)
	nodeSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	nodeSpanFound := endedSpans[0]
	assert.Equal(t, startTime.Truncate(time.Nanosecond), nodeSpanFound.StartTime().Truncate(time.Nanosecond))
}

func Test_NewNodeSpan_WithInvalidTimestamp(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	event := models.NodeStartedEvent{
		Timestamp: "invalid-timestamp",
		Payload: models.NodeStartedPayload{
			NodeID:   "InvalidTimestampNode",
			NodeType: "n8n-nodes-base.test",
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
	_, nodeSpan := spans.NewNodeSpan(spanCtx, event)
	nodeSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)
}

func Test_NewNodeSpan_WithoutJobID(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	event := models.NodeStartedEvent{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Payload: models.NodeStartedPayload{
			NodeID:   "node-71",
			NodeType: "n8n-nodes-base.start",
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
	// Create node span without job ID (regular mode)
	_, nodeSpan := spans.NewNodeSpan(spanCtx, event)
	nodeSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	attrs := harness.ToMap(endedSpans[0].Attributes())
	assert.Equal(t, "exec-regular", attrs["execution.id"])
	assert.Equal(t, "wf-regular", attrs["workflow.id"])
	assert.Equal(t, "host-regular", attrs["host.id"])
	assert.Equal(t, "node-71", attrs["node.id"])
	assert.Equal(t, "n8n-nodes-base.start", attrs["node.type"])
	assert.NotContains(t, attrs, "job.id", "job.id should not be present when empty")
}

func Test_EndNodeSpan_WithInvalidTimestamp(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, span := tracer.Start(ctx, "node.executing")

	// Should not panic with invalid timestamp
	spans.EndNodeSpan(span, "invalid-timestamp")

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)
}

func Test_EndNodeSpanOnStall_WithInvalidTimestamp(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, span := tracer.Start(ctx, "node.executing")

	event := models.JobStalledEvent{
		Timestamp: "invalid-timestamp",
		Payload: models.JobStalledPayload{
			JobID: "job-123",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-123",
				WorkflowID:  "wf-456",
				HostID:      "host-789",
			},
		},
	}

	// Should not panic with invalid timestamp
	spans.EndNodeSpanOnStall(span, event)

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	nodeSpan := endedSpans[0]

	assert.Equal(t, codes.Error, nodeSpan.Status().Code)
	assert.Equal(t, "The job stalled while this node was executing.", nodeSpan.Status().Description)

	attrs := harness.ToMap(nodeSpan.Attributes())
	assert.Equal(t, "stalled", attrs["node.status"])
	assert.Equal(t, "ERROR_JOB_STALLED", attrs["otel.status_code"])
}
