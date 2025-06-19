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

func Test_NewJobLifetimeSpan(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	startTime := time.Now()

	event := models.JobEnqueuedEvent{
		Timestamp: startTime.Format(time.RFC3339Nano),
		Payload: models.JobEnqueuedPayload{
			JobID: "job-123",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-456",
				WorkflowID:  "wf-789",
				HostID:      "host-abc",
			},
		},
	}

	_, jobSpan := spans.NewJobLifetimeSpan(ctx, tracer, event)
	jobSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	jobSpanFound := harness.FindSpanByName(t, endedSpans, "job.lifetime")

	// Verify no parent (should be root span)
	assert.False(t, jobSpanFound.Parent().IsValid())

	assert.Equal(t, startTime.Truncate(time.Nanosecond), jobSpanFound.StartTime().Truncate(time.Nanosecond))

	attrs := harness.ToMap(jobSpanFound.Attributes())
	assert.Equal(t, "exec-456", attrs["execution.id"])
	assert.Equal(t, "wf-789", attrs["workflow.id"])
	assert.Equal(t, "job-123", attrs["job.id"])
}

func Test_NewJobLifetimeSpan_WithInvalidTimestamp(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	event := models.JobEnqueuedEvent{
		Timestamp: "invalid-timestamp",
		Payload: models.JobEnqueuedPayload{
			JobID: "job-123",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-456",
				WorkflowID:  "wf-789",
				HostID:      "host-abc",
			},
		},
	}

	// Should not panic with invalid timestamp
	_, jobSpan := spans.NewJobLifetimeSpan(ctx, tracer, event)
	jobSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)
}

func Test_NewJobPendingSpan(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, parentSpan := tracer.Start(ctx, "job.lifetime")

	enqueuedTime := time.Now()
	dequeuedTime := enqueuedTime.Add(500 * time.Millisecond)

	event := models.JobDequeuedEvent{
		Timestamp: dequeuedTime.Format(time.RFC3339Nano),
		Payload: models.JobDequeuedPayload{
			JobID: "job-123",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-456",
				WorkflowID:  "wf-789",
				HostID:      "host-consumer",
			},
		},
	}

	spanCtx := spans.SpanContext{
		Ctx:               ctx,
		Tracer:            tracer,
		ParentSpan:        parentSpan,
		ProducerHostID:    "host-producer",
		EnqueuedTimestamp: enqueuedTime.Format(time.RFC3339Nano),
	}
	spans.NewJobPendingSpan(spanCtx, event)
	parentSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 2, "Expected parent and pending spans")

	pendingSpan := harness.FindSpanByName(t, endedSpans, "job.pending")

	// Verify parent relationship
	assert.Equal(t, parentSpan.SpanContext().SpanID(), pendingSpan.Parent().SpanID())

	assert.Equal(t, enqueuedTime.Truncate(time.Nanosecond), pendingSpan.StartTime().Truncate(time.Nanosecond))
	assert.Equal(t, dequeuedTime.Truncate(time.Nanosecond), pendingSpan.EndTime().Truncate(time.Nanosecond))

	attrs := harness.ToMap(pendingSpan.Attributes())
	assert.Equal(t, "exec-456", attrs["execution.id"])
	assert.Equal(t, "wf-789", attrs["workflow.id"])
	assert.Equal(t, "host-producer", attrs["job.producer.host.id"])
	assert.Equal(t, "host-consumer", attrs["job.consumer.host.id"])
	assert.Equal(t, "job-123", attrs["job.id"])
}

func Test_NewJobPendingSpan_WithInvalidTimestamps(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, parentSpan := tracer.Start(ctx, "job.lifetime")

	event := models.JobDequeuedEvent{
		Timestamp: "invalid-timestamp",
		Payload: models.JobDequeuedPayload{
			JobID: "job-123",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-456",
				WorkflowID:  "wf-789",
				HostID:      "host-consumer",
			},
		},
	}

	spanCtx := spans.SpanContext{
		Ctx:               ctx,
		Tracer:            tracer,
		ParentSpan:        parentSpan,
		ProducerHostID:    "host-producer",
		EnqueuedTimestamp: "invalid-enqueued-timestamp",
	}
	// Should not create pending span with invalid timestamps
	spans.NewJobPendingSpan(spanCtx, event)
	parentSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1, "Only parent span should exist")
}

func Test_NewJobProcessingSpan(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, parentSpan := tracer.Start(ctx, "job.lifetime")

	startTime := time.Now()

	event := models.JobDequeuedEvent{
		Timestamp: startTime.Format(time.RFC3339Nano),
		Payload: models.JobDequeuedPayload{
			JobID: "job-123",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-456",
				WorkflowID:  "wf-789",
				HostID:      "host-consumer",
			},
		},
	}

	spanCtx := spans.SpanContext{
		Ctx:            ctx,
		Tracer:         tracer,
		ParentSpan:     parentSpan,
		ProducerHostID: "host-producer",
	}
	_, processingSpan := spans.NewJobProcessingSpan(spanCtx, event)
	processingSpan.End()
	parentSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 2, "Expected parent and processing spans")

	processingSpanFound := harness.FindSpanByName(t, endedSpans, "job.processing")

	// Verify parent relationship
	assert.Equal(t, parentSpan.SpanContext().SpanID(), processingSpanFound.Parent().SpanID())

	assert.Equal(t, startTime.Truncate(time.Nanosecond), processingSpanFound.StartTime().Truncate(time.Nanosecond))

	attrs := harness.ToMap(processingSpanFound.Attributes())
	assert.Equal(t, "exec-456", attrs["execution.id"])
	assert.Equal(t, "wf-789", attrs["workflow.id"])
	assert.Equal(t, "host-producer", attrs["job.producer.host.id"])
	assert.Equal(t, "host-consumer", attrs["job.consumer.host.id"])
	assert.Equal(t, "job-123", attrs["job.id"])
}

func Test_NewJobProcessingSpan_WithInvalidTimestamp(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, parentSpan := tracer.Start(ctx, "job.lifetime")

	event := models.JobDequeuedEvent{
		Timestamp: "invalid-timestamp",
		Payload: models.JobDequeuedPayload{
			JobID: "job-123",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-456",
				WorkflowID:  "wf-789",
				HostID:      "host-consumer",
			},
		},
	}

	spanCtx := spans.SpanContext{
		Ctx:            ctx,
		Tracer:         tracer,
		ParentSpan:     parentSpan,
		ProducerHostID: "host-producer",
	}
	// Should not panic with invalid timestamp
	_, processingSpan := spans.NewJobProcessingSpan(spanCtx, event)
	processingSpan.End()
	parentSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 2)
}

func Test_EndJobProcessingSpanOnStall(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	startTime := time.Now()
	stallTime := startTime.Add(200 * time.Millisecond)

	_, span := tracer.Start(ctx, "job.processing", trace.WithTimestamp(startTime))

	event := models.JobStalledEvent{
		Timestamp: stallTime.Format(time.RFC3339Nano),
		Payload: models.JobStalledPayload{
			JobID: "job-123",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-456",
				WorkflowID:  "wf-789",
				HostID:      "host-abc",
			},
		},
	}

	spans.EndJobProcessingSpanOnStall(span, event)

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	processingSpan := endedSpans[0]

	assert.Equal(t, startTime.Truncate(time.Nanosecond), processingSpan.StartTime().Truncate(time.Nanosecond))
	assert.Equal(t, stallTime.Truncate(time.Nanosecond), processingSpan.EndTime().Truncate(time.Nanosecond))

	assert.Equal(t, codes.Error, processingSpan.Status().Code)
	assert.Equal(t, "The job stalled during processing.", processingSpan.Status().Description)

	attrs := harness.ToMap(processingSpan.Attributes())
	assert.Equal(t, "ERROR_JOB_STALLED", attrs["otel.status_code"])
}

func Test_EndJobSpan(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	startTime := time.Now()
	endTime := startTime.Add(1 * time.Second)

	_, span := tracer.Start(ctx, "job.lifetime", trace.WithTimestamp(startTime))

	spans.EndJobSpan(span, endTime.Format(time.RFC3339Nano))

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	jobSpan := endedSpans[0]
	assert.Equal(t, startTime.Truncate(time.Nanosecond), jobSpan.StartTime().Truncate(time.Nanosecond))
	assert.Equal(t, endTime.Truncate(time.Nanosecond), jobSpan.EndTime().Truncate(time.Nanosecond))
}

func Test_EndJobSpan_WithInvalidTimestamp(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, span := tracer.Start(ctx, "job.lifetime")

	// Should not panic with invalid timestamp
	spans.EndJobSpan(span, "invalid-timestamp")

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)
}

func Test_AddJobStalledEvent(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	startTime := time.Now()
	stallTime := startTime.Add(300 * time.Millisecond)

	_, span := tracer.Start(ctx, "job.lifetime", trace.WithTimestamp(startTime))

	event := models.JobStalledEvent{
		Timestamp: stallTime.Format(time.RFC3339Nano),
		Payload: models.JobStalledPayload{
			JobID: "job-123",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-456",
				WorkflowID:  "wf-789",
				HostID:      "host-abc",
			},
		},
	}

	spans.AddJobStalledEvent(span, event)
	span.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	jobSpan := endedSpans[0]

	events := jobSpan.Events()
	require.Len(t, events, 1)
	assert.Equal(t, "job.stalled", events[0].Name)
	assert.Equal(t, stallTime.Truncate(time.Nanosecond), events[0].Time.Truncate(time.Nanosecond))
}

func Test_AddJobStalledEvent_WithInvalidTimestamp(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, span := tracer.Start(ctx, "job.lifetime")

	event := models.JobStalledEvent{
		Timestamp: "invalid-timestamp",
		Payload: models.JobStalledPayload{
			JobID: "job-123",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-456",
				WorkflowID:  "wf-789",
				HostID:      "host-abc",
			},
		},
	}

	// Should not panic with invalid timestamp
	spans.AddJobStalledEvent(span, event)
	span.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)
}

func Test_JobPendingSpan_ValidTimestampsOnly(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, parentSpan := tracer.Start(ctx, "job.lifetime")

	enqueuedTime := time.Now()

	event := models.JobDequeuedEvent{
		Timestamp: "invalid-timestamp",
		Payload: models.JobDequeuedPayload{
			JobID: "job-123",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-456",
				WorkflowID:  "wf-789",
				HostID:      "host-consumer",
			},
		},
	}

	spanCtx := spans.SpanContext{
		Ctx:               ctx,
		Tracer:            tracer,
		ParentSpan:        parentSpan,
		ProducerHostID:    "host-producer",
		EnqueuedTimestamp: enqueuedTime.Format(time.RFC3339Nano),
	}
	// Should not create pending span when dequeued timestamp is invalid
	spans.NewJobPendingSpan(spanCtx, event)
	parentSpan.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1, "Only parent span should exist")

	// Test the other way around - valid dequeued, invalid enqueued
	_, parentSpan2 := tracer.Start(ctx, "job.lifetime")

	validEvent := models.JobDequeuedEvent{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Payload: models.JobDequeuedPayload{
			JobID: "job-456",
			BasePayload: models.BasePayload{
				ExecutionID: "exec-789",
				WorkflowID:  "wf-123",
				HostID:      "host-consumer",
			},
		},
	}

	spanCtx2 := spans.SpanContext{
		Ctx:               ctx,
		Tracer:            tracer,
		ParentSpan:        parentSpan2,
		ProducerHostID:    "host-producer",
		EnqueuedTimestamp: "invalid-timestamp",
	}
	spans.NewJobPendingSpan(spanCtx2, validEvent)
	parentSpan2.End()

	endedSpans = h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 2, "Still only two parent spans should exist")
}
