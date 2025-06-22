package spans

import (
	"context"

	"github.com/ivov/n8n-tracer/internal/models"
	t "github.com/ivov/n8n-tracer/internal/timeutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// NewJobLifetimeSpan creates a span for the time a job took from enqueuing to completion
// or failure, including retries after stallings
func NewJobLifetimeSpan(
	ctx context.Context,
	tracer trace.Tracer,
	event models.JobEnqueuedEvent,
) (context.Context, trace.Span) {
	opts := []trace.SpanStartOption{}
	if eventTime := t.ParseEventTime(event.Timestamp); !eventTime.IsZero() {
		opts = append(opts, trace.WithTimestamp(eventTime))
	}

	ctx, span := tracer.Start(ctx, "job.lifetime", opts...)

	span.SetAttributes(
		attribute.String("execution.id", event.Payload.ExecutionID),
		attribute.String("workflow.id", event.Payload.WorkflowID),
		attribute.String("job.id", event.Payload.JobID),
	)

	return ctx, span
}

// NewJobPendingSpan creates a span for the time a job spent in the queue.
//
// Most spans have simple start and end events, but a `job.pending` span can start
// on enqueuing _or_ on stalling, so a queue span is created retroactively on dequeuing.
func NewJobPendingSpan(
	spanCtx SpanContext,
	event models.JobDequeuedEvent,
) {
	enqueuedAt := t.ParseEventTime(spanCtx.EnqueuedTimestamp)
	dequeuedAt := t.ParseEventTime(event.Timestamp)

	if enqueuedAt.IsZero() || dequeuedAt.IsZero() {
		return
	}

	parentCtx := trace.ContextWithSpan(spanCtx.Ctx, spanCtx.ParentSpan)

	_, jobPendingSpan := spanCtx.Tracer.Start(
		parentCtx,
		"job.pending",
		trace.WithTimestamp(enqueuedAt),
	)

	jobPendingSpan.SetAttributes(
		attribute.String("execution.id", event.Payload.ExecutionID),
		attribute.String("workflow.id", event.Payload.WorkflowID),
		attribute.String("job.producer.host.id", spanCtx.ProducerHostID),
		attribute.String("job.consumer.host.id", event.Payload.HostID),
		attribute.String("job.id", event.Payload.JobID),
	)

	jobPendingSpan.End(trace.WithTimestamp(dequeuedAt))
}

// NewJobProcessingSpan creates a span for the actual processing time of a job,
// excluding setup and teardown
func NewJobProcessingSpan(
	spanCtx SpanContext,
	event models.JobDequeuedEvent,
) (context.Context, trace.Span) {
	parentCtx := trace.ContextWithSpan(spanCtx.Ctx, spanCtx.ParentSpan)

	opts := []trace.SpanStartOption{}
	if eventTime := t.ParseEventTime(event.Timestamp); !eventTime.IsZero() {
		opts = append(opts, trace.WithTimestamp(eventTime))
	}

	ctx, jobProcessingSpan := spanCtx.Tracer.Start(parentCtx, "job.processing", opts...)

	jobProcessingSpan.SetAttributes(
		attribute.String("execution.id", event.Payload.ExecutionID),
		attribute.String("workflow.id", event.Payload.WorkflowID),
		attribute.String("job.producer.host.id", spanCtx.ProducerHostID),
		attribute.String("job.consumer.host.id", event.Payload.HostID),
		attribute.String("job.id", event.Payload.JobID),
	)

	return ctx, jobProcessingSpan
}

func EndJobProcessingSpanOnStall(span trace.Span, event models.JobStalledEvent) {
	span.SetAttributes(
		attribute.String("otel.status_code", "ERROR_JOB_STALLED"),
	)
	span.SetStatus(codes.Error, "The job stalled during processing.")
	eventTime := t.ParseEventTime(event.Timestamp)
	span.End(trace.WithTimestamp(eventTime))
}

func EndJobSpan(span trace.Span, timestamp string) {
	eventTime := t.ParseEventTime(timestamp)
	span.End(trace.WithTimestamp(eventTime))
}

func AddJobStalledEvent(span trace.Span, event models.JobStalledEvent) {
	eventTime := t.ParseEventTime(event.Timestamp)
	span.AddEvent("job.stalled", trace.WithTimestamp(eventTime))
}
