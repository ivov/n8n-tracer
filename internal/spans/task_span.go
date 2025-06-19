package spans

import (
	"context"

	"github.com/ivov/n8n-tracer/internal/models"
	t "github.com/ivov/n8n-tracer/internal/timeutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// NewTaskSpan creates a span for the time a runner task took to execute
func NewTaskSpan(
	spanCtx SpanContext,
	event models.RunnerTaskRequestedEvent,
) (context.Context, trace.Span) {
	parentCtx := trace.ContextWithSpan(spanCtx.Ctx, spanCtx.ParentSpan)

	opts := []trace.SpanStartOption{}
	if eventTime := t.ParseEventTime(event.Timestamp); !eventTime.IsZero() {
		opts = append(opts, trace.WithTimestamp(eventTime))
	}

	ctx, span := spanCtx.Tracer.Start(parentCtx, "task.executing", opts...)

	span.SetAttributes(
		attribute.String("execution.id", event.Payload.ExecutionID),
		attribute.String("workflow.id", event.Payload.WorkflowID),
		attribute.String("host.id", event.Payload.HostID),
		attribute.String("task.id", event.Payload.TaskID),
		attribute.String("node.id", event.Payload.NodeID),
	)

	if spanCtx.JobID != "" {
		span.SetAttributes(attribute.String("job.id", spanCtx.JobID))
	}

	return ctx, span
}

func EndTaskSpan(span trace.Span, timestamp string) {
	eventTime := t.ParseEventTime(timestamp)
	span.End(trace.WithTimestamp(eventTime))
}

func EndTaskSpanOnStall(span trace.Span, event models.JobStalledEvent) {
	span.SetAttributes(
		attribute.String("task.status", "stalled"),
		attribute.String("otel.status_code", "ERROR_JOB_STALLED"),
	)
	span.SetStatus(codes.Error, "The job stalled while this task was executing.")
	eventTime := t.ParseEventTime(event.Timestamp)
	span.End(trace.WithTimestamp(eventTime))
}
