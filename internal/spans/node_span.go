package spans

import (
	"context"

	"github.com/ivov/n8n-tracer/internal/models"
	t "github.com/ivov/n8n-tracer/internal/timeutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// NewNodeSpan creates a span for the time a node took to execute
func NewNodeSpan(
	spanCtx SpanContext,
	event models.NodeStartedEvent,
) (context.Context, trace.Span) {
	parentCtx := trace.ContextWithSpan(spanCtx.Ctx, spanCtx.ParentSpan)

	opts := []trace.SpanStartOption{}
	if eventTime := t.ParseEventTime(event.Timestamp); !eventTime.IsZero() {
		opts = append(opts, trace.WithTimestamp(eventTime))
	}

	ctx, span := spanCtx.Tracer.Start(parentCtx, "node.executing", opts...)

	span.SetAttributes(
		attribute.String("execution.id", event.Payload.ExecutionID),
		attribute.String("workflow.id", event.Payload.WorkflowID),
		attribute.String("host.id", event.Payload.HostID),
		attribute.String("node.id", event.Payload.NodeID),
		attribute.String("node.type", event.Payload.NodeType),
	)

	if spanCtx.JobID != "" {
		span.SetAttributes(attribute.String("job.id", spanCtx.JobID))
	}

	return ctx, span
}

func EndNodeSpan(span trace.Span, timestamp string) {
	eventTime := t.ParseEventTime(timestamp)
	span.End(trace.WithTimestamp(eventTime))
}

func EndNodeSpanOnStall(span trace.Span, event models.JobStalledEvent) {
	span.SetAttributes(
		attribute.String("node.status", "stalled"),
		attribute.String("otel.status_code", "ERROR_JOB_STALLED"),
	)
	span.SetStatus(codes.Error, "The job stalled while this node was executing.")
	eventTime := t.ParseEventTime(event.Timestamp)
	span.End(trace.WithTimestamp(eventTime))
}
