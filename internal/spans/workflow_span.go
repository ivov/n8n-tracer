package spans

import (
	"context"

	"github.com/ivov/n8n-tracer/internal/models"
	t "github.com/ivov/n8n-tracer/internal/timeutil"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// NewWorkflowSpan creates a span for the time a workflow took to execute
func NewWorkflowSpan(
	spanCtx SpanContext,
	event models.WorkflowStartedEvent,
) (context.Context, trace.Span) {
	parentCtx := spanCtx.Ctx
	if spanCtx.ParentSpan != nil {
		parentCtx = trace.ContextWithSpan(spanCtx.Ctx, spanCtx.ParentSpan)
	}

	opts := []trace.SpanStartOption{}
	if eventTime := t.ParseEventTime(event.Timestamp); !eventTime.IsZero() {
		opts = append(opts, trace.WithTimestamp(eventTime))
	}

	ctx, span := spanCtx.Tracer.Start(parentCtx, "workflow.executing", opts...)

	span.SetAttributes(
		attribute.String("execution.id", event.Payload.ExecutionID),
		attribute.String("workflow.id", event.Payload.WorkflowID),
		attribute.String("host.id", event.Payload.HostID),
	)

	if spanCtx.JobID != "" {
		span.SetAttributes(attribute.String("job.id", spanCtx.JobID))
	}

	return ctx, span
}

func AddWorkflowSuccessAttributes(span trace.Span, event models.WorkflowSuccessEvent) {
	span.SetAttributes(attribute.String("workflow.status", "success"))
}

func AddWorkflowFailureAttributes(span trace.Span, event models.WorkflowFailedEvent) {
	span.SetAttributes(
		attribute.String("workflow.status", "failed"),
		attribute.String("workflow.error.lastNodeExecuted", event.Payload.LastNodeExecuted),
		attribute.String("workflow.error.node.type", event.Payload.ErrorNodeType),
		attribute.String("workflow.error.errorMessage", event.Payload.ErrorMessage),
	)
}

func EndWorkflowSpan(span trace.Span, timestamp string) {
	eventTime := t.ParseEventTime(timestamp)
	span.End(trace.WithTimestamp(eventTime))
}

func EndWorkflowSpanOnStall(span trace.Span, event models.JobStalledEvent) {
	span.SetAttributes(
		attribute.String("otel.status_code", "ERROR_JOB_STALLED"),
	)
	span.SetStatus(codes.Error, "The job stalled, terminating workflow execution.")
	eventTime := t.ParseEventTime(event.Timestamp)
	span.End(trace.WithTimestamp(eventTime))
}
