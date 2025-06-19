package spans

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

type SpanContext struct {
	Ctx               context.Context
	Tracer            trace.Tracer
	ParentSpan        trace.Span
	JobID             string
	ProducerHostID    string
	EnqueuedTimestamp string
}
