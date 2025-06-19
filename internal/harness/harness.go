package harness

import (
	"testing"
	"time"

	"github.com/ivov/n8n-tracer/internal/config"
	"github.com/ivov/n8n-tracer/internal/core"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type TestHarness struct {
	Tracer       *core.Tracer
	SpanRecorder *tracetest.SpanRecorder
}

func NewTestHarness(t *testing.T) (*TestHarness, func()) {
	sr := tracetest.NewSpanRecorder() // in-memory
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))

	originalTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)

	testCfg := config.N8NConfig{
		DeploymentMode:      "regular", // or "scaling" depending on test
		Version:             "test",
		WorkflowStartOffset: 50 * time.Millisecond,
	}

	tracer := core.NewTracer(testCfg)

	harness := &TestHarness{
		Tracer:       tracer,
		SpanRecorder: sr,
	}

	cleanup := func() {
		otel.SetTracerProvider(originalTP)
	}

	return harness, cleanup
}

// FindSpanByName searches a slice of spans for the first one with a matching name.
func FindSpanByName(t *testing.T, spans []trace.ReadOnlySpan, name string) trace.ReadOnlySpan {
	t.Helper()
	for _, s := range spans {
		if s.Name() == name {
			return s
		}
	}
	require.FailNowf(t, "span not found", "Could not find span with name: %s", name)

	return nil // unreachable
}

// FindSpanByNodeID searches a slice of spans for the first one with a matching "node.id" attribute.
func FindSpanByNodeID(t *testing.T, spans []trace.ReadOnlySpan, nodeID string) trace.ReadOnlySpan {
	t.Helper()
	for _, s := range spans {
		// Ensure the span is a node span before checking attributes
		if s.Name() != "node.executing" {
			continue
		}
		attrs := ToMap(s.Attributes())
		if id, ok := attrs[attribute.Key("node.id")]; ok && id == nodeID {
			return s
		}
	}
	require.FailNowf(t, "span not found", "Could not find node span with name attribute: %s", nodeID)
	return nil // Unreachable
}

// ToMap converts a slice of KeyValue to a map for easier lookups in tests.
func ToMap(attrs []attribute.KeyValue) map[attribute.Key]string {
	m := make(map[attribute.Key]string, len(attrs))
	for _, a := range attrs {
		m[a.Key] = a.Value.AsString()
	}
	return m
}
