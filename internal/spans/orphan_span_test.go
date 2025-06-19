package spans_test

import (
	"context"
	"testing"

	"github.com/ivov/n8n-tracer/internal/harness"
	"github.com/ivov/n8n-tracer/internal/spans"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

func Test_SetOrphanAttributes_MissingWorkflowSpan(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, span := tracer.Start(ctx, "node.executing")

	executionID := "exec-123"
	warning, err := spans.SetOrphanAttributes(span, spans.ReasonMissingWorkflowSpan, executionID)

	require.NoError(t, err)
	assert.Equal(t, "Missing parent workflow span - Created orphan node span in execution exec-123", warning)

	span.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	attrs := endedSpans[0].Attributes()
	var hasOrphanAttr, hasOrphanReasonAttr bool
	var orphanValue bool
	var reasonValue string

	for _, attr := range attrs {
		if attr.Key == "span.orphaned" {
			hasOrphanAttr = true
			orphanValue = attr.Value.AsBool()
		}
		if attr.Key == "span.orphaned.reason" {
			hasOrphanReasonAttr = true
			reasonValue = attr.Value.AsString()
		}
	}

	assert.True(t, hasOrphanAttr, "span.orphaned attribute should be present")
	assert.True(t, orphanValue, "span.orphaned should be true")
	assert.True(t, hasOrphanReasonAttr, "span.orphaned.reason attribute should be present")
	assert.Equal(t, "missing_workflow_span", reasonValue)
}

func Test_SetOrphanAttributes_MissingNodeSpan(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, span := tracer.Start(ctx, "task.executing")

	executionID := "exec-456"
	warning, err := spans.SetOrphanAttributes(span, spans.ReasonMissingNodeSpan, executionID)

	require.NoError(t, err)
	assert.Equal(t, "Missing parent node span - Created orphan task span in execution exec-456", warning)

	span.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	attrs := endedSpans[0].Attributes()
	var hasOrphanAttr, hasOrphanReasonAttr bool
	var orphanValue bool
	var reasonValue string

	for _, attr := range attrs {
		if attr.Key == "span.orphaned" {
			hasOrphanAttr = true
			orphanValue = attr.Value.AsBool()
		}
		if attr.Key == "span.orphaned.reason" {
			hasOrphanReasonAttr = true
			reasonValue = attr.Value.AsString()
		}
	}

	assert.True(t, hasOrphanAttr, "span.orphaned attribute should be present")
	assert.True(t, orphanValue, "span.orphaned should be true")
	assert.True(t, hasOrphanReasonAttr, "span.orphaned.reason attribute should be present")
	assert.Equal(t, "missing_node_span", reasonValue)
}

func Test_SetOrphanAttributes_UnknownReason(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, span := tracer.Start(ctx, "test.span")

	executionID := "exec-789"
	warning, err := spans.SetOrphanAttributes(span, "unknown_reason", executionID)

	require.Error(t, err)
	assert.Equal(t, "unknown orphan reason: unknown_reason", err.Error())
	assert.Empty(t, warning)

	span.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	// Check that orphan attributes were still set despite the error
	// This is the current behavior based on the implementation
	attrs := endedSpans[0].Attributes()
	var hasOrphanAttr, hasOrphanReasonAttr bool

	for _, attr := range attrs {
		if attr.Key == "span.orphaned" {
			hasOrphanAttr = true
		}
		if attr.Key == "span.orphaned.reason" {
			hasOrphanReasonAttr = true
		}
	}

	assert.True(t, hasOrphanAttr, "span.orphaned attribute should be present even with error")
	assert.True(t, hasOrphanReasonAttr, "span.orphaned.reason attribute should be present even with error")
}

func Test_SetOrphanAttributes_AttributesSetCorrectly(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	tracer := otel.Tracer("test")
	ctx := context.Background()

	_, span := tracer.Start(ctx, "node.executing")

	span.SetAttributes(
		attribute.String("test.attr", "test.value"),
		attribute.String("test.ID", "42"), // use string instead of int to avoid ToMap conversion issues
	)

	executionID := "exec-test"
	_, err := spans.SetOrphanAttributes(span, spans.ReasonMissingWorkflowSpan, executionID)
	require.NoError(t, err)

	span.End()

	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1)

	attrs := harness.ToMap(endedSpans[0].Attributes())

	assert.Equal(t, "test.value", attrs["test.attr"])
	assert.Equal(t, "42", attrs["test.ID"])

	// We need to check raw attributes for boolean values since ToMap converts everything to strings
	rawAttrs := endedSpans[0].Attributes()
	var foundOrphanBool bool
	for _, attr := range rawAttrs {
		if attr.Key == "span.orphaned" && attr.Value.AsBool() == true {
			foundOrphanBool = true
			break
		}
	}
	assert.True(t, foundOrphanBool, "span.orphaned should be set to true")
	assert.Equal(t, "missing_workflow_span", attrs["span.orphaned.reason"])
}
