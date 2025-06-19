package spans

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	ReasonMissingWorkflowSpan = "missing_workflow_span"
	ReasonMissingNodeSpan     = "missing_node_span"
)

func SetOrphanAttributes(span trace.Span, reason string, executionID string) (string, error) {
	span.SetAttributes(
		attribute.Bool("span.orphaned", true),
		attribute.String("span.orphaned.reason", reason),
	)

	switch reason {
	case ReasonMissingWorkflowSpan:
		return fmt.Sprintf("Missing parent workflow span - Created orphan node span in execution %s", executionID), nil
	case ReasonMissingNodeSpan:
		return fmt.Sprintf("Missing parent node span - Created orphan task span in execution %s", executionID), nil
	default:
		return "", fmt.Errorf("unknown orphan reason: %s", reason)
	}
}
