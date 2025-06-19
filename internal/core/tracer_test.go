package core_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ivov/n8n-tracer/internal/config"
	"github.com/ivov/n8n-tracer/internal/core"
	"github.com/ivov/n8n-tracer/internal/harness"
	"github.com/ivov/n8n-tracer/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func Test_RegularMode_SuccessfulWorkflow(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	executionID := "exec-happy-regular"
	workflowID := "wf-jkl"
	hostID := "host-main"
	now := time.Now()
	ts := func(offset int) string {
		return now.Add(time.Duration(offset) * time.Millisecond).Format(time.RFC3339Nano)
	}

	// ----------------
	//     arrange
	// ----------------

	// sequence of events for a successful workflow in regular mode

	events := []interface{}{
		models.WorkflowStartedEvent{
			Timestamp: ts(0),
			Payload:   models.WorkflowStartedPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
		models.NodeStartedEvent{
			Timestamp: ts(50),
			Payload:   models.NodeStartedPayload{NodeID: "node-9", NodeType: "n8n-nodes-base.airtable", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
		models.RunnerTaskRequestedEvent{
			Timestamp: ts(51),
			Payload:   models.RunnerTaskRequestedPayload{TaskID: "task-2", NodeID: "node-9", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
		models.RunnerResponseReceivedEvent{
			Timestamp: ts(150),
			Payload:   models.RunnerResponseReceivedPayload{TaskID: "task-2", NodeID: "node-9", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
		models.NodeFinishedEvent{
			Timestamp: ts(151),
			Payload:   models.NodeFinishedPayload{NodeID: "node-9", NodeType: "n8n-nodes-base.airtable", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
		models.WorkflowSuccessEvent{
			Timestamp: ts(200),
			Payload:   models.WorkflowSuccessPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
	}

	// ----------------
	//      act
	// ----------------

	for _, event := range events {
		err := h.Tracer.ProcessEvent(event)
		require.NoError(t, err)
	}

	// ----------------
	//     assert
	// ----------------

	// Assert ended spans
	endedSpans := h.SpanRecorder.Ended()
	assert.Len(t, endedSpans, 3, "Expected 3 spans to be created (workflow, node, task)")
	assert.Equal(t, 0, h.Tracer.ExecutionStatesInMemory(), "Tracer memory should be empty")

	// Assert spans by name
	workflowSpan := harness.FindSpanByName(t, endedSpans, "workflow.executing")
	nodeSpan := harness.FindSpanByNodeID(t, endedSpans, "node-9")
	taskSpan := harness.FindSpanByName(t, endedSpans, "task.executing")

	// Assert hierarchy
	assert.False(t, workflowSpan.Parent().IsValid(), "workflow.executing should be the root span in regular mode")
	assert.Equal(t, workflowSpan.SpanContext().SpanID(), nodeSpan.Parent().SpanID(), "node.executing should be a child of workflow.executing")
	assert.Equal(t, nodeSpan.SpanContext().SpanID(), taskSpan.Parent().SpanID(), "task.executing should be a child of node.executing")

	// Assert common attributes
	expectedCommonAttrs := []attribute.KeyValue{
		attribute.String("execution.id", executionID),
		attribute.String("workflow.id", workflowID),
	}
	spans := []trace.ReadOnlySpan{workflowSpan, nodeSpan, taskSpan}
	for _, span := range spans {
		attrsMap := harness.ToMap(span.Attributes())
		t.Run(fmt.Sprintf("CommonAttrsIn_%s", span.Name()), func(t *testing.T) {
			for _, attr := range expectedCommonAttrs {
				assert.Equal(t, attr.Value.AsString(), attrsMap[attr.Key])
			}
			assert.NotContains(t, attrsMap, attribute.Key("job.id"), "job.id should be absent in regular mode")
		})
	}

	// Assert span-specific attributes
	assert.Equal(t, "node-9", harness.ToMap(nodeSpan.Attributes())["node.id"])
	assert.Equal(t, "task-2", harness.ToMap(taskSpan.Attributes())["task.id"])
	assert.Equal(t, "success", harness.ToMap(workflowSpan.Attributes())["workflow.status"])
}

func Test_RegularMode_FailedWorkflow(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	executionID := "exec-fail-regular"
	workflowID := "wf-mno"
	hostID := "host-main"
	now := time.Now()
	ts := func(offset int) string {
		return now.Add(time.Duration(offset) * time.Millisecond).Format(time.RFC3339Nano)
	}

	// ----------------
	//     arrange
	// ----------------

	// sequence of events for a failed workflow in regular mode

	events := []interface{}{
		models.WorkflowStartedEvent{
			Timestamp: ts(0),
			Payload:   models.WorkflowStartedPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
		models.NodeStartedEvent{
			Timestamp: ts(50),
			Payload:   models.NodeStartedPayload{NodeID: "Start", NodeType: "n8n-nodes-base.start", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
		models.NodeFinishedEvent{
			Timestamp: ts(100),
			Payload:   models.NodeFinishedPayload{NodeID: "Start", NodeType: "n8n-nodes-base.start", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
		models.NodeStartedEvent{
			Timestamp: ts(101),
			Payload:   models.NodeStartedPayload{NodeID: "HttpNodeThatErrors", NodeType: "n8n-nodes-base.httpRequest", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
		// Workflow fails while "HttpNodeThatErrors" is executing
		models.WorkflowFailedEvent{
			Timestamp: ts(150),
			Payload: models.WorkflowFailedPayload{
				LastNodeExecuted: "HttpNodeThatErrors",
				ErrorNodeType:    "n8n-nodes-base.httpRequest",
				ErrorMessage:     "An error occurred",
				BasePayload:      models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID},
			},
		},
	}

	// ----------------
	//      act
	// ----------------

	for _, event := range events {
		err := h.Tracer.ProcessEvent(event)
		require.NoError(t, err)
	}

	// ----------------
	//     assert
	// ----------------

	// Assert ended spans
	endedSpans := h.SpanRecorder.Ended()
	assert.Len(t, endedSpans, 3, "Expected 3 spans to be ended (workflow, Start node, and unterminated HttpNode)")
	assert.Equal(t, 0, h.Tracer.ExecutionStatesInMemory(), "Tracer memory should be empty after workflow failure")

	// Assert spans by name
	workflowSpan := harness.FindSpanByName(t, endedSpans, "workflow.executing")
	startNodeSpan := harness.FindSpanByNodeID(t, endedSpans, "Start")
	httpNodeSpan := harness.FindSpanByNodeID(t, endedSpans, "HttpNodeThatErrors")

	// Assert hierarchy
	assert.False(t, workflowSpan.Parent().IsValid(), "workflow.executing should be the root span")
	assert.Equal(t, workflowSpan.SpanContext().SpanID(), startNodeSpan.Parent().SpanID(), "Start node should be a child of workflow.executing")

	// Assert span-specific attributes
	workflowAttrs := harness.ToMap(workflowSpan.Attributes())
	assert.Equal(t, "failed", workflowAttrs["workflow.status"])
	assert.Equal(t, "HttpNodeThatErrors", workflowAttrs["workflow.error.lastNodeExecuted"])
	assert.Equal(t, "An error occurred", workflowAttrs["workflow.error.errorMessage"])

	// Assert that the unterminated node span was ended by the cleanup logic
	assert.True(t, httpNodeSpan.EndTime().After(httpNodeSpan.StartTime()))
}

func Test_ScalingMode_SuccessfulJob(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	executionID := "exec-happy-scaling"
	workflowID := "wf-abc"
	jobID := "job-xyz"
	hostID1 := "host-enqueue"
	hostID2 := "host-process"
	now := time.Now()
	ts := func(offset int) string {
		return now.Add(time.Duration(offset) * time.Millisecond).Format(time.RFC3339Nano)
	}

	// ----------------
	//     arrange
	// ----------------

	// sequence of events for a successful job in scaling mode

	events := []interface{}{
		models.JobEnqueuedEvent{
			Timestamp: ts(0),
			Payload:   models.JobEnqueuedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID1}},
		},
		models.JobDequeuedEvent{
			Timestamp: ts(100),
			Payload:   models.JobDequeuedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
		models.WorkflowStartedEvent{
			Timestamp: ts(101),
			Payload:   models.WorkflowStartedPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
		models.NodeStartedEvent{
			Timestamp: ts(150),
			Payload:   models.NodeStartedPayload{NodeID: "node-5", NodeType: "n8n-nodes-base.start", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
		models.RunnerTaskRequestedEvent{
			Timestamp: ts(151),
			Payload:   models.RunnerTaskRequestedPayload{TaskID: "task-1", NodeID: "node-5", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
		models.RunnerResponseReceivedEvent{
			Timestamp: ts(200),
			Payload:   models.RunnerResponseReceivedPayload{TaskID: "task-1", NodeID: "node-5", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
		models.NodeFinishedEvent{
			Timestamp: ts(201),
			Payload:   models.NodeFinishedPayload{NodeID: "node-5", NodeType: "n8n-nodes-base.start", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
		models.WorkflowSuccessEvent{
			Timestamp: ts(300),
			Payload:   models.WorkflowSuccessPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
		models.JobCompletedEvent{
			Timestamp: ts(301),
			Payload:   models.JobCompletedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
	}

	// ----------------
	//      act
	// ----------------

	for _, event := range events {
		err := h.Tracer.ProcessEvent(event)
		require.NoError(t, err)
	}

	// ----------------
	//     assert
	// ----------------

	// Assert ended spans
	endedSpans := h.SpanRecorder.Ended()
	assert.Len(t, endedSpans, 6, "6 spans should be created")

	// Assert no leftovers in memory
	assert.Equal(t, 0, h.Tracer.ExecutionStatesInMemory(), "Tracer memory should be empty")

	// Assert spans by name
	jobSpan := harness.FindSpanByName(t, endedSpans, "job.lifetime")
	jobPendingSpan := harness.FindSpanByName(t, endedSpans, "job.pending")
	jobProcessingSpan := harness.FindSpanByName(t, endedSpans, "job.processing")
	workflowSpan := harness.FindSpanByName(t, endedSpans, "workflow.executing")
	nodeSpan := harness.FindSpanByName(t, endedSpans, "node.executing")
	taskSpan := harness.FindSpanByName(t, endedSpans, "task.executing")

	// Assert hierarchy
	assert.False(t, jobSpan.Parent().IsValid(), "job.lifetime should be a root span")
	assert.Equal(t, jobSpan.SpanContext().SpanID(), jobPendingSpan.Parent().SpanID(), "job.pending should be a child of job.lifetime")
	assert.Equal(t, jobSpan.SpanContext().SpanID(), jobProcessingSpan.Parent().SpanID(), "job.processing should be a child of job.lifetime")
	assert.Equal(t, jobProcessingSpan.SpanContext().SpanID(), workflowSpan.Parent().SpanID(), "workflow.executing should be a child of job.processing")
	assert.Equal(t, workflowSpan.SpanContext().SpanID(), nodeSpan.Parent().SpanID(), "node.executing should be a child of workflow.executing")
	assert.Equal(t, nodeSpan.SpanContext().SpanID(), taskSpan.Parent().SpanID(), "task.executing should be a child of node.executing")

	// common attributes
	expectedCommonAttrs := []attribute.KeyValue{
		attribute.String("execution.id", executionID),
		attribute.String("workflow.id", workflowID),
		attribute.String("job.id", jobID),
	}
	spans := []trace.ReadOnlySpan{jobSpan, jobPendingSpan, jobProcessingSpan, workflowSpan, nodeSpan, taskSpan}
	for _, span := range spans {
		attrsMap := harness.ToMap(span.Attributes())
		t.Run(fmt.Sprintf("CommonAttrsIn_%s", span.Name()), func(t *testing.T) {
			for _, attr := range expectedCommonAttrs {
				assert.Equal(t, attr.Value.AsString(), attrsMap[attr.Key])
			}
		})
	}

	// span-specific attributes
	assert.Equal(t, hostID1, harness.ToMap(jobPendingSpan.Attributes())["job.producer.host.id"])
	assert.Equal(t, hostID2, harness.ToMap(jobPendingSpan.Attributes())["job.consumer.host.id"])
	assert.Equal(t, "node-5", harness.ToMap(nodeSpan.Attributes())["node.id"])
	assert.Equal(t, "task-1", harness.ToMap(taskSpan.Attributes())["task.id"])
}

func Test_ScalingMode_FailedJob(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	executionID := "exec-fail-scaling"
	workflowID := "wf-def"
	jobID := "job-abc"
	hostID := "host-process"
	now := time.Now()
	ts := func(offset int) string {
		return now.Add(time.Duration(offset) * time.Millisecond).Format(time.RFC3339Nano)
	}

	// ----------------
	//     arrange
	// ----------------

	// sequence of events for a failed job in scaling mode

	events := []interface{}{
		models.JobEnqueuedEvent{
			Timestamp: ts(0),
			Payload:   models.JobEnqueuedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: "host-enqueue"}},
		},
		models.JobDequeuedEvent{
			Timestamp: ts(100),
			Payload:   models.JobDequeuedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
		models.WorkflowStartedEvent{
			Timestamp: ts(101),
			Payload:   models.WorkflowStartedPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
		models.NodeStartedEvent{
			Timestamp: ts(150),
			Payload:   models.NodeStartedPayload{NodeID: "FailingNode", NodeType: "n8n-nodes-base.httpRequest", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
		models.WorkflowFailedEvent{
			Timestamp: ts(200),
			Payload: models.WorkflowFailedPayload{
				LastNodeExecuted: "FailingNode",
				ErrorNodeType:    "n8n-nodes-base.httpRequest",
				ErrorMessage:     "Request failed with status code 404",
				BasePayload:      models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID},
			},
		},
		models.JobFailedEvent{
			Timestamp: ts(201),
			Payload:   models.JobCompletedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID}},
		},
	}

	// ----------------
	//      act
	// ----------------

	for _, event := range events {
		err := h.Tracer.ProcessEvent(event)
		require.NoError(t, err)
	}

	// ----------------
	//     assert
	// ----------------

	// The node span is never finished, so it won't be in the "ended" list,
	// but all parent spans should be correctly terminated.
	endedSpans := h.SpanRecorder.Ended()
	assert.Len(t, endedSpans, 5, "Expected 5 spans to be ended")

	// The "FailingNode" span was started but never finished, but the final
	// job failed event should trigger cleanup.
	assert.Equal(t, 0, h.Tracer.ExecutionStatesInMemory(), "Tracer memory should be empty")

	// Assert spans by name
	jobLifetimeSpan := harness.FindSpanByName(t, endedSpans, "job.lifetime")
	jobPendingSpan := harness.FindSpanByName(t, endedSpans, "job.pending")
	jobProcessingSpan := harness.FindSpanByName(t, endedSpans, "job.processing")
	workflowSpan := harness.FindSpanByName(t, endedSpans, "workflow.executing")

	// Assert hierarchy
	assert.False(t, jobLifetimeSpan.Parent().IsValid(), "job.lifetime should be a root span")
	assert.Equal(t, jobLifetimeSpan.SpanContext().SpanID(), jobPendingSpan.Parent().SpanID())
	assert.Equal(t, jobLifetimeSpan.SpanContext().SpanID(), jobProcessingSpan.Parent().SpanID())
	assert.Equal(t, jobProcessingSpan.SpanContext().SpanID(), workflowSpan.Parent().SpanID())

	// Assert workflow span failure attributes and status
	workflowAttrs := harness.ToMap(workflowSpan.Attributes())
	assert.Equal(t, "failed", workflowAttrs["workflow.status"])
	assert.Equal(t, "FailingNode", workflowAttrs["workflow.error.lastNodeExecuted"])
	assert.Equal(t, "Request failed with status code 404", workflowAttrs["workflow.error.errorMessage"])
}

func Test_ScalingMode_WorkflowStartedBeforeJobDequeued(t *testing.T) {
	// Create test harness with scaling mode configuration
	originalTP := otel.GetTracerProvider()
	defer otel.SetTracerProvider(originalTP)

	sr := tracetest.NewSpanRecorder()
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)

	// Configure for scaling mode (important!)
	testCfg := config.N8NConfig{
		DeploymentMode:      "scaling", // This is crucial
		Version:             "test",
		WorkflowStartOffset: 50 * time.Millisecond,
	}

	tracer := core.NewTracer(testCfg)

	executionID := "exec-out-of-order-workflow"
	workflowID := "wf-ooo"
	jobID := "job-ooo"
	hostID1 := "host-enqueue"
	hostID2 := "host-process"
	now := time.Now()
	ts := func(offset int) string {
		return now.Add(time.Duration(offset) * time.Millisecond).Format(time.RFC3339Nano)
	}

	// ----------------
	//     arrange
	// ----------------

	// sequence of events where workflow.started arrives before job.dequeued

	events := []interface{}{
		models.JobEnqueuedEvent{
			Timestamp: ts(0),
			Payload:   models.JobEnqueuedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID1}},
		},
		// Workflow started arrives BEFORE job dequeued (out of order)
		models.WorkflowStartedEvent{
			Timestamp: ts(50),
			Payload:   models.WorkflowStartedPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
		models.JobDequeuedEvent{
			Timestamp: ts(100),
			Payload:   models.JobDequeuedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
		models.NodeStartedEvent{
			Timestamp: ts(150),
			Payload:   models.NodeStartedPayload{NodeID: "node-1", NodeType: "n8n-nodes-base.start", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
		models.NodeFinishedEvent{
			Timestamp: ts(200),
			Payload:   models.NodeFinishedPayload{NodeID: "node-1", NodeType: "n8n-nodes-base.start", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
		models.WorkflowSuccessEvent{
			Timestamp: ts(250),
			Payload:   models.WorkflowSuccessPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
		models.JobCompletedEvent{
			Timestamp: ts(300),
			Payload:   models.JobCompletedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2}},
		},
	}

	// ----------------
	//      act
	// ----------------

	for _, event := range events {
		err := tracer.ProcessEvent(event)
		require.NoError(t, err)
	}

	// ----------------
	//     assert
	// ----------------

	// Assert ended spans
	endedSpans := sr.Ended()
	assert.Len(t, endedSpans, 5, "Expected 5 spans to be created")
	assert.Equal(t, 0, tracer.ExecutionStatesInMemory(), "Tracer memory should be empty")

	// Assert spans by name
	jobSpan := harness.FindSpanByName(t, endedSpans, "job.lifetime")
	jobPendingSpan := harness.FindSpanByName(t, endedSpans, "job.pending")
	jobProcessingSpan := harness.FindSpanByName(t, endedSpans, "job.processing")
	workflowSpan := harness.FindSpanByName(t, endedSpans, "workflow.executing")
	nodeSpan := harness.FindSpanByNodeID(t, endedSpans, "node-1")

	// Assert hierarchy
	assert.False(t, jobSpan.Parent().IsValid(), "job.lifetime should be a root span")
	assert.Equal(t, jobSpan.SpanContext().SpanID(), jobPendingSpan.Parent().SpanID(), "job.pending should be a child of job.lifetime")
	assert.Equal(t, jobSpan.SpanContext().SpanID(), jobProcessingSpan.Parent().SpanID(), "job.processing should be a child of job.lifetime")
	assert.Equal(t, jobProcessingSpan.SpanContext().SpanID(), workflowSpan.Parent().SpanID(), "workflow.executing should be a child of job.processing")
	assert.Equal(t, workflowSpan.SpanContext().SpanID(), nodeSpan.Parent().SpanID(), "node.executing should be a child of workflow.executing")

	// Assert timing - workflow span should start after job.dequeued due to the offset correction
	jobDequeuedTime := now.Add(100 * time.Millisecond)
	expectedWorkflowStartTime := jobDequeuedTime.Add(50 * time.Millisecond) // N8N_WORKFLOW_START_OFFSET default

	assert.Equal(t, expectedWorkflowStartTime.Truncate(time.Nanosecond), workflowSpan.StartTime().Truncate(time.Nanosecond),
		"Workflow span should start with corrected time (dequeued + offset)")

	// Assert common attributes
	expectedCommonAttrs := []attribute.KeyValue{
		attribute.String("execution.id", executionID),
		attribute.String("workflow.id", workflowID),
		attribute.String("job.id", jobID),
	}
	spans := []trace.ReadOnlySpan{jobSpan, jobPendingSpan, jobProcessingSpan, workflowSpan, nodeSpan}
	for _, span := range spans {
		attrsMap := harness.ToMap(span.Attributes())
		t.Run(fmt.Sprintf("CommonAttrsIn_%s", span.Name()), func(t *testing.T) {
			for _, attr := range expectedCommonAttrs {
				assert.Equal(t, attr.Value.AsString(), attrsMap[attr.Key])
			}
		})
	}

	// Assert span-specific attributes
	assert.Equal(t, hostID1, harness.ToMap(jobPendingSpan.Attributes())["job.producer.host.id"])
	assert.Equal(t, hostID2, harness.ToMap(jobPendingSpan.Attributes())["job.consumer.host.id"])
	assert.Equal(t, "node-1", harness.ToMap(nodeSpan.Attributes())["node.id"])
	assert.Equal(t, "success", harness.ToMap(workflowSpan.Attributes())["workflow.status"])
}

func Test_ScalingMode_StallingJob(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	executionID := "exec-stall-scaling"
	workflowID := "wf-ghi"
	jobID := "job-def"
	hostID1 := "host-enqueue"
	hostID2_stalled := "host-process-stalled"
	hostID3_resumed := "host-process-resumed"
	now := time.Now()
	ts := func(offset int) string {
		return now.Add(time.Duration(offset) * time.Millisecond).Format(time.RFC3339Nano)
	}

	// ----------------
	//     arrange
	// ----------------

	// sequence of events for a job that stalls then succeeds in scaling mode

	events := []interface{}{
		// first attempt (will stall)
		models.JobEnqueuedEvent{
			Timestamp: ts(0),
			Payload:   models.JobEnqueuedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID1}},
		},
		models.JobDequeuedEvent{
			Timestamp: ts(100),
			Payload:   models.JobDequeuedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2_stalled}},
		},
		models.WorkflowStartedEvent{
			Timestamp: ts(101),
			Payload:   models.WorkflowStartedPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2_stalled}},
		},
		models.NodeStartedEvent{
			Timestamp: ts(150),
			Payload:   models.NodeStartedPayload{NodeID: "NodeBeforeStall", NodeType: "n8n-nodes-base.wait", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2_stalled}},
		},
		models.JobStalledEvent{
			Timestamp: ts(300),
			Payload:   models.JobStalledPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2_stalled}},
		},

		// second attempt (will succeed)
		models.JobDequeuedEvent{
			Timestamp: ts(400), // Dequeued again by a new host
			Payload:   models.JobDequeuedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID3_resumed}},
		},
		models.WorkflowStartedEvent{
			Timestamp: ts(401),
			Payload:   models.WorkflowStartedPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID3_resumed}},
		},
		models.NodeStartedEvent{
			Timestamp: ts(450),
			Payload:   models.NodeStartedPayload{NodeID: "Start", NodeType: "n8n-nodes-base.start", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID3_resumed}},
		},
		models.NodeFinishedEvent{
			Timestamp: ts(500),
			Payload:   models.NodeFinishedPayload{NodeID: "Start", NodeType: "n8n-nodes-base.start", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID3_resumed}},
		},
		models.WorkflowSuccessEvent{
			Timestamp: ts(600),
			Payload:   models.WorkflowSuccessPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID3_resumed}},
		},
		models.JobCompletedEvent{
			Timestamp: ts(601),
			Payload:   models.JobCompletedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID3_resumed}},
		},
	}

	// ----------------
	//      act
	// ----------------

	for _, event := range events {
		err := h.Tracer.ProcessEvent(event)
		require.NoError(t, err)
	}

	// ----------------
	//     assert
	// ----------------

	// Assert ended spans
	endedSpans := h.SpanRecorder.Ended()
	assert.Len(t, endedSpans, 9, "Expected 9 spans to be ended after stall and completion")
	assert.Equal(t, 0, h.Tracer.ExecutionStatesInMemory(), "Tracer memory should be empty after final completion")

	// assert ended spans
	jobSpan := harness.FindSpanByName(t, endedSpans, "job.lifetime")
	var jobPendingSpans, jobProcessingSpans, workflowSpans, nodeSpans []trace.ReadOnlySpan
	for _, s := range endedSpans {
		switch s.Name() {
		case "job.pending":
			jobPendingSpans = append(jobPendingSpans, s)
		case "job.processing":
			jobProcessingSpans = append(jobProcessingSpans, s)
		case "workflow.executing":
			workflowSpans = append(workflowSpans, s)
		case "node.executing":
			nodeSpans = append(nodeSpans, s)
		}
	}
	require.Len(t, jobPendingSpans, 2, "Should have two job.pending spans (initial + after stall)")
	require.Len(t, jobProcessingSpans, 2, "Should have two job.processing spans (one for each attempt)")
	require.Len(t, workflowSpans, 2, "Should have two workflow.executing spans")
	require.Len(t, nodeSpans, 2, "Should have two node.executing spans")
	assert.Equal(t, "job.stalled", jobSpan.Events()[0].Name)

	// Assert hierarchy
	assert.False(t, jobSpan.Parent().IsValid(), "job.lifetime should be a root span")

	// Assert stalling event
	assert.Len(t, jobSpan.Events(), 1, "job.lifetime should have one 'job.stalled' event")
	assert.Equal(t, "job.stalled", jobSpan.Events()[0].Name)

	// Assert first attempt (stalled)
	pendingSpan1 := jobPendingSpans[0]
	processingSpan1 := jobProcessingSpans[0]
	workflowSpan1 := workflowSpans[0]
	nodeSpan1 := harness.FindSpanByNodeID(t, nodeSpans, "NodeBeforeStall")

	assert.Equal(t, jobSpan.SpanContext().SpanID(), pendingSpan1.Parent().SpanID())
	assert.Equal(t, jobSpan.SpanContext().SpanID(), processingSpan1.Parent().SpanID())
	assert.Equal(t, processingSpan1.SpanContext().SpanID(), workflowSpan1.Parent().SpanID())
	assert.Equal(t, workflowSpan1.SpanContext().SpanID(), nodeSpan1.Parent().SpanID())
	assert.Equal(t, codes.Error, nodeSpan1.Status().Code, "Stalled node span should have Error status")
	assert.Equal(t, "ERROR_JOB_STALLED", harness.ToMap(nodeSpan1.Attributes())["otel.status_code"])

	// Assert second attempt (success)
	jobPendingSpan2 := jobPendingSpans[1]
	jobProcessingSpan2 := jobProcessingSpans[1]
	workflowSpan2 := workflowSpans[1]
	nodeSpan2 := harness.FindSpanByNodeID(t, nodeSpans, "Start")

	assert.Equal(t, jobSpan.SpanContext().SpanID(), jobPendingSpan2.Parent().SpanID())
	assert.Equal(t, jobSpan.SpanContext().SpanID(), jobProcessingSpan2.Parent().SpanID())
	assert.Equal(t, jobProcessingSpan2.SpanContext().SpanID(), workflowSpan2.Parent().SpanID())
	assert.Equal(t, workflowSpan2.SpanContext().SpanID(), nodeSpan2.Parent().SpanID())
}

func Test_OutOfOrder_NodeFinishBeforeStart(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	executionID := "exec-out-of-order-node"
	now := time.Now()
	ts := func(offset int) string {
		return now.Add(time.Duration(offset) * time.Millisecond).Format(time.RFC3339Nano)
	}

	events := []interface{}{
		models.WorkflowStartedEvent{
			Timestamp: ts(0),
			Payload:   models.WorkflowStartedPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: "wf-1", HostID: "host-1"}},
		},
		// Finish event arrives BEFORE start event
		models.NodeFinishedEvent{
			Timestamp: ts(150),
			Payload:   models.NodeFinishedPayload{NodeID: "node-1", NodeType: "n8n-nodes-base.test", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: "wf-1", HostID: "host-1"}},
		},
		models.NodeStartedEvent{
			Timestamp: ts(50),
			Payload:   models.NodeStartedPayload{NodeID: "node-1", NodeType: "n8n-nodes-base.test", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: "wf-1", HostID: "host-1"}},
		},
		models.WorkflowSuccessEvent{
			Timestamp: ts(200),
			Payload:   models.WorkflowSuccessPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: "wf-1", HostID: "host-1"}},
		},
	}

	// Process NodeFinished first
	err := h.Tracer.ProcessEvent(events[1])
	require.NoError(t, err)
	assert.Equal(t, 1, h.Tracer.ExecutionStatesInMemory(), "Tracer should hold state for the execution")
	assert.Empty(t, h.SpanRecorder.Ended(), "No spans should be ended yet")

	// Process WorkflowStarted
	err = h.Tracer.ProcessEvent(events[0])
	require.NoError(t, err)

	// Process NodeStarted, which should trigger reconciliation
	err = h.Tracer.ProcessEvent(events[2])
	require.NoError(t, err)

	// Now the node span should be complete
	endedSpans := h.SpanRecorder.Ended()
	require.Len(t, endedSpans, 1, "Node span should be ended now")
	nodeSpan := harness.FindSpanByNodeID(t, endedSpans, "node-1")
	assert.Equal(t, now.Add(50*time.Millisecond).Truncate(time.Nanosecond), nodeSpan.StartTime().Truncate(time.Nanosecond))
	assert.Equal(t, now.Add(150*time.Millisecond).Truncate(time.Nanosecond), nodeSpan.EndTime().Truncate(time.Nanosecond))

	// Process final event
	err = h.Tracer.ProcessEvent(events[3])
	require.NoError(t, err)
	assert.Equal(t, 0, h.Tracer.ExecutionStatesInMemory(), "Tracer memory should be empty after workflow completes")
	assert.Len(t, h.SpanRecorder.Ended(), 2, "Workflow and Node spans should be ended")
}

func Test_NodeStarted_WithoutWorkflowSpan_CreatesOrphan(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	executionID := "exec-orphan-node"

	// ----------------
	//      act
	// ----------------

	nodeStartedEvent := models.NodeStartedEvent{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Payload:   models.NodeStartedPayload{NodeID: "node-612", NodeType: "n8n-nodes-base.test", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: "wf-123", HostID: "host-123"}},
	}

	err := h.Tracer.ProcessEvent(nodeStartedEvent)
	require.NoError(t, err)

	// ----------------
	//     assert
	// ----------------

	assert.Equal(t, 1, h.Tracer.ExecutionStatesInMemory(), "Execution state should be created in memory")

	// End the span to check its attributes
	nodeFinishedEvent := models.NodeFinishedEvent{
		Timestamp: time.Now().Add(100 * time.Millisecond).Format(time.RFC3339Nano),
		Payload:   models.NodeFinishedPayload{NodeID: "node-612", NodeType: "n8n-nodes-base.test", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: "wf-123", HostID: "host-123"}},
	}
	err = h.Tracer.ProcessEvent(nodeFinishedEvent)
	require.NoError(t, err)

	endedSpans := h.SpanRecorder.Ended()
	assert.Len(t, endedSpans, 1, "Expected 1 ended span")

	nodeSpan := harness.FindSpanByNodeID(t, endedSpans, "node-612")

	// Check for orphan attributes using the raw attributes because ToMap converts everything to strings using Value.AsString()
	var hasOrphanAttr, hasOrphanReasonAttr bool
	var orphanReason string
	for _, attr := range nodeSpan.Attributes() {
		if attr.Key == "span.orphaned" && attr.Value.AsBool() == true {
			hasOrphanAttr = true
		}
		if attr.Key == "span.orphaned.reason" {
			hasOrphanReasonAttr = true
			orphanReason = attr.Value.AsString()
		}
	}

	assert.True(t, hasOrphanAttr, "span.orphaned should be true")
	assert.True(t, hasOrphanReasonAttr, "span.orphaned.reason should be present")
	assert.Equal(t, "missing_workflow_span", orphanReason)
	assert.False(t, nodeSpan.Parent().IsValid(), "Orphan node span should have no parent")
}

func Test_TaskStarted_WithoutNodeSpan_CreatesOrphan(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	executionID := "exec-orphan-task"
	taskID := "task-orphan"

	// ----------------
	//      act
	// ----------------

	taskStartedEvent := models.RunnerTaskRequestedEvent{
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Payload:   models.RunnerTaskRequestedPayload{TaskID: taskID, NodeID: "node-901", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: "wf-123", HostID: "host-123"}},
	}

	err := h.Tracer.ProcessEvent(taskStartedEvent)
	require.NoError(t, err)

	// ----------------
	//     assert
	// ----------------

	assert.Equal(t, 1, h.Tracer.ExecutionStatesInMemory(), "Task span should be created in memory")

	taskFinishedEvent := models.RunnerResponseReceivedEvent{
		Timestamp: time.Now().Add(100 * time.Millisecond).Format(time.RFC3339Nano),
		Payload:   models.RunnerResponseReceivedPayload{TaskID: taskID, NodeID: "node-901", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: "wf-123", HostID: "host-123"}},
	}
	err = h.Tracer.ProcessEvent(taskFinishedEvent)
	require.NoError(t, err)

	endedSpans := h.SpanRecorder.Ended()
	assert.Len(t, endedSpans, 1, "Expected 1 ended span")

	taskSpan := harness.FindSpanByName(t, endedSpans, "task.executing")

	// Check for orphan attributes using the raw attributes because ToMap converts everything to strings using Value.AsString()
	var hasOrphanAttr, hasOrphanReasonAttr bool
	var orphanReason string
	for _, attr := range taskSpan.Attributes() {
		if attr.Key == "span.orphaned" && attr.Value.AsBool() == true {
			hasOrphanAttr = true
		}
		if attr.Key == "span.orphaned.reason" {
			hasOrphanReasonAttr = true
			orphanReason = attr.Value.AsString()
		}
	}

	assert.True(t, hasOrphanAttr, "span.orphaned should be true")
	assert.True(t, hasOrphanReasonAttr, "span.orphaned.reason should be present")
	assert.Equal(t, "missing_node_span", orphanReason)
	assert.False(t, taskSpan.Parent().IsValid(), "Orphan task span should have no parent")
}

func Test_ScalingMode_StalledJobCreatesRetroactiveWorkflowSpan(t *testing.T) {
	h, cleanup := harness.NewTestHarness(t)
	defer cleanup()

	executionID := "exec-stall-retroactive"
	workflowID := "wf-retroactive"
	jobID := "job-retroactive"
	hostID1 := "host-enqueue"
	hostID2_stalled := "host-process-stalled"
	hostID3_resumed := "host-process-resumed"
	now := time.Now()
	ts := func(offset int) string {
		return now.Add(time.Duration(offset) * time.Millisecond).Format(time.RFC3339Nano)
	}

	// ----------------
	//     arrange
	// ----------------

	// Sequence of events where a job stalls and then gets dequeued again
	// The key point is that there's NO second n8n.workflow.started event
	// because n8n only sends this once on initial enqueuing

	events := []interface{}{
		// Initial attempt
		models.JobEnqueuedEvent{
			Timestamp: ts(0),
			Payload:   models.JobEnqueuedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID1}},
		},
		models.JobDequeuedEvent{
			Timestamp: ts(100),
			Payload:   models.JobDequeuedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2_stalled}},
		},
		models.WorkflowStartedEvent{
			Timestamp: ts(101),
			Payload:   models.WorkflowStartedPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2_stalled}},
		},
		models.NodeStartedEvent{
			Timestamp: ts(150),
			Payload:   models.NodeStartedPayload{NodeID: "LongRunningNode", NodeType: "n8n-nodes-base.wait", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2_stalled}},
		},
		// Job stalls
		models.JobStalledEvent{
			Timestamp: ts(300),
			Payload:   models.JobStalledPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID2_stalled}},
		},

		// Second attempt - NO n8n.workflow.started event here!
		models.JobDequeuedEvent{
			Timestamp: ts(400), // Dequeued again by a new host
			Payload:   models.JobDequeuedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID3_resumed}},
		},
		// Workflow execution continues from where it left off
		models.NodeStartedEvent{
			Timestamp: ts(450),
			Payload:   models.NodeStartedPayload{NodeID: "Start", NodeType: "n8n-nodes-base.start", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID3_resumed}},
		},
		models.NodeFinishedEvent{
			Timestamp: ts(500),
			Payload:   models.NodeFinishedPayload{NodeID: "Start", NodeType: "n8n-nodes-base.start", BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID3_resumed}},
		},
		models.WorkflowSuccessEvent{
			Timestamp: ts(600),
			Payload:   models.WorkflowSuccessPayload{BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID3_resumed}},
		},
		models.JobCompletedEvent{
			Timestamp: ts(601),
			Payload:   models.JobCompletedPayload{JobID: jobID, BasePayload: models.BasePayload{ExecutionID: executionID, WorkflowID: workflowID, HostID: hostID3_resumed}},
		},
	}

	// ----------------
	//      act
	// ----------------

	for _, event := range events {
		err := h.Tracer.ProcessEvent(event)
		require.NoError(t, err)
	}

	// ----------------
	//     assert
	// ----------------

	endedSpans := h.SpanRecorder.Ended()
	assert.Len(t, endedSpans, 9, "Expected 9 spans after stall and completion")
	assert.Equal(t, 0, h.Tracer.ExecutionStatesInMemory(), "Tracer memory should be empty after completion")

	// Find all spans
	jobSpan := harness.FindSpanByName(t, endedSpans, "job.lifetime")

	var workflowSpans []trace.ReadOnlySpan
	for _, s := range endedSpans {
		if s.Name() == "workflow.executing" {
			workflowSpans = append(workflowSpans, s)
		}
	}
	require.Len(t, workflowSpans, 2, "Should have two workflow.executing spans")

	// Sort workflow spans by start time to identify first and second
	firstWorkflowSpan := workflowSpans[0]
	secondWorkflowSpan := workflowSpans[1]
	if firstWorkflowSpan.StartTime().After(secondWorkflowSpan.StartTime()) {
		_, secondWorkflowSpan = secondWorkflowSpan, firstWorkflowSpan
	}

	// Verify job.lifetime has stall event
	jobEvents := jobSpan.Events()
	require.Len(t, jobEvents, 1, "job.lifetime should have one 'job.stalled' event")
	assert.Equal(t, "job.stalled", jobEvents[0].Name)

	// Verify second workflow span timing
	// It should start at the second dequeue time + 50ms (workflow start offset)
	expectedSecondWorkflowStart := now.Add(400 * time.Millisecond).Add(50 * time.Millisecond)
	assert.Equal(t, expectedSecondWorkflowStart.Truncate(time.Nanosecond), secondWorkflowSpan.StartTime().Truncate(time.Nanosecond),
		"Second workflow span should start at dequeue time + 50ms (synthetic span with workflow start offset)")

	// Verify attributes of synthetic workflow span
	secondWorkflowAttrs := harness.ToMap(secondWorkflowSpan.Attributes())
	assert.Equal(t, executionID, secondWorkflowAttrs["execution.id"])
	assert.Equal(t, workflowID, secondWorkflowAttrs["workflow.id"])
	assert.Equal(t, hostID3_resumed, secondWorkflowAttrs["host.id"], "Synthetic workflow span should have the resumed host ID")
	assert.Equal(t, jobID, secondWorkflowAttrs["job.id"])
	assert.Equal(t, "success", secondWorkflowAttrs["workflow.status"])

	// Verify hierarchy - second workflow span should be child of second job.processing span
	var jobProcessingSpans []trace.ReadOnlySpan
	for _, s := range endedSpans {
		if s.Name() == "job.processing" {
			jobProcessingSpans = append(jobProcessingSpans, s)
		}
	}
	require.Len(t, jobProcessingSpans, 2, "Should have two job.processing spans")

	// Find the second job.processing span (the one that starts after the stall)
	var secondJobProcessingSpan trace.ReadOnlySpan
	for _, span := range jobProcessingSpans {
		if span.StartTime().Equal(now.Add(400 * time.Millisecond).Add(1 * time.Microsecond)) { // The artificial delay
			secondJobProcessingSpan = span
			break
		}
	}
	require.NotNil(t, secondJobProcessingSpan, "Should find second job.processing span")

	assert.Equal(t, secondJobProcessingSpan.SpanContext().SpanID(), secondWorkflowSpan.Parent().SpanID(),
		"Synthetic workflow span should be child of second job.processing span")
}
