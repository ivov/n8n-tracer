package core_test

import (
	"testing"

	"github.com/ivov/n8n-tracer/internal/core"
	"github.com/ivov/n8n-tracer/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Parser_ToEvent_JobEvents(t *testing.T) {
	parser := core.NewParser()

	tests := []struct {
		name        string
		logline     string
		expected    interface{}
		expectError bool
	}{
		{
			name:    "JobEnqueued",
			logline: `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}`,
			expected: models.JobEnqueuedEvent{
				Timestamp: "2025-06-09T20:00:00.000Z",
				Payload: models.JobEnqueuedPayload{
					JobID:       "job-abc",
					BasePayload: models.BasePayload{ExecutionID: "exec-123", WorkflowID: "wf-456", HostID: "host-789"},
				},
			},
		},
		{
			name:    "JobDequeued",
			logline: `{"ts":"2025-06-09T20:00:01.000Z","eventName":"n8n.queue.job.dequeued","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}`,
			expected: models.JobDequeuedEvent{
				Timestamp: "2025-06-09T20:00:01.000Z",
				Payload: models.JobDequeuedPayload{
					JobID:       "job-abc",
					BasePayload: models.BasePayload{ExecutionID: "exec-123", WorkflowID: "wf-456", HostID: "host-789"},
				},
			},
		},
		{
			name:    "JobCompleted",
			logline: `{"ts":"2025-06-09T20:00:02.000Z","eventName":"n8n.queue.job.completed","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}`,
			expected: models.JobCompletedEvent{
				Timestamp: "2025-06-09T20:00:02.000Z",
				Payload: models.JobCompletedPayload{
					JobID:       "job-abc",
					BasePayload: models.BasePayload{ExecutionID: "exec-123", WorkflowID: "wf-456", HostID: "host-789"},
				},
			},
		},
		{
			name:    "JobFailed",
			logline: `{"ts":"2025-06-09T20:00:03.000Z","eventName":"n8n.queue.job.failed","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}`,
			expected: models.JobFailedEvent{
				Timestamp: "2025-06-09T20:00:03.000Z",
				Payload: models.JobCompletedPayload{
					JobID:       "job-abc",
					BasePayload: models.BasePayload{ExecutionID: "exec-123", WorkflowID: "wf-456", HostID: "host-789"},
				},
			},
		},
		{
			name:    "JobStalled",
			logline: `{"ts":"2025-06-09T20:00:04.000Z","eventName":"n8n.queue.job.stalled","payload":{"executionId":"exec-123","workflowId":"wf-456","hostId":"host-789","jobId":"job-abc"}}`,
			expected: models.JobStalledEvent{
				Timestamp: "2025-06-09T20:00:04.000Z",
				Payload: models.JobStalledPayload{
					JobID:       "job-abc",
					BasePayload: models.BasePayload{ExecutionID: "exec-123", WorkflowID: "wf-456", HostID: "host-789"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := parser.ToEvent([]byte(tt.logline))

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, event)
		})
	}
}

func Test_Parser_ToEvent_WorkflowEvents(t *testing.T) {
	parser := core.NewParser()

	tests := []struct {
		name        string
		logline     string
		expected    interface{}
		expectError bool
	}{
		{
			name:    "WorkflowStarted",
			logline: `{"ts":"2025-06-09T20:01:00.000Z","eventName":"n8n.workflow.started","payload":{"executionId":"exec-456","workflowId":"wf-789","hostId":"host-123"}}`,
			expected: models.WorkflowStartedEvent{
				Timestamp: "2025-06-09T20:01:00.000Z",
				Payload: models.WorkflowStartedPayload{
					BasePayload: models.BasePayload{ExecutionID: "exec-456", WorkflowID: "wf-789", HostID: "host-123"},
				},
			},
		},
		{
			name:    "WorkflowSuccess",
			logline: `{"ts":"2025-06-09T20:01:01.000Z","eventName":"n8n.workflow.success","payload":{"executionId":"exec-456","workflowId":"wf-789","hostId":"host-123"}}`,
			expected: models.WorkflowSuccessEvent{
				Timestamp: "2025-06-09T20:01:01.000Z",
				Payload: models.WorkflowSuccessPayload{
					BasePayload: models.BasePayload{ExecutionID: "exec-456", WorkflowID: "wf-789", HostID: "host-123"},
				},
			},
		},
		{
			name:    "WorkflowFailed",
			logline: `{"ts":"2025-06-09T20:01:02.000Z","eventName":"n8n.workflow.failed","payload":{"executionId":"exec-456","workflowId":"wf-789","hostId":"host-123","lastNodeExecuted":"FailingNode","errorNodeType":"n8n-nodes-base.httpRequest","errorMessage":"Request failed with status 404"}}`,
			expected: models.WorkflowFailedEvent{
				Timestamp: "2025-06-09T20:01:02.000Z",
				Payload: models.WorkflowFailedPayload{
					LastNodeExecuted: "FailingNode",
					ErrorNodeType:    "n8n-nodes-base.httpRequest",
					ErrorMessage:     "Request failed with status 404",
					BasePayload:      models.BasePayload{ExecutionID: "exec-456", WorkflowID: "wf-789", HostID: "host-123"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := parser.ToEvent([]byte(tt.logline))

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, event)
		})
	}
}

func Test_Parser_ToEvent_NodeEvents(t *testing.T) {
	parser := core.NewParser()

	tests := []struct {
		name        string
		logline     string
		expected    interface{}
		expectError bool
	}{
		{
			name:    "NodeStarted",
			logline: `{"ts":"2025-06-09T20:02:00.000Z","eventName":"n8n.node.started","payload":{"executionId":"exec-789","workflowId":"wf-123","hostId":"host-456","nodeId":"node-123","nodeType":"n8n-nodes-base.httpRequest"}}`,
			expected: models.NodeStartedEvent{
				Timestamp: "2025-06-09T20:02:00.000Z",
				Payload: models.NodeStartedPayload{
					NodeID:      "node-123",
					NodeType:    "n8n-nodes-base.httpRequest",
					BasePayload: models.BasePayload{ExecutionID: "exec-789", WorkflowID: "wf-123", HostID: "host-456"},
				},
			},
		},
		{
			name:    "NodeFinished",
			logline: `{"ts":"2025-06-09T20:02:01.000Z","eventName":"n8n.node.finished","payload":{"executionId":"exec-789","workflowId":"wf-123","hostId":"host-456","nodeId":"node-123","nodeType":"n8n-nodes-base.httpRequest"}}`,
			expected: models.NodeFinishedEvent{
				Timestamp: "2025-06-09T20:02:01.000Z",
				Payload: models.NodeFinishedPayload{
					NodeID:      "node-123",
					NodeType:    "n8n-nodes-base.httpRequest",
					BasePayload: models.BasePayload{ExecutionID: "exec-789", WorkflowID: "wf-123", HostID: "host-456"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := parser.ToEvent([]byte(tt.logline))

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, event)
		})
	}
}

func Test_Parser_ToEvent_TaskEvents(t *testing.T) {
	parser := core.NewParser()

	tests := []struct {
		name        string
		logline     string
		expected    interface{}
		expectError bool
	}{
		{
			name:    "RunnerTaskRequested",
			logline: `{"ts":"2025-06-09T20:03:00.000Z","eventName":"n8n.runner.task.requested","payload":{"executionId":"exec-abc","workflowId":"wf-def","hostId":"host-ghi","taskId":"task-123","nodeId":"Start"}}`,
			expected: models.RunnerTaskRequestedEvent{
				Timestamp: "2025-06-09T20:03:00.000Z",
				Payload: models.RunnerTaskRequestedPayload{
					TaskID:      "task-123",
					NodeID:      "Start",
					BasePayload: models.BasePayload{ExecutionID: "exec-abc", WorkflowID: "wf-def", HostID: "host-ghi"},
				},
			},
		},
		{
			name:    "RunnerResponseReceived",
			logline: `{"ts":"2025-06-09T20:03:01.000Z","eventName":"n8n.runner.response.received","payload":{"executionId":"exec-abc","workflowId":"wf-def","hostId":"host-ghi","taskId":"task-123","nodeId":"Start"}}`,
			expected: models.RunnerResponseReceivedEvent{
				Timestamp: "2025-06-09T20:03:01.000Z",
				Payload: models.RunnerResponseReceivedPayload{
					TaskID:      "task-123",
					NodeID:      "Start",
					BasePayload: models.BasePayload{ExecutionID: "exec-abc", WorkflowID: "wf-def", HostID: "host-ghi"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := parser.ToEvent([]byte(tt.logline))

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, event)
		})
	}
}

func Test_Parser_ToEvent_ErrorCases(t *testing.T) {
	parser := core.NewParser()

	tests := []struct {
		name        string
		logline     string
		expected    interface{}
		expectError bool
	}{
		{
			name:        "InvalidJSON",
			logline:     `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":`,
			expected:    nil,
			expectError: true,
		},
		{
			name:        "MissingEventName",
			logline:     `{"ts":"2025-06-09T20:00:00.000Z","payload":{"executionId":"exec-123"}}`,
			expected:    nil,
			expectError: true,
		},
		{
			name:        "EmptyEventName",
			logline:     `{"ts":"2025-06-09T20:00:00.000Z","eventName":"","payload":{"executionId":"exec-123"}}`,
			expected:    nil,
			expectError: true,
		},
		{
			name:        "MissingTimestamp",
			logline:     `{"eventName":"n8n.queue.job.enqueued","payload":{"executionId":"exec-123"}}`,
			expected:    nil,
			expectError: true,
		},
		{
			name:        "MissingPayload",
			logline:     `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued"}`,
			expected:    nil,
			expectError: true,
		},
		{
			name:        "UnknownEventType",
			logline:     `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.unknown.event","payload":{"executionId":"exec-123"}}`,
			expected:    nil,
			expectError: true,
		},
		{
			name:        "InvalidPayloadStructure",
			logline:     `{"ts":"2025-06-09T20:00:00.000Z","eventName":"n8n.queue.job.enqueued","payload":"not an object"}`,
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := parser.ToEvent([]byte(tt.logline))

			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, event)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, event)
		})
	}
}
