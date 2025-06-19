package models

const (
	EventTypeJobEnqueued  = "n8n.queue.job.enqueued"
	EventTypeJobDequeued  = "n8n.queue.job.dequeued"
	EventTypeJobCompleted = "n8n.queue.job.completed"
	EventTypeJobFailed    = "n8n.queue.job.failed"
	EventTypeJobStalled   = "n8n.queue.job.stalled"

	EventTypeWorkflowStarted = "n8n.workflow.started"
	EventTypeWorkflowSuccess = "n8n.workflow.success"
	EventTypeWorkflowFailed  = "n8n.workflow.failed"

	EventTypeNodeStarted  = "n8n.node.started"
	EventTypeNodeFinished = "n8n.node.finished"

	EventTypeRunnerTaskRequested    = "n8n.runner.task.requested"
	EventTypeRunnerResponseReceived = "n8n.runner.response.received"

	EventTypeDestinationTest = "n8n.destination.test"
)

// ----------------
//       job
// ----------------

type JobEnqueuedEvent struct {
	Timestamp string             `json:"ts"`
	Payload   JobEnqueuedPayload `json:"payload"`
}

type JobDequeuedEvent struct {
	Timestamp string             `json:"ts"`
	Payload   JobDequeuedPayload `json:"payload"`
}

type JobCompletedEvent struct {
	Timestamp string              `json:"ts"`
	Payload   JobCompletedPayload `json:"payload"`
}

type JobFailedEvent struct {
	Timestamp string              `json:"ts"`
	Payload   JobCompletedPayload `json:"payload"`
}

type JobStalledEvent struct {
	Timestamp string            `json:"ts"`
	Payload   JobStalledPayload `json:"payload"`
}

// ----------------
//    workflow
// ----------------

type WorkflowStartedEvent struct {
	Timestamp string                 `json:"ts"`
	Payload   WorkflowStartedPayload `json:"payload"`
}

type WorkflowSuccessEvent struct {
	Timestamp string                 `json:"ts"`
	Payload   WorkflowSuccessPayload `json:"payload"`
}

type WorkflowFailedEvent struct {
	Timestamp string                `json:"ts"`
	Payload   WorkflowFailedPayload `json:"payload"`
}

// ----------------
//      node
// ----------------

type NodeStartedEvent struct {
	Timestamp string             `json:"ts"`
	Payload   NodeStartedPayload `json:"payload"`
}

type NodeFinishedEvent struct {
	Timestamp string              `json:"ts"`
	Payload   NodeFinishedPayload `json:"payload"`
}

// ----------------
//      task
// ----------------

type RunnerTaskRequestedEvent struct {
	Timestamp string                     `json:"ts"`
	Payload   RunnerTaskRequestedPayload `json:"payload"`
}

type RunnerResponseReceivedEvent struct {
	Timestamp string                        `json:"ts"`
	Payload   RunnerResponseReceivedPayload `json:"payload"`
}

// ----------------
//     dst test
// ----------------

type DestinationTestEvent struct {
	Timestamp string `json:"ts"`
}
