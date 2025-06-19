package models

type BasePayload struct {
	ExecutionID string `json:"executionId"`
	WorkflowID  string `json:"workflowId"`
	HostID      string `json:"hostId"`
}

// ----------------
//       job
// ----------------

type JobEnqueuedPayload struct {
	BasePayload
	JobID string `json:"jobId"`
}

type JobDequeuedPayload struct {
	BasePayload
	JobID string `json:"jobId"`
}

type JobCompletedPayload struct {
	BasePayload
	JobID string `json:"jobId"`
}

type JobStalledPayload struct {
	BasePayload
	JobID string `json:"jobId"`
}

// ----------------
//    workflow
// ----------------

type WorkflowStartedPayload struct {
	BasePayload
}

type WorkflowSuccessPayload struct {
	BasePayload
}

type WorkflowFailedPayload struct {
	BasePayload
	LastNodeExecuted string `json:"lastNodeExecuted"`
	ErrorNodeType    string `json:"errorNodeType"`
	ErrorMessage     string `json:"errorMessage"`
}

// ----------------
//      node
// ----------------

type NodeStartedPayload struct {
	BasePayload
	NodeID   string `json:"nodeId"`
	NodeType string `json:"nodeType"`
}

type NodeFinishedPayload struct {
	BasePayload
	NodeID   string `json:"nodeId"`
	NodeType string `json:"nodeType"`
}

// ----------------
//      runner
// ----------------

type RunnerTaskRequestedPayload struct {
	BasePayload
	TaskID string `json:"taskId"`
	NodeID string `json:"nodeId"`
}

type RunnerResponseReceivedPayload struct {
	BasePayload
	TaskID string `json:"taskId"`
	NodeID string `json:"nodeId"`
}
