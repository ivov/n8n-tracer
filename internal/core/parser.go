package core

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	m "github.com/ivov/n8n-tracer/internal/models"
)

type Parser struct{}

func NewParser() *Parser {
	return &Parser{}
}

// ToEvent parses a logline into one of the `models.Event` types
func (parser *Parser) ToEvent(logline []byte) (interface{}, error) {
	var rawEvent struct {
		Timestamp string          `json:"ts"`
		EventName string          `json:"eventName"`
		Payload   json.RawMessage `json:"payload"`
	}

	if err := json.Unmarshal(logline, &rawEvent); err != nil {
		return nil, fmt.Errorf("failed to unmarshal raw event: %w", err)
	}

	if os.Getenv("VERBOSE") != "" {
		// TODO: Leveled logging
		log.Printf("VERBOSE: Parsing event: %s", rawEvent.EventName)
	}

	if rawEvent.EventName == "" {
		return nil, fmt.Errorf("missing or empty eventName field")
	}

	if rawEvent.Timestamp == "" {
		return nil, fmt.Errorf("missing or empty timestamp field")
	}

	if len(rawEvent.Payload) == 0 && rawEvent.EventName != m.EventTypeDestinationTest {
		return nil, fmt.Errorf("missing or empty payload field")
	}

	switch rawEvent.EventName {
	case m.EventTypeJobEnqueued:
		var payload m.JobEnqueuedPayload
		if err := json.Unmarshal(rawEvent.Payload, &payload); err != nil {
			return nil, err
		}
		return m.JobEnqueuedEvent{Timestamp: rawEvent.Timestamp, Payload: payload}, nil

	case m.EventTypeJobDequeued:
		var payload m.JobDequeuedPayload
		if err := json.Unmarshal(rawEvent.Payload, &payload); err != nil {
			return nil, err
		}
		return m.JobDequeuedEvent{Timestamp: rawEvent.Timestamp, Payload: payload}, nil

	case m.EventTypeJobCompleted:
		var payload m.JobCompletedPayload
		if err := json.Unmarshal(rawEvent.Payload, &payload); err != nil {
			return nil, err
		}
		return m.JobCompletedEvent{Timestamp: rawEvent.Timestamp, Payload: payload}, nil

	case m.EventTypeJobFailed:
		var payload m.JobCompletedPayload
		if err := json.Unmarshal(rawEvent.Payload, &payload); err != nil {
			return nil, err
		}
		return m.JobFailedEvent{Timestamp: rawEvent.Timestamp, Payload: payload}, nil

	case m.EventTypeJobStalled:
		var payload m.JobStalledPayload
		if err := json.Unmarshal(rawEvent.Payload, &payload); err != nil {
			return nil, err
		}
		return m.JobStalledEvent{Timestamp: rawEvent.Timestamp, Payload: payload}, nil

	case m.EventTypeWorkflowStarted:
		var payload m.WorkflowStartedPayload
		if err := json.Unmarshal(rawEvent.Payload, &payload); err != nil {
			return nil, err
		}
		return m.WorkflowStartedEvent{Timestamp: rawEvent.Timestamp, Payload: payload}, nil

	case m.EventTypeWorkflowSuccess:
		var payload m.WorkflowSuccessPayload
		if err := json.Unmarshal(rawEvent.Payload, &payload); err != nil {
			return nil, err
		}
		return m.WorkflowSuccessEvent{Timestamp: rawEvent.Timestamp, Payload: payload}, nil

	case m.EventTypeWorkflowFailed:
		var payload m.WorkflowFailedPayload
		if err := json.Unmarshal(rawEvent.Payload, &payload); err != nil {
			return nil, err
		}
		return m.WorkflowFailedEvent{Timestamp: rawEvent.Timestamp, Payload: payload}, nil

	case m.EventTypeNodeStarted:
		var payload m.NodeStartedPayload
		if err := json.Unmarshal(rawEvent.Payload, &payload); err != nil {
			return nil, err
		}
		return m.NodeStartedEvent{Timestamp: rawEvent.Timestamp, Payload: payload}, nil

	case m.EventTypeNodeFinished:
		var payload m.NodeFinishedPayload
		if err := json.Unmarshal(rawEvent.Payload, &payload); err != nil {
			return nil, err
		}
		return m.NodeFinishedEvent{Timestamp: rawEvent.Timestamp, Payload: payload}, nil

	case m.EventTypeRunnerTaskRequested:
		var payload m.RunnerTaskRequestedPayload
		if err := json.Unmarshal(rawEvent.Payload, &payload); err != nil {
			return nil, err
		}
		return m.RunnerTaskRequestedEvent{Timestamp: rawEvent.Timestamp, Payload: payload}, nil

	case m.EventTypeRunnerResponseReceived:
		var payload m.RunnerResponseReceivedPayload
		if err := json.Unmarshal(rawEvent.Payload, &payload); err != nil {
			return nil, err
		}
		return m.RunnerResponseReceivedEvent{Timestamp: rawEvent.Timestamp, Payload: payload}, nil

	case m.EventTypeDestinationTest:
		return m.DestinationTestEvent{Timestamp: rawEvent.Timestamp}, nil

	default:
		return nil, fmt.Errorf("unknown event type: %s", rawEvent.EventName)
	}
}
