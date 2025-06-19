package core

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/ivov/n8n-tracer/internal/config"
	m "github.com/ivov/n8n-tracer/internal/models"
	s "github.com/ivov/n8n-tracer/internal/spans"
	"github.com/ivov/n8n-tracer/internal/timeutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// TODO: Pass state to span helpers?

type (
	ExecutionID = string
	NodeID      = string
	TaskID      = string
)

type ExecutionState struct {
	JobLifetimeSpan   trace.Span
	JobProcessingSpan trace.Span
	WorkflowSpan      trace.Span

	// `node.executing` spans waiting for the `node.finished` event
	openNodeSpans map[NodeID]trace.Span

	// `task.executing` spans waiting for the `task.finished` event
	openTaskSpans map[TaskID]trace.Span

	// `node.finished` events that arrived before their `node.started` events
	prematureNodeFinishedEvents map[NodeID]m.NodeFinishedEvent

	// `task.finished` events that arrived before their `task.started` events
	prematureTaskFinishedEvents map[TaskID]m.RunnerResponseReceivedEvent

	// `job.dequeued` event that arrived before the `job.enqueued` event
	prematureJobDequeuedEvent *m.JobDequeuedEvent

	// `workflow.started` event that arrived before the `job.dequeued` event, see sequencing note in README
	prematureWorkflowStartedEvent *m.WorkflowStartedEvent

	jobID      string
	enqueuing  Enqueuing
	hasStalled bool

	lastUpdated time.Time
	mu          sync.Mutex
}

func newExecutionState() *ExecutionState {
	return &ExecutionState{
		openNodeSpans:               make(map[string]trace.Span),
		openTaskSpans:               make(map[string]trace.Span),
		prematureNodeFinishedEvents: make(map[string]m.NodeFinishedEvent),
		hasStalled:                  false,
		prematureTaskFinishedEvents: make(map[string]m.RunnerResponseReceivedEvent),
		lastUpdated:                 time.Now(),
	}
}

type Enqueuing struct {
	HostID    string
	Timestamp string
}

type Tracer struct {
	tracer     trace.Tracer
	executions map[ExecutionID]*ExecutionState
	mu         sync.Mutex
	ctx        context.Context
	config     config.N8NConfig
}

func NewTracer(cfg config.N8NConfig) *Tracer {
	return &Tracer{
		tracer:     otel.Tracer("n8n-tracer"),
		executions: make(map[string]*ExecutionState),
		ctx:        context.Background(),
		config:     cfg,
	}
}

func (t *Tracer) getOrCreateExecutionState(executionID string) *ExecutionState {
	t.mu.Lock()
	defer t.mu.Unlock()

	state, ok := t.executions[executionID]
	if !ok {
		state = newExecutionState()
		t.executions[executionID] = state
	}
	return state
}

func getExecutionID(event interface{}) string {
	v := reflect.ValueOf(event)
	if v.Kind() != reflect.Struct {
		return ""
	}

	payloadField := v.FieldByName("Payload")
	if !payloadField.IsValid() {
		return ""
	}

	executionIDField := payloadField.FieldByName("ExecutionID")
	if !executionIDField.IsValid() || executionIDField.Kind() != reflect.String {
		return ""
	}

	return executionIDField.String()
}

func (t *Tracer) ProcessEvent(event interface{}) error {
	if _, ok := event.(m.DestinationTestEvent); ok {
		log.Print("Received destination test event")
		return nil
	}

	executionID := getExecutionID(event)
	if executionID == "" {
		return fmt.Errorf("failed to find executionID for event: %T", event)
	}

	if os.Getenv("VERBOSE") != "" {
		// TODO: Leveled logging
		eventType := reflect.TypeOf(event).Name()
		log.Printf("VERBOSE: Processing %s for execution %s", eventType, executionID)
	}

	state := t.getOrCreateExecutionState(executionID)
	state.mu.Lock()
	defer state.mu.Unlock()

	state.lastUpdated = time.Now()

	isFinalEvent := false // whether this event marks the end of an execution lifecycle

	switch e := event.(type) {
	case m.JobEnqueuedEvent:
		t.handleJobEnqueued(state, e)
	case m.JobDequeuedEvent:
		t.handleJobDequeued(state, e)
	case m.JobCompletedEvent:
		isFinalEvent = true
		t.handleJobCompleted(state, e)
	case m.JobFailedEvent:
		isFinalEvent = true
		t.handleJobFailed(state, e)
	case m.JobStalledEvent:
		t.handleJobStalled(state, e)
	case m.WorkflowStartedEvent:
		t.handleWorkflowStarted(state, e)
	case m.WorkflowSuccessEvent:
		if state.jobID == "" {
			isFinalEvent = true
		}
		t.handleWorkflowSuccess(state, e)
	case m.WorkflowFailedEvent:
		if state.jobID == "" {
			isFinalEvent = true
		}
		t.handleWorkflowFailed(state, e)
	case m.NodeStartedEvent:
		t.handleNodeStarted(state, e)
	case m.NodeFinishedEvent:
		t.handleNodeFinished(state, e)
	case m.RunnerTaskRequestedEvent:
		t.handleTaskStarted(state, e)
	case m.RunnerResponseReceivedEvent:
		t.handleTaskFinished(state, e)
	}

	if isFinalEvent {
		t.clearExecution(state, executionID)
	}

	return nil
}

// ----------------
//       job
// ----------------

func (t *Tracer) handleJobEnqueued(state *ExecutionState, event m.JobEnqueuedEvent) {
	if state.JobLifetimeSpan != nil {
		return // No need to process - job enqueued event already arrived
	}

	_, span := s.NewJobLifetimeSpan(t.ctx, t.tracer, event)
	state.JobLifetimeSpan = span
	state.enqueuing = Enqueuing{Timestamp: event.Timestamp, HostID: event.Payload.HostID}
	state.jobID = event.Payload.JobID
}

func (t *Tracer) handleJobDequeued(state *ExecutionState, event m.JobDequeuedEvent) {
	if state.JobLifetimeSpan == nil {
		state.prematureJobDequeuedEvent = &event // Cannot find root span, so buffer this event
		return
	}

	if state.JobProcessingSpan != nil {
		return // No need to process - job dequeued event already arrived
	}

	spanCtx := s.SpanContext{
		Ctx:               t.ctx,
		Tracer:            t.tracer,
		ParentSpan:        state.JobLifetimeSpan,
		ProducerHostID:    state.enqueuing.HostID,
		EnqueuedTimestamp: state.enqueuing.Timestamp,
	}
	s.NewJobPendingSpan(spanCtx, event)

	jobProcessingStartEvent := event
	if dequeuedTime := timeutil.ParseEventTime(event.Timestamp); !dequeuedTime.IsZero() {
		// WORKAROUND: Create an artificial 1 microsecond delay to force tracing UIs to render self-time correctly.
		// Without this gap, the `job.pending` span ends at the exact timestamp that `job.processing` begins,
		// causing queue self-time to be misrendered on the parent span `job.lifetime`.
		jobProcessingStartEvent.Timestamp = dequeuedTime.Add(1 * time.Microsecond).Format(time.RFC3339Nano)
	}
	_, span := s.NewJobProcessingSpan(spanCtx, jobProcessingStartEvent)
	state.JobProcessingSpan = span

	if state.hasStalled {
		// If this job previously stalled, there will be no `n8n.workflow.started` event, because main sends
		// this event _only once_ on enqueuing. Hence we create a `workflow.executing` span on the fly.

		syntheticWorkflowStartedEvent := m.WorkflowStartedEvent{
			Timestamp: event.Timestamp,
			Payload: m.WorkflowStartedPayload{
				BasePayload: m.BasePayload{
					ExecutionID: event.Payload.ExecutionID,
					WorkflowID:  event.Payload.WorkflowID,
					HostID:      event.Payload.HostID,
				},
			},
		}

		if dequeuedTime := timeutil.ParseEventTime(event.Timestamp); !dequeuedTime.IsZero() {
			// WORKAROUND: Override workflow start time to be 50ms after `job.dequeued`. See sequencing note in README.
			correctedStartTime := dequeuedTime.Add(t.config.WorkflowStartOffset)
			syntheticWorkflowStartedEvent.Timestamp = correctedStartTime.Format(time.RFC3339Nano)
		}

		workflowSpanCtx := s.SpanContext{
			Ctx:        t.ctx,
			Tracer:     t.tracer,
			ParentSpan: state.JobProcessingSpan,
			JobID:      state.jobID,
		}

		_, workflowSpan := s.NewWorkflowSpan(workflowSpanCtx, syntheticWorkflowStartedEvent)
		state.WorkflowSpan = workflowSpan

		state.hasStalled = false
		return
	}

	if state.prematureWorkflowStartedEvent != nil {
		workflowEvent := *state.prematureWorkflowStartedEvent
		state.prematureWorkflowStartedEvent = nil

		if dequeuedTime := timeutil.ParseEventTime(event.Timestamp); !dequeuedTime.IsZero() {
			// WORKAROUND: Override workflow start time to be 50ms after `job.dequeued`. See sequencing note in README.
			correctedStartTime := dequeuedTime.Add(t.config.WorkflowStartOffset)
			workflowEvent.Timestamp = correctedStartTime.Format(time.RFC3339Nano)
		}

		spanCtx := s.SpanContext{
			Ctx:        t.ctx,
			Tracer:     t.tracer,
			ParentSpan: state.JobProcessingSpan,
			JobID:      state.jobID,
		}
		_, workflowSpan := s.NewWorkflowSpan(spanCtx, workflowEvent)
		state.WorkflowSpan = workflowSpan
	}
}

func (t *Tracer) handleJobCompleted(state *ExecutionState, event m.JobCompletedEvent) {
	if state.JobProcessingSpan != nil {
		s.EndJobSpan(state.JobProcessingSpan, event.Timestamp)
	}

	if state.JobLifetimeSpan != nil {
		s.EndJobSpan(state.JobLifetimeSpan, event.Timestamp)
	}
}

func (t *Tracer) handleJobFailed(state *ExecutionState, event m.JobFailedEvent) {
	if state.JobProcessingSpan != nil {
		s.EndJobSpan(state.JobProcessingSpan, event.Timestamp)
	}

	if state.JobLifetimeSpan != nil {
		s.EndJobSpan(state.JobLifetimeSpan, event.Timestamp)
	}
}

func (t *Tracer) handleJobStalled(state *ExecutionState, event m.JobStalledEvent) {
	if state.JobLifetimeSpan == nil {
		return // No job to stall - stall event arrived before enqueue event
	}

	s.AddJobStalledEvent(state.JobLifetimeSpan, event)

	// Terminate all open spans

	for nodeID, span := range state.openNodeSpans {
		s.EndNodeSpanOnStall(span, event)
		delete(state.openNodeSpans, nodeID)
	}

	for taskID, span := range state.openTaskSpans {
		s.EndTaskSpanOnStall(span, event)
		delete(state.openTaskSpans, taskID)
	}

	if state.WorkflowSpan != nil {
		s.EndWorkflowSpanOnStall(state.WorkflowSpan, event)
		state.WorkflowSpan = nil
	}

	if state.JobProcessingSpan != nil {
		s.EndJobProcessingSpanOnStall(state.JobProcessingSpan, event)
		state.JobProcessingSpan = nil
	}

	// On stalling, Bull makes the job available again for pickup, without re-enqueuing.
	// But for tracing we mark it as re-enqueued by the stalling host, so that the
	// upcoming `job.pending` span has a start time and so can be created.
	state.enqueuing = Enqueuing{Timestamp: event.Timestamp, HostID: event.Payload.HostID}

	state.hasStalled = true
	state.prematureWorkflowStartedEvent = nil
}

// ----------------
//    workflow
// ----------------

func (t *Tracer) handleWorkflowStarted(state *ExecutionState, event m.WorkflowStartedEvent) {
	if state.WorkflowSpan != nil {
		return // No need to process - workflow started event already arrived
	}

	if t.config.DeploymentMode == "scaling" && state.JobProcessingSpan == nil {
		// In scaling mode, a workflow is always part of a job and _must_ have a parent span.
		// If the `job.processing` parent span is not ready yet, we buffer this event.
		state.prematureWorkflowStartedEvent = &event
		return
	}

	parentSpan := state.JobProcessingSpan // In regular mode parent is nil
	spanCtx := s.SpanContext{
		Ctx:        t.ctx,
		Tracer:     t.tracer,
		ParentSpan: parentSpan,
		JobID:      state.jobID,
	}
	_, span := s.NewWorkflowSpan(spanCtx, event)
	state.WorkflowSpan = span
}

func (t *Tracer) handleWorkflowSuccess(state *ExecutionState, event m.WorkflowSuccessEvent) {
	if state.WorkflowSpan == nil {
		return // TODO: Handle
	}

	s.AddWorkflowSuccessAttributes(state.WorkflowSpan, event)
	s.EndWorkflowSpan(state.WorkflowSpan, event.Timestamp)
}

func (t *Tracer) handleWorkflowFailed(state *ExecutionState, event m.WorkflowFailedEvent) {
	if state.WorkflowSpan == nil {
		return // TODO: Handle
	}

	for nodeID, span := range state.openNodeSpans {
		s.EndNodeSpan(span, event.Timestamp)
		delete(state.openNodeSpans, nodeID)
	}

	for taskID, span := range state.openTaskSpans {
		s.EndTaskSpan(span, event.Timestamp)
		delete(state.openTaskSpans, taskID)
	}

	s.AddWorkflowFailureAttributes(state.WorkflowSpan, event)
	s.EndWorkflowSpan(state.WorkflowSpan, event.Timestamp)
}

// ----------------
//      node
// ----------------

func (t *Tracer) handleNodeStarted(state *ExecutionState, event m.NodeStartedEvent) {
	nodeID := event.Payload.NodeID

	if _, ok := state.openNodeSpans[nodeID]; ok {
		return // No need to process, `node.started` event already arrived
	}

	parentSpan := state.WorkflowSpan
	spanCtx := s.SpanContext{
		Ctx:        t.ctx,
		Tracer:     t.tracer,
		ParentSpan: parentSpan,
		JobID:      state.jobID,
	}
	_, span := s.NewNodeSpan(spanCtx, event)

	if parentSpan == nil {
		warning, _ := s.SetOrphanAttributes(span, s.ReasonMissingWorkflowSpan, event.Payload.ExecutionID)
		log.Print(warning)
	}

	if finishEvent, ok := state.prematureNodeFinishedEvents[nodeID]; ok {
		// `node.finished` event arrived before this `node.started` event
		s.EndNodeSpan(span, finishEvent.Timestamp)
		delete(state.prematureNodeFinishedEvents, nodeID)
		return
	}

	state.openNodeSpans[nodeID] = span
}

func (t *Tracer) handleNodeFinished(state *ExecutionState, event m.NodeFinishedEvent) {
	nodeID := event.Payload.NodeID

	if _, ok := state.openNodeSpans[nodeID]; !ok {
		// Cannot find `node.started` event, so buffer this `node.finished` event
		state.prematureNodeFinishedEvents[nodeID] = event
		return
	}

	span := state.openNodeSpans[nodeID]
	s.EndNodeSpan(span, event.Timestamp)
	delete(state.openNodeSpans, nodeID)
}

// ----------------
//      task
// ----------------

func (t *Tracer) handleTaskStarted(state *ExecutionState, event m.RunnerTaskRequestedEvent) {
	taskID := event.Payload.TaskID

	if _, ok := state.openTaskSpans[taskID]; ok {
		return // No need to process, `task.started` event already arrived
	}

	parentSpan := state.openNodeSpans[event.Payload.NodeID]
	spanCtx := s.SpanContext{
		Ctx:        t.ctx,
		Tracer:     t.tracer,
		ParentSpan: parentSpan,
		JobID:      state.jobID,
	}
	_, span := s.NewTaskSpan(spanCtx, event)

	if parentSpan == nil {
		warning, _ := s.SetOrphanAttributes(span, s.ReasonMissingNodeSpan, event.Payload.ExecutionID)
		log.Print(warning)
	}

	if finishEvent, ok := state.prematureTaskFinishedEvents[taskID]; ok {
		// Cannot find `task.started` event, so buffer this `task.finished` event
		s.EndTaskSpan(span, finishEvent.Timestamp)
		delete(state.prematureTaskFinishedEvents, taskID)
		return
	}

	state.openTaskSpans[taskID] = span
}

func (t *Tracer) handleTaskFinished(state *ExecutionState, event m.RunnerResponseReceivedEvent) {
	taskID := event.Payload.TaskID

	if _, ok := state.openTaskSpans[taskID]; !ok {
		// Cannot find `task.started` event, so buffer this `task.finished` event
		state.prematureTaskFinishedEvents[taskID] = event
		return
	}

	span := state.openTaskSpans[taskID]
	s.EndTaskSpan(span, event.Timestamp)
	delete(state.openTaskSpans, taskID)
}

// ----------------
//     memory
// ----------------

func (t *Tracer) clearExecution(state *ExecutionState, executionID string) {
	nowStr := time.Now().Format(time.RFC3339Nano)

	// Terminate any spans that were never closed
	for _, span := range state.openTaskSpans {
		s.EndTaskSpan(span, nowStr)
	}

	for _, span := range state.openNodeSpans {
		s.EndNodeSpan(span, nowStr)
	}

	if state.WorkflowSpan != nil {
		s.EndWorkflowSpan(state.WorkflowSpan, nowStr)
	}

	t.mu.Lock()
	delete(t.executions, executionID)
	t.mu.Unlock()
}

// StartGC runs a periodic garbage collector for stale execution states.
func (t *Tracer) StartGC(ctx context.Context, staleThreshold time.Duration, gcInterval time.Duration) {
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()

	log.Printf("Starting GC to clear spans older than %v every %v", staleThreshold, gcInterval)

	for {
		select {
		case <-ticker.C:
			t.clearStaleSpans(staleThreshold)
		case <-ctx.Done():
			return
		}
	}
}

func (t *Tracer) clearStaleSpans(staleThreshold time.Duration) {
	t.mu.Lock()
	staleExecutions := make(map[string]*ExecutionState)
	for executionID, state := range t.executions {
		state.mu.Lock()
		if time.Since(state.lastUpdated) > staleThreshold {
			staleExecutions[executionID] = state
		}
		state.mu.Unlock()
	}
	t.mu.Unlock()

	if len(staleExecutions) == 0 {
		return
	}

	log.Printf("GC found %d stale executions to clean up", len(staleExecutions))

	nowStr := time.Now().Format(time.RFC3339Nano)

	for executionID, state := range staleExecutions {
		state.mu.Lock()
		// Terminate all open spans
		for _, span := range state.openTaskSpans {
			s.EndTaskSpan(span, nowStr)
		}

		for _, span := range state.openNodeSpans {
			s.EndNodeSpan(span, nowStr)
		}

		if state.WorkflowSpan != nil {
			s.EndWorkflowSpan(state.WorkflowSpan, nowStr)
		}

		if state.JobProcessingSpan != nil {
			s.EndJobSpan(state.JobProcessingSpan, nowStr)
		}

		if state.JobLifetimeSpan != nil {
			s.EndJobSpan(state.JobLifetimeSpan, nowStr)
		}

		state.mu.Unlock()

		t.mu.Lock()
		delete(t.executions, executionID)
		t.mu.Unlock()
	}
}

func (t *Tracer) ExecutionStatesInMemory() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.executions)
}
