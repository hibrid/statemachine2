package statemachine

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

type StateTransitionError struct {
	StateMachineError
	FromState State
	ToState   State
	Event     Event
}

func NewStateTransitionError(fromState State, toState State, event Event) *StateTransitionError {
	return &StateTransitionError{
		StateMachineError: StateMachineError{Msg: fmt.Sprintf("invalid state transition from %s to %s on event %s", fromState, toState, event)},
		FromState:         fromState,
		ToState:           toState,
		Event:             event,
	}
}

type DatabaseOperationError struct {
	StateMachineError
	Operation string
}

func NewDatabaseOperationError(operation string, err error) *DatabaseOperationError {
	return &DatabaseOperationError{
		StateMachineError: StateMachineError{Msg: fmt.Sprintf("database operation error in %s: %v", operation, err)},
		Operation:         operation,
	}
}

type LockAcquisitionError struct {
	StateMachineError
	LockType LockType
	Detail   string
}

func NewLockAcquisitionError(lockType LockType, detail string, err error) *LockAcquisitionError {
	return &LockAcquisitionError{
		StateMachineError: StateMachineError{Msg: fmt.Sprintf("error acquiring %s lock: %s, detail: %v", lockType, detail, err)},
		LockType:          lockType,
		Detail:            detail,
	}
}

type LockAlreadyHeldError struct {
	StateMachineError
	LockType LockType
}

func NewLockAlreadyHeldError(lockType LockType) *LockAlreadyHeldError {
	return &LockAlreadyHeldError{
		StateMachineError: StateMachineError{Msg: fmt.Sprintf("lock of type %s is already held by another instance", lockType)},
		LockType:          lockType,
	}
}

type Callback func(*StateMachine, *Context) error
type StateCallbacks struct {
	AfterAnEvent  Callback // callback executes immediately after the event is handled and before any state transition
	BeforeTheStep Callback // callback executes immediately after the state transition
	AfterTheStep  Callback // callback executes immediately before the state transition
}

type ForwardEvent int

const (
	ForwardSuccess ForwardEvent = iota
	ForwardFail
	ForwardComplete
	ForwardPause
	ForwardRollback
	ForwardRetry
	ForwardLock
	ForwardCancel
	ForwardUnknown
)

func (e ForwardEvent) String() string {
	switch e {
	case ForwardSuccess:
		return "ForwardSuccess"
	case ForwardFail:
		return "ForwardFail"
	case ForwardComplete:
		return "ForwardComplete"
	case ForwardPause:
		return "ForwardPause"
	case ForwardRollback:
		return "ForwardRollback"
	case ForwardRetry:
		return "ForwardRetry"
	case ForwardLock:
		return "ForwardLock"
	case ForwardCancel:
		return "ForwardCancel"
	default:
		return fmt.Sprintf("UnknownForwardEvent(%d)", e)
	}
}

// convert the ForwardEvent to a Event
func (e ForwardEvent) ToEvent() Event {
	switch e {
	case ForwardSuccess:
		return OnSuccess
	case ForwardFail:
		return OnFailed
	case ForwardComplete:
		return OnCompleted
	case ForwardPause:
		return OnPause
	case ForwardRollback:
		return OnRollback
	case ForwardRetry:
		return OnRetry
	case ForwardLock:
		return OnLock
	case ForwardCancel:
		return OnCancelled
	default:
		return OnUnknownSituation
	}
}

type BackwardEvent int

const (
	BackwardSuccess BackwardEvent = iota
	BackwardComplete
	BackwardFail
	BackwardRollback
	BackwardCancel
	BackwardRetry
	BackwardUnknown
)

func (e BackwardEvent) String() string {
	switch e {
	case BackwardSuccess:
		return "BackwardSuccess"
	case BackwardComplete:
		return "BackwardComplete"
	case BackwardFail:
		return "BackwardFail"
	case BackwardRollback:
		return "BackwardRollback"
	case BackwardCancel:
		return "BackwardCancel"
	case BackwardRetry:
		return "BackwardRetry"
	default:
		return fmt.Sprintf("UnknownBackwardEvent(%d)", e)
	}
}

// convert the BackwardEvent to a Event
func (e BackwardEvent) ToEvent() Event {
	switch e {
	case BackwardSuccess:
		return OnRollback
	case BackwardComplete:
		return OnRollbackCompleted
	case BackwardFail:
		return OnFailed
	case BackwardRollback:
		return OnRollback
	case BackwardCancel:
		return OnCancelled
	case BackwardRetry:
		return OnRetry
	default:
		return OnUnknownSituation
	}
}

type PauseEvent int

const (
	PauseSuccess PauseEvent = iota
	PauseFail
	PauseUnknown
)

func (e PauseEvent) String() string {
	switch e {
	case PauseSuccess:
		return "PauseSuccess"
	case PauseFail:
		return "PauseFail"
	default:
		return fmt.Sprintf("UnknownPauseEvent(%d)", e)
	}
}

// convert the PauseEvent to a Event
func (e PauseEvent) ToEvent() Event {
	switch e {
	case PauseSuccess:
		return OnPause
	case PauseFail:
		return OnFailed
	default:
		return OnUnknownSituation
	}
}

type RetryEvent int

const (
	RetrySuccess RetryEvent = iota
	RetryFail
	RetryRetry
	RetryLock
	RetryCancel
	RetryResetTimeout
	RetryUnknown
)

func (e RetryEvent) String() string {
	switch e {
	case RetrySuccess:
		return "RetrySuccess"
	case RetryFail:
		return "RetryFail"
	case RetryRetry:
		return "RetryRetry"
	case RetryLock:
		return "RetryLock"
	case RetryCancel:
		return "RetryCancel"
	case RetryResetTimeout:
		return "RetryResetTimeout"
	default:
		return fmt.Sprintf("UnknownRetryEvent(%d)", e)
	}
}

// convert the RetryEvent to a Event
func (e RetryEvent) ToEvent() Event {
	switch e {
	case RetrySuccess:
		return OnSuccess
	case RetryFail:
		return OnFailed
	case RetryRetry:
		return OnRetry
	case RetryLock:
		return OnLock
	case RetryCancel:
		return OnCancelled
	case RetryResetTimeout:
		return OnResetTimeout
	default:
		return OnUnknownSituation
	}
}

type ManualOverrideEvent int

const (
	ManualOverrideAny ManualOverrideEvent = iota
	ManualOverrideUnknown
)

func (e ManualOverrideEvent) String() string {
	switch e {
	case ManualOverrideAny:
		return "ManualOverrideAny"
	default:
		return fmt.Sprintf("UnknownManualOverrideEvent(%d)", e)
	}
}

// convert the ManualOverrideEvent to a Event
func (e ManualOverrideEvent) ToEvent() Event {
	switch e {
	case ManualOverrideAny:
		return OnManualOverride
	default:
		return OnUnknownSituation
	}
}

type ResumeEvent int

const (
	ResumeSuccess ResumeEvent = iota
	ResumeFail
)

func (e ResumeEvent) String() string {
	switch e {
	case ResumeSuccess:
		return "ResumeSuccess"
	case ResumeFail:
		return "ResumeFail"
	default:
		return fmt.Sprintf("UnknownResumeEvent(%d)", e)
	}
}

// convert the ResumeEvent to a Event
func (e ResumeEvent) ToEvent() Event {
	switch e {
	case ResumeSuccess:
		return OnSuccess
	case ResumeFail:
		return OnFailed
	default:
		return OnUnknownSituation
	}
}

// Event represents an event that triggers state transitions.
type Event int

const (
	OnSuccess Event = iota
	OnFailed
	OnAlreadyCompleted
	OnPause
	OnResume
	OnRollback
	OnRetry
	OnResetTimeout
	OnUnknownSituation
	OnManualOverride
	OnError
	OnBeforeEvent
	OnAfterEvent
	OnCompleted
	OnRollbackCompleted
	OnAlreadyRollbackCompleted
	OnRollbackFailed
	OnCancelled
	OnParked
	OnLock
)

func (e Event) String() string {
	switch e {
	case OnSuccess:
		return "OnSuccess"
	case OnFailed:
		return "OnFailed"
	case OnAlreadyCompleted:
		return "OnAlreadyCompleted"
	case OnPause:
		return "OnPause"
	case OnResume:
		return "OnResume"
	case OnRollback:
		return "OnRollback"
	case OnRetry:
		return "OnRetry"
	case OnResetTimeout:
		return "OnResetTimeout"
	case OnUnknownSituation:
		return "OnUnknownSituation"
	case OnManualOverride:
		return "OnManualOverride"
	case OnError:
		return "OnError"
	case OnBeforeEvent:
		return "OnBeforeEvent"
	case OnAfterEvent:
		return "OnAfterEvent"
	case OnCompleted:
		return "OnCompleted"
	case OnRollbackCompleted:
		return "OnRollbackCompleted"
	case OnAlreadyRollbackCompleted:
		return "OnAlreadyRollbackCompleted"
	case OnRollbackFailed:
		return "OnRollbackFailed"
	case OnCancelled:
		return "OnCancelled"
	case OnParked:
		return "OnParked"
	case OnLock:
		return "OnLock"
	default:
		return fmt.Sprintf("UnknownEvent(%d)", e)
	}
}

type State string

func (s State) String() string {
	return string(s)
}

const (

	// StatePending is the initial state of a state machine waiting to be picked up.
	// ExecuteForward
	StatePending State = "pending"

	// StateOpen is the state of a state machine when it is in progress and not locked.
	// ExecuteForward
	StateOpen State = "open"

	// StateInProgress is the state of a state machine when it is in progress and locked.
	// ExecuteForward
	StateInProgress State = "in_progress"

	// StateCompleted is the final state of a state machine.
	// AlreadyCompleted - No action
	StateCompleted State = "completed"

	// StateFailed is the state of a state machine when it fails.
	// AlreadyFailed - No action
	StateFailed State = "failed"

	// StateRetry is the state of a state machine when it needs to retry later.
	StateRetry State = "retry"

	// StateRetryStart is the state of a state machine when it needs to retry later.
	// ExecuteForward and change state in machine to StateRetry
	StateRetryStart State = "start_retry"

	// StateRetryFailed is the state of a state machine when its retry fails.
	StateRetryFailed State = "retry_failed"

	// StateFailed is the state of a state machine when it fails.
	// ExecuteBackward
	StateRollback State = "rollback"

	// We are going to start the rollback process
	// ExecuteBackward and change state in machine to StateRollback
	StateStartRollback State = "start_rollback"

	// StateRollbackFailed is the state of a state machine when its rollback fails.
	// AlreadyRollbackFailed - No action
	StateRollbackFailed State = "rollback_failed"

	// StateRollbackCompleted is the state of a state machine when its rollback is completed.
	// AlreadyRollbackCompleted - No action
	StateRollbackCompleted State = "rollback_completed"

	// StatePaused is the state of a state machine when it is paused.
	// ExecutePause
	StatePaused State = "paused"

	// StatePaused is the state of a state machine when it is paused.
	// ExecuteResume
	StateResume State = "resume"

	// StateCancelled is the state of a state machine when it is cancelled.
	// AlreadyCancelled - No action
	StateCancelled State = "cancelled"

	// StateParked is the state of a state machine when it is parked because we don't know what to do with it.
	// AlreadyParked - No Action
	StateParked State = "parked"

	// StateUnknown is the state of a state machine when it is in an unknown state.
	StateUnknown State = "unknown"

	// AnyState is a special state that can be used to indicate that a transition can happen from any state.
	AnyState State = "any"
)

// StateMachine represents a state machine instance.
type StateMachine struct {
	Name                          string                    `json:"name"`
	UniqueID                      string                    `json:"id"`
	Handlers                      []StepHandler             `json:"-"` // List of handlers with names
	Callbacks                     map[string]StateCallbacks `json:"-"`
	CurrentState                  State                     `json:"currentState"`
	DB                            *sql.DB                   `json:"-"`
	KafkaProducer                 *kafka.Producer           `json:"-"`
	LookupKey                     string                    `json:"lookupKey"`                     // User or account ID for locking
	ResumeFromStep                int                       `json:"resumeFromStep"`                // Step to resume from when recreated
	SaveAfterEachStep             bool                      `json:"saveAfterStep"`                 // Flag to save state after each step
	UnlockGlobalLockAfterEachStep bool                      `json:"unlockGlobalLockAfterEachStep"` // Flag to unlock global lock after each step
	KafkaEventTopic               string                    `json:"kafkaEventTopic"`               // Kafka topic to send events
	History                       []TransitionHistory       `json:"history"`                       // History of executed transitions
	ExecuteSynchronously          bool                      `json:"executeSynchronously"`          // Flag to control whether to execute the next step immediately

	CurrentArbitraryData map[string]interface{} `json:"currentArbitraryData"` // Additional field for storing arbitrary data
	CreatedTimestamp     time.Time              `json:"createdTimestamp"`
	UpdatedTimestamp     time.Time              `json:"updatedTimestamp"`
	UnlockedTimestamp    *time.Time             `json:"unlockedTimestamp"`
	LockType             LockType               `json:"lockType"`
	RetryCount           int                    `json:"retryCount"`
	RetryType            RetryType              `json:"retryType"`
	MaxTimeout           time.Duration          `json:"maxTimeout"`
	BaseDelay            time.Duration          `json:"baseDelay"`
	LastRetry            *time.Time             `json:"lastRetry"`
	SerializedState      []byte                 `json:"-"`
}

type LockType int

const (
	NoLock LockType = iota
	GlobalLock
	LocalLock
)

func (lt LockType) String() string {
	switch lt {
	case NoLock:
		return "NoLock"
	case GlobalLock:
		return "GlobalLock"
	case LocalLock:
		return "LocalLock"
	default:
		return fmt.Sprintf("UnknownLockType(%d)", lt)
	}
}

type RetryType string

const (
	ExponentialBackoff RetryType = "exponential_backoff"
)

type RetryPolicy struct {
	RetryType  RetryType     `json:"retryType"`  // Type of retry policy
	MaxTimeout time.Duration `json:"maxTimeout"` // Maximum timeout for retries
	BaseDelay  time.Duration `jason:"baseDelay"` // Base delay for retries
}

type StateMachineConfig struct {
	Name                 string
	UniqueStateMachineID string
	LookupKey            string
	DB                   *sql.DB
	KafkaProducer        *kafka.Producer
	KafkaEventTopic      string
	ExecuteSynchronously bool
	Handlers             []StepHandler
	RetryPolicy          RetryPolicy
	LockType             LockType
}

// TransitionHistory stores information about executed transitions.
type TransitionHistory struct {
	FromStep            int                    `json:"fromStep"`            // Index of the "from" step
	ToStep              int                    `json:"toStep"`              // Index of the "to" step
	HandlerName         string                 `json:"handlerName"`         // Name of the handler
	InitialState        State                  `json:"initialState"`        // Initial state
	ModifiedState       State                  `json:"modifiedState"`       // Modified state
	InputArbitraryData  map[string]interface{} `json:"inputArbitraryData"`  // Arbitrary data associated with the transition
	OutputArbitraryData map[string]interface{} `json:"outputArbitraryData"` // Arbitrary data associated with the transition
	EventEmitted        Event                  `json:"eventEmitted"`        // Event emitted by the transition
}

// StepHandler defines the interface for state machine handlers.
type StepHandler interface {
	Name() string
	ExecuteForward(data map[string]interface{}, transitionHistory []TransitionHistory) (ForwardEvent, map[string]interface{}, error)
	ExecuteBackward(data map[string]interface{}, transitionHistory []TransitionHistory) (BackwardEvent, map[string]interface{}, error)
	ExecutePause(data map[string]interface{}, transitionHistory []TransitionHistory) (PauseEvent, map[string]interface{}, error)
	ExecuteResume(data map[string]interface{}, transitionHistory []TransitionHistory) (ResumeEvent, map[string]interface{}, error)
}

// TerminalStates defines the states where the state machine stops processing.
var TerminalStates = map[State]bool{
	StateCompleted:         true,
	StateFailed:            true,
	StateRollbackCompleted: true,
	StateCancelled:         true,
}

// IsTerminalState checks if the given state is a terminal state.
func IsTerminalState(state State) bool {
	_, isTerminal := TerminalStates[state]
	return isTerminal
}

var ValidTransitions = map[State]map[Event][]State{
	StatePending: {
		OnSuccess:   []State{StateOpen},
		OnFailed:    []State{StateFailed},
		OnCancelled: []State{StateCancelled},

		OnUnknownSituation: []State{StateParked},
	},
	StateOpen: {
		OnSuccess:   []State{StateOpen},
		OnCompleted: []State{StateCompleted},
		OnFailed:    []State{StateFailed},
		OnPause:     []State{StatePaused},
		OnRollback:  []State{StateStartRollback},
		OnRetry:     []State{StateRetry, StateRetryStart},
		OnLock:      []State{StateInProgress},
		OnCancelled: []State{StateCancelled},

		OnUnknownSituation: []State{StateParked},
	},
	StateRetry: {
		OnSuccess:      []State{StateOpen},
		OnFailed:       []State{StateFailed},
		OnRetry:        []State{StateRetry},
		OnLock:         []State{StateInProgress},
		OnCancelled:    []State{StateCancelled},
		OnResetTimeout: []State{StateFailed},

		OnUnknownSituation: []State{StateParked},
	},
	StateRollback: {
		OnSuccess:           []State{StateOpen},
		OnRetry:             []State{StateRetry, StateRetryStart},
		OnRollbackCompleted: []State{StateRollbackCompleted},
		OnRollbackFailed:    []State{StateRollbackFailed},
		OnRollback:          []State{StateRollback},
		OnCancelled:         []State{StateCancelled},

		OnUnknownSituation: []State{StateParked},
	},
	StatePaused: {
		OnResume: []State{StateOpen},
		OnFailed: []State{StateFailed},
	},
	StateParked: {
		OnManualOverride: []State{AnyState},
	},
}

func deserializeFromJSON(data []byte) (map[string]interface{}, error) {
	var mapData map[string]interface{}
	if err := json.Unmarshal(data, &mapData); err != nil {
		return nil, err
	}
	return mapData, nil
}

func (sm *StateMachine) serializeToJSON() ([]byte, error) {
	serialized, err := json.Marshal(sm)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

// Save the state machine's serialized JSON to the database
func (sm *StateMachine) saveStateToDB() error {
	if err := updateStateMachineState(sm, sm.CurrentState); err != nil {
		return NewDatabaseOperationError("updateStateMachineState", err)
	}
	return nil
}

// TODO: create unit tests for this
// Load the serialized JSON state data from the database
func loadStateMachineFromDB(stateMachineType string, id string, db *sql.DB) (*StateMachine, error) {
	// Load serialized state data from the database (replace with your database logic)
	sm := &StateMachine{
		UniqueID: id,
		Name:     stateMachineType,
		DB:       db,
	}
	sm, err := loadAndLockStateMachine(sm)
	if err != nil {
		return nil, NewDatabaseOperationError("loadAndLockStateMachine", err)
	}
	return sm, nil
}

// TODO: create unit tests for this
// processStateMachine processes the state machine based on the current context.
func (sm *StateMachine) processStateMachine(context *Context) error {
	// Check that CurrentArbitraryData is not nil
	if sm.CurrentArbitraryData == nil {
		sm.CurrentArbitraryData = make(map[string]interface{})
	}

	var handler StepHandler
	// Let's check if this is a success and we are done
	if sm.ResumeFromStep >= len(sm.Handlers) || sm.ResumeFromStep < 0 {
		handler = &completeHandler{}
	} else {
		handler = sm.Handlers[sm.ResumeFromStep]
	}

	context.Handler = handler

	executionEvent, err := context.Handle()
	if err != nil {
		return err
	}

	return sm.HandleEvent(context, executionEvent)
}

// createContext creates a new context for the state machine.
// important that we need to copy the map to avoid referencing the same space in memory as we manipulate the data
func (sm *StateMachine) createContext() *Context {
	return &Context{
		InputState:          sm.CurrentState,
		InputArbitraryData:  CopyMap(sm.CurrentArbitraryData),
		OutputArbitraryData: make(map[string]interface{}),
		StepNumber:          sm.ResumeFromStep,
		TransitionHistory:   sm.History,
		StateMachine:        sm,
	}
}

// validateHandlers checks if the state machine has valid handlers.
func (sm *StateMachine) validateHandlers() error {
	if sm.Handlers == nil || len(sm.Handlers) == 0 {
		return fmt.Errorf("no handlers found")
	}
	return nil
}

func (sm *StateMachine) HandleEvent(context *Context, event Event) error {
	return sm.handleTransition(context, event)
}

func (sm *StateMachine) handleTransition(context *Context, event Event) error {
	if err := sm.executeLeaveStateCallback(context); err != nil {
		return err
	}

	newState, shouldRetry, err := sm.determineNewState(context, event)
	if err != nil {
		return err
	}

	if err := sm.updateStateMachineState(context, newState, event); err != nil {
		return err
	}

	if err := sm.executeEnterStateCallback(context); err != nil {
		return err
	}

	if err := sm.handleRetryLogic(shouldRetry); err != nil {
		if err := sm.updateStateMachineState(context, StateFailed, event); err != nil {
			return err
		}
	}

	if err := sm.handleSynchoronousExecution(context); err != nil {
		return err
	}

	if err := sm.handleAsynchronousExecution(context); err != nil {
		return err
	}
	return nil
}

func (sm *StateMachine) handleAsynchronousExecution(context *Context) error {
	if !IsTerminalState(sm.CurrentState) && !sm.ExecuteSynchronously {
		// Send Kafka event if configured
		if sm.KafkaEventTopic != "" {
			if err := sm.sendKafkaEvent(); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

func (sm *StateMachine) handleSynchoronousExecution(context *Context) error {
	if !IsTerminalState(sm.CurrentState) && sm.ExecuteSynchronously {
		return sm.Run()
	}
	return nil
}

func (sm *StateMachine) handleRetryLogic(shouldRetry bool) error {
	if !shouldRetry {
		return nil
	}
	remainingDelay := sm.GetRemainingDelay()
	if remainingDelay < 0 {
		return fmt.Errorf("max timeout reached")
	}
	if remainingDelay > 0 {
		time.Sleep(remainingDelay)
	}

	return nil
}

func (sm *StateMachine) executeLeaveStateCallback(context *Context) error {
	if callbacks, ok := sm.Callbacks[string(sm.CurrentState)]; ok {
		if err := callbacks.AfterTheStep(sm, context); err != nil {
			return err
		}
	}
	return nil
}

func (sm *StateMachine) updateStateMachineState(context *Context, newState State, event Event) error {

	sm.CurrentArbitraryData = context.OutputArbitraryData
	stepNumber := context.StepNumber
	historyEntry := TransitionHistory{
		FromStep:            stepNumber,
		ToStep:              stepNumber,
		HandlerName:         context.Handler.Name(),
		InitialState:        context.InputState,
		ModifiedState:       sm.CurrentState,
		InputArbitraryData:  context.InputArbitraryData,
		OutputArbitraryData: context.OutputArbitraryData,
		EventEmitted:        event,
	}

	switch newState {
	case StateOpen:
		historyEntry.FromStep = stepNumber - 1 // the previous step is the current step minus 1
		sm.ResumeFromStep = stepNumber + 1     // set the resume step to the next step
	case StateStartRollback:
		newState = StateRollback
		// we want to resume from the same step we are on to execute the backward function
	case StateRollback:
		sm.ResumeFromStep = stepNumber - 1
		// we're moving backward direction and the FromStep is the current step
	case StateRetryStart:
		newState = StateRetry
	case StateRetryFailed:
		newState = StateFailed
		fallthrough
	case StateRetry:
		sm.RetryCount++
		lastRetry := time.Now()
		sm.LastRetry = &lastRetry
	}

	sm.CurrentState = newState
	historyEntry.ModifiedState = sm.CurrentState
	sm.History = append(sm.History, historyEntry)
	now := time.Now()
	sm.UnlockedTimestamp = &now // Set the unlocked timestamp to the current time

	if sm.SaveAfterEachStep {
		if err := sm.saveStateToDB(); err != nil {
			// Handle state save error.
			return err
		}
	}
	return nil
}

func (sm *StateMachine) executeEnterStateCallback(context *Context) error {
	if callbacks, ok := sm.Callbacks[string(sm.CurrentState)]; ok {
		if err := callbacks.BeforeTheStep(sm, context); err != nil {
			return err
		}
	}
	return nil
}

func (sm *StateMachine) determineNewState(context *Context, event Event) (State, bool, error) {

	var shouldRetry bool
	var newState State

	// Handle events and transitions using the handler's Execute methods
	switch event {
	case OnCompleted:
		newState = StateCompleted
	case OnRollbackCompleted:
		newState = StateRollbackCompleted
	case OnFailed:
		newState = StateFailed
	case OnSuccess:
		newState = StateOpen
	case OnResetTimeout:
		newState = StateFailed
	case OnPause:
		newState = StatePaused
	case OnAlreadyCompleted:
		newState = sm.CurrentState
	case OnRollback:
		newState = StateRollback
		if sm.CurrentState != StateRollback {
			newState = StateStartRollback
		}
	case OnRollbackFailed:
		newState = StateRollbackFailed
	case OnResume:
		newState = StateOpen
	case OnRetry:
		newState = StateRetry
		if sm.CurrentState != StateRetry {
			newState = StateRetryStart
		}
		shouldRetry = true
	case OnCancelled:
		newState = StateCancelled
	case OnLock:
		newState = StateInProgress
	case OnUnknownSituation:
		newState = StateParked
	default:
		newState = StateParked
		//TODO: Log Error
	}

	if !IsValidTransition(sm.CurrentState, event, newState) {
		return StateUnknown, false, NewStateTransitionError(sm.CurrentState, newState, event)
	}

	return newState, shouldRetry, nil
}

// checkAndAcquireLocks checks if locks exist for the state machine and obtains them if possible.
func (sm *StateMachine) checkAndAcquireLocks() error {

	switch sm.LockType {
	case GlobalLock:
		return sm.checkAndObtainGlobalLock()
	case LocalLock:
		return sm.checkAndObtainLocalLock()
	default:
		return nil // No locking required
	}

}

// checkAndObtainGlobalLock checks if a global lock exists for the state machine and obtains it if possible.
func (sm *StateMachine) checkAndObtainGlobalLock() error {
	tx, err := sm.DB.Begin()
	if err != nil {
		return NewLockAcquisitionError(GlobalLock, "begin transaction", err)
	}
	defer tx.Rollback() // Rollback the transaction if it's not committed.

	// Check if a global lock exists for this LookupKey.
	lockExists, err := checkGlobalLockExists(tx, sm)
	if err != nil {
		return err
	}

	if lockExists {
		// A global lock exists for this state machine and LookupKey.
		// Check if this instance owns the lock.
		if ownedByThisInstance, err := isGlobalLockOwnedByThisInstance(tx, sm); err != nil {
			return err
		} else if !ownedByThisInstance {
			return NewLockAlreadyHeldError(GlobalLock)
		}
		// This instance already owns the lock, so proceed.
		return nil
	}

	// No global lock exists; attempt to obtain it.
	if err := obtainGlobalLock(tx, sm); err != nil {
		return NewLockAcquisitionError(GlobalLock, "obtain global lock", err)
	}

	// Commit the transaction to confirm lock acquisition.
	if err := tx.Commit(); err != nil {
		return NewLockAcquisitionError(GlobalLock, "commit transaction", err)
	}

	return nil
}

// checkAndObtainLocalLock checks if a local lock exists for the state machine and obtains it if possible.
func (sm *StateMachine) checkAndObtainLocalLock() error {
	tx, err := sm.DB.Begin()
	if err != nil {
		return NewLockAcquisitionError(LocalLock, "begin transaction", err)
	}
	defer tx.Rollback() // Rollback the transaction if it's not committed.

	// Check if a global lock exists for this LookupKey. We can't get a local lock if a global lock exists and this instance don't own it.
	globalLockExists, err := checkGlobalLockExists(tx, sm)
	if err != nil {
		return err
	}

	if globalLockExists {
		// A global lock exists for this state machine and LookupKey.
		// we need to check if this instance owns the lock
		if ownedByThisInstance, err := isGlobalLockOwnedByThisInstance(tx, sm); err != nil {
			return err
		} else if !ownedByThisInstance {
			return NewLockAlreadyHeldError(GlobalLock)
		}
	}

	// Check if a local lock exists for this state machine.
	localLockExists, err := checkLocalLockExists(tx, sm)
	if err != nil {
		return NewLockAcquisitionError(LocalLock, "obtain global lock", err)
	}

	if localLockExists {
		// A local lock exists for this state machine.
		// Check if this instance owns the lock.
		if ownedByThisInstance, err := isLocalLockOwnedByThisInstance(tx, sm); err != nil {
			return err
		} else if !ownedByThisInstance {
			return NewLockAlreadyHeldError(LocalLock)
		}
		// This instance already owns the lock, so proceed.
		return nil
	}

	// No local lock exists; attempt to obtain it.
	if err := obtainLocalLock(tx, sm); err != nil {
		// Failed to obtain the local lock.
		return err
	}

	// Commit the transaction to confirm lock acquisition.
	if err := tx.Commit(); err != nil {
		return NewLockAcquisitionError(LocalLock, "commit transaction", err)
	}

	return nil
}

// TODO: implement this
// sendKafkaEvent sends a Kafka event.
func (sm *StateMachine) sendKafkaEvent() error {
	// Implement logic to send Kafka event.
	return nil
}

// IsValidTransition checks if a transition from one state to another is valid.
func IsValidTransition(currentState State, event Event, newState State) bool {
	validTransitions, ok := ValidTransitions[currentState]
	if !ok {
		return false
	}

	validNextStates, ok := validTransitions[event]
	if !ok {
		return false
	}

	for _, state := range validNextStates {
		if state == newState || state == AnyState {
			return true
		}
	}
	return false
}

// CalculateNextRetryDelay calculates the next retry delay based on the retry policy.
func (sm *StateMachine) CalculateNextRetryDelay() time.Duration {
	baseDelay := sm.BaseDelay // Starting delay of 1 second
	retryCount := sm.RetryCount
	retryCount = retryCount + 1 //for the purposes of calculating the delay, we want to start at 1 but want our retry count to be accurate in the state
	delay := time.Duration(math.Pow(2, float64(retryCount))) * baseDelay

	return delay
}

// Run executes the state machine.
// Run can be called on any instantiated state machine.
func (sm *StateMachine) Run() error {
	if err := sm.checkAndAcquireLocks(); err != nil {
		return err
	}

	context := sm.createContext()
	if err := sm.validateHandlers(); err != nil {
		return err
	}

	return sm.processStateMachine(context)
}

// GetRemainingDelay returns the remaining delay until the next retry.
// If the delay is greater than the max timeout, it returns -1.
// If the delay is less than 0, it returns 0.
// Otherwise, it returns the remaining delay.
// This is useful if you want to now how long to sleep before retrying.
func (sm *StateMachine) GetRemainingDelay() time.Duration {
	lastRetry := time.Now()
	if sm.LastRetry != nil {
		lastRetry = *sm.LastRetry
	}
	delay := sm.CalculateNextRetryDelay()
	if delay > sm.MaxTimeout {
		return -1
	}
	nextRetryTime := lastRetry.Add(delay)
	remainingDelay := time.Until(nextRetryTime)

	if remainingDelay < 0 {
		return 0
	}
	return remainingDelay
}

// NewStateMachine initializes a new StateMachine instance with the given config and sensible defaults if ommitted.
func NewStateMachine(config StateMachineConfig) (*StateMachine, error) {
	err := CreateGlobalLockTableIfNotExists(config.DB)
	if err != nil {
		panic(err)
	}

	err = CreateStateMachineTableIfNotExists(config.DB, config.Name)
	if err != nil {
		panic(err)
	}

	if config.RetryPolicy.RetryType == "" {
		config.RetryPolicy.RetryType = ExponentialBackoff
	}

	if config.RetryPolicy.BaseDelay == 0 {
		config.RetryPolicy.BaseDelay = 1 * time.Second
	}

	if config.RetryPolicy.MaxTimeout == 0 {
		config.RetryPolicy.MaxTimeout = 10 * time.Second
	}

	sm := &StateMachine{
		Name:                          config.Name,
		UniqueID:                      config.UniqueStateMachineID,
		LookupKey:                     config.LookupKey,
		DB:                            config.DB,
		KafkaProducer:                 config.KafkaProducer,
		KafkaEventTopic:               config.KafkaEventTopic,
		ExecuteSynchronously:          config.ExecuteSynchronously,
		CreatedTimestamp:              time.Now(),
		UpdatedTimestamp:              time.Now(),
		CurrentState:                  StatePending,
		SaveAfterEachStep:             true,
		LockType:                      config.LockType,
		RetryType:                     config.RetryPolicy.RetryType,
		MaxTimeout:                    config.RetryPolicy.MaxTimeout,
		BaseDelay:                     config.RetryPolicy.BaseDelay,
		UnlockGlobalLockAfterEachStep: true,
	}

	if len(config.Handlers) > 0 {
		sm.Handlers = config.Handlers
	}

	// If UniqueID is not provided, generate it.
	if sm.UniqueID == "" {
		sm.GenerateAndSetUniqueID()
	}

	err = insertStateMachine(sm)

	if err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			// Duplicate entry error
			return nil, fmt.Errorf("state machine with ID %s already exists", config.UniqueStateMachineID)
		} else {
			return nil, err
		}
	}

	return sm, nil
}

// GetCurrentArbitraryData returns the current arbitrary data associated with the state machine.
// This explicitly returns a copy of the map to avoid referencing the same space in memory as we manipulate the data
func (sm *StateMachine) GetCurrentArbitraryData() map[string]interface{} {
	return CopyMap(sm.CurrentArbitraryData)
}

// GetHistory returns the history of executed transitions.
func (sm *StateMachine) GetHistory() []TransitionHistory {
	return sm.History
}

// GetState returns the current state of the state machine.
func (sm *StateMachine) GetState() State {
	return sm.CurrentState
}

// DidStateMachineComplete returns true if the state machine completed successfully.
func (sm *StateMachine) DidStateMachineComplete() bool {
	return sm.CurrentState == StateCompleted
}

// DidStateMachineFail returns true if the state machine failed.
func (sm *StateMachine) DidStateMachineFail() bool {
	return sm.CurrentState == StateFailed
}

// DidStateMachineRollbackFail returns true if the state machine failed to rollback.
func (sm *StateMachine) DidStateMachineRollback() bool {
	return sm.CurrentState == StateRollbackCompleted
}

// DidStateMachineRollbackFail returns true if the state machine failed to rollback.
func (sm *StateMachine) DidStateMachineRollbackFail() bool {
	return sm.CurrentState == StateRollbackFailed
}

// DidStateMachineCancel returns true if the state machine was cancelled.
func (sm *StateMachine) DidStateMachineCancel() bool {
	return sm.CurrentState == StateCancelled
}

// DidStateMachinePause returns true if the state machine was paused.
func (sm *StateMachine) DidStateMachinePause() bool {
	return sm.CurrentState == StatePaused
}

// DidStateMachinePark returns true if the state machine was parked.
func (sm *StateMachine) DidStateMachinePark() bool {
	return sm.CurrentState == StateParked
}

// DidStateMachineRetry returns true if a step in the state machine will be retried.
func (sm *StateMachine) DidStateMachineRetry() bool {
	return sm.CurrentState == StateRetry
}

// DidStateMachineRetryFail returns true if a step in the state machine failed to retry.
func (sm *StateMachine) DidStateMachineRetryFail() bool {
	return sm.CurrentState == StateRetryFailed
}

// GetRetryCount returns the number of times the state machine has retried.
func (sm *StateMachine) GetRetryCount() int {
	return sm.RetryCount
}

// GetLastRetryTime returns the time of the last retry.
func (sm *StateMachine) GetLastRetryTime() time.Time {
	return *sm.LastRetry
}

// IsTheStateMachineInATerminalState returns true if the state machine is in a terminal state.
// A terminal state is a state where the state machine stops processing.
func (sm *StateMachine) IsTheStateMachineInATerminalState() bool {
	return IsTerminalState(sm.CurrentState)
}

// GenerateAndSetUniqueID generates a unique ID for the StateMachine and updates the UniqueID field.
func (sm *StateMachine) GenerateAndSetUniqueID() string {
	// Generate a unique ID (e.g., UUID)
	uniqueID := uuid.New().String() // Using UUID as an example
	sm.UniqueID = uniqueID
	return uniqueID
}

// SetUniqueID sets the unique ID for the state machine.
// This can be set through the config when creating the state machine and this will overwrite that value.
func (sm *StateMachine) SetUniqueID(uniqueStateMachineID string) *StateMachine {
	sm.UniqueID = uniqueStateMachineID
	return sm
}

// SetLookupKey sets the lookup key for the state machine. Something like a user or account ID to lock on.
// This can be set through the config when creating the state machine and this will overwrite that value.
func (sm *StateMachine) SetLookupKey(lookUpKey string) *StateMachine {
	sm.LookupKey = lookUpKey
	return sm
}

// ForceState sets the state of the state machine without validation.
// This is useful if you want to force the state machine to a specific state.
// This is not recommended unless you know what you are doing.
func (sm *StateMachine) ForceState(state State) *StateMachine {
	sm.CurrentState = state
	return sm
}

// SetState sets the state machine's state, returning an error if the transition is invalid.
func (sm *StateMachine) SetState(newState State, event Event) error {
	if !IsValidTransition(sm.CurrentState, event, newState) {
		return NewStateTransitionError(sm.CurrentState, newState, event)
	}
	sm.CurrentState = newState
	return nil
}

// AddStep adds a handler to the state machine.
func (sm *StateMachine) AddStep(handler StepHandler, name string) *StateMachine {
	sm.Handlers = append(sm.Handlers, handler)
	return sm
}

// RegisterCallback registers a callback for a statemachine event
func (sm *StateMachine) AddStateCallbacks(state State, callbacks StateCallbacks) *StateMachine {
	if sm.Callbacks == nil {
		sm.Callbacks = make(map[string]StateCallbacks)
	}
	sm.Callbacks[string(state)] = callbacks
	return sm
}

// SaveState saves the state machine's state to the database.
func (sm *StateMachine) SaveState() error {
	return sm.saveStateToDB()
}

// DisableAutoGlobalUnlock disables removing the global lock between each step.
// This is useful if you want to run the state machine in a loop.
// You will need to manually unlock the global lock when you are done.
// The Global lock is auto removed when a terminal state is reached.
func (sm *StateMachine) DisableAutoGlobalUnlock() *StateMachine {
	sm.UnlockGlobalLockAfterEachStep = false
	return sm
}

// DisableSaveAfterEachStep disables saving the state after each step.
func (sm *StateMachine) DisableSaveAfterEachStep() *StateMachine {
	sm.SaveAfterEachStep = false
	return sm
}

// LockAndLoadStateMachine loads a StateMachine from the database.
// This is useful if you want to resume or continue a state machine that was previously created and has not completed.
func LockAndLoadStateMachine(name, id string, db *sql.DB) (*StateMachine, error) {
	sm, err := loadStateMachineFromDB(name, id, db)
	if err != nil {
		return nil, NewDatabaseOperationError("loadAndLockStateMachine", err)
	}

	// Deserialize the state machine from the loaded data
	resumedSMData, err := deserializeFromJSON(sm.SerializedState)
	if err != nil {
		return nil, err
	}

	// Initialize the resumed state machine with additional information as needed
	sm.Name = name
	sm.UniqueID = id
	sm.DB = db
	sm.CurrentArbitraryData = resumedSMData

	return sm, nil
}

// Rollback sets the state machine to enter a rollback state, it will attempt to continue to roll back all steps.
func (sm *StateMachine) Rollback() error {
	// Check if the transition from the current state should be to StateRollback or StateStartRollback and pick the event accordingly
	state := StateStartRollback
	if sm.CurrentState == StateRollback {
		state = StateRollback
	}

	// Check if the transition from the current state with OnRollback event is valid
	if !IsValidTransition(sm.CurrentState, OnRollback, state) {
		return NewStateTransitionError(sm.CurrentState, state, OnRollback)
	}
	sm.CurrentState = StateRollback
	return sm.Run()
}

// Resume resumes the execution of the state machine.
// This is useful if you want to resume the state machine after it has been paused.
// This only works when you are in a paused state and the state machine has not been completed.
func (sm *StateMachine) Resume() error {
	// Check if the transition from the current state with OnResume event is valid
	if !IsValidTransition(sm.CurrentState, OnResume, StateOpen) {
		return NewStateTransitionError(sm.CurrentState, StateOpen, OnResume)
	}
	sm.CurrentState = StateOpen
	return sm.Run()
}

// ExitParkedState exits the parked state into the specified state.
func (sm *StateMachine) ExitParkedState(newState State) error {
	// Validate the transition out of StateParked
	if !IsValidTransition(StateParked, OnManualOverride, newState) {
		return NewStateTransitionError(StateParked, newState, OnManualOverride)
	}
	sm.CurrentState = newState
	return nil
}
