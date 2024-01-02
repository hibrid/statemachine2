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

type Callback func(*StateMachine, *Context) error
type StateCallbacks struct {
	AfterEvent Callback
	EnterState Callback
	LeaveState Callback
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

	// StateStartRetry is the state of a state machine when it needs to retry later.
	// ExecuteForward and change state in machine to StateRetry
	StateStartRetry State = "start_retry"

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
	Name                 string                    `json:"name"`
	UniqueID             string                    `json:"id"`
	Handlers             []Handler                 `json:"-"` // List of handlers with names
	Callbacks            map[string]StateCallbacks `json:"-"`
	CurrentState         State                     `json:"currentState"`
	DB                   *sql.DB                   `json:"-"`
	KafkaProducer        *kafka.Producer           `json:"-"`
	LookupKey            string                    `json:"lookupKey"`            // User or account ID for locking
	ResumeFromStep       int                       `json:"resumeFromStep"`       // Step to resume from when recreated
	SaveAfterEachStep    bool                      `json:"saveAfterStep"`        // Flag to save state after each step
	KafkaEventTopic      string                    `json:"kafkaEventTopic"`      // Kafka topic to send events
	History              []TransitionHistory       `json:"history"`              // History of executed transitions
	ExecuteSynchronously bool                      `json:"executeSynchronously"` // Flag to control whether to execute the next step immediately

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
	Handlers             []Handler
	RetryPolicy          RetryPolicy
	SaveAfterEachStep    bool
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

// Handler defines the interface for state machine handlers.
type Handler interface {
	Name() string
	ExecuteForward(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error)
	ExecuteBackward(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error)
	ExecutePause(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error)
	ExecuteResume(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error)
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
		OnRetry:     []State{StateRetry, StateStartRetry},
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
		OnRollbackCompleted: []State{StateRollbackCompleted},
		OnRollbackFailed:    []State{StateRollbackFailed},
		OnRollback:          []State{StateRollback},
		OnCancelled:         []State{StateCancelled},

		OnUnknownSituation: []State{StateParked},
	},
	StatePaused: {
		OnResume: []State{StateOpen},
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
		return err
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
		return nil, err
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

	var handler Handler
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
		return err
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
	if remainingDelay > 0 {
		time.Sleep(remainingDelay)
	}

	return nil
}

func (sm *StateMachine) executeLeaveStateCallback(context *Context) error {
	if callbacks, ok := sm.Callbacks[string(sm.CurrentState)]; ok {
		if err := callbacks.LeaveState(sm, context); err != nil {
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
	case StateStartRetry:
		newState = StateRetry
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
		if err := callbacks.EnterState(sm, context); err != nil {
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
		return StateUnknown, false, fmt.Errorf("invalid state transition from %s to %s on event %v", sm.CurrentState, newState, event)
	}

	return newState, shouldRetry, nil
}

// TODO: create unit tests for this
// shouldCheckLocks is replaced by checkAndAcquireLocks
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
		return err
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
		} else if ownedByThisInstance {
			// This instance already owns the lock, so proceed.
			return nil
		}
		// Another instance owns the lock; do not proceed.
		return fmt.Errorf("another instance holds the global lock")
	}

	// No global lock exists; attempt to obtain it.
	if err := obtainGlobalLock(tx, sm); err != nil {
		// Failed to obtain the global lock.
		return err
	}

	// Commit the transaction to confirm lock acquisition.
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// checkAndObtainLocalLock checks if a local lock exists for the state machine and obtains it if possible.
func (sm *StateMachine) checkAndObtainLocalLock() error {
	tx, err := sm.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // Rollback the transaction if it's not committed.

	// Check if a global lock exists for this LookupKey.
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
			// This instance already owns the lock, so proceed.
			return fmt.Errorf("another instance holds the global lock")
		}
	}

	// Check if a local lock exists for this state machine.
	localLockExists, err := checkLocalLockExists(tx, sm)
	if err != nil {
		return err
	}

	if localLockExists {
		// A local lock exists for this state machine.
		// Check if this instance owns the lock.
		if ownedByThisInstance, err := isLocalLockOwnedByThisInstance(tx, sm); err != nil {
			return err
		} else if ownedByThisInstance {
			// This instance already owns the lock, so proceed.
			return nil
		}
		// Another instance owns the lock; do not proceed.
		return fmt.Errorf("another instance holds the local lock")
	}

	// No local lock exists; attempt to obtain it.
	if err := obtainLocalLock(tx, sm); err != nil {
		// Failed to obtain the local lock.
		return err
	}

	// Commit the transaction to confirm lock acquisition.
	if err := tx.Commit(); err != nil {
		return err
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

// TODO: create unit tests for this
func (sm *StateMachine) CalculateNextRetryDelay() time.Duration {
	baseDelay := sm.BaseDelay // Starting delay of 1 second
	maxDelay := sm.MaxTimeout // Maximum delay of 60 seconds

	delay := time.Duration(math.Pow(2, float64(sm.RetryCount))) * baseDelay
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

// create unit tests for this
// Run executes the state machine.
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

// TODO: create unit tests for this
func (sm *StateMachine) GetRemainingDelay() time.Duration {
	if sm.LastRetry == nil {
		return 0
	}
	nextRetryTime := sm.LastRetry.Add(sm.CalculateNextRetryDelay())
	remainingDelay := time.Until(nextRetryTime)

	if remainingDelay < 0 {
		return 0
	}
	return remainingDelay
}

// TODO: Create Unit tests for this
func (sm *StateMachine) HandleEvent(context *Context, event Event) error {
	return sm.handleTransition(context, event)
}

// NewStateMachine initializes a new StateMachine instance.
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
		Name:                 config.Name,
		UniqueID:             config.UniqueStateMachineID,
		LookupKey:            config.LookupKey,
		DB:                   config.DB,
		KafkaProducer:        config.KafkaProducer,
		KafkaEventTopic:      config.KafkaEventTopic,
		ExecuteSynchronously: config.ExecuteSynchronously,
		CreatedTimestamp:     time.Now(),
		UpdatedTimestamp:     time.Now(),
		CurrentState:         StatePending,
		SaveAfterEachStep:    config.SaveAfterEachStep,
		LockType:             config.LockType,
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

// TODO: create unit tests for this
// GenerateAndSetUniqueID generates a unique ID for the StateMachine and updates the UniqueID field.
func (sm *StateMachine) GenerateAndSetUniqueID() string {
	// Generate a unique ID (e.g., UUID)
	uniqueID := uuid.New().String() // Using UUID as an example
	sm.UniqueID = uniqueID
	return uniqueID
}

// TODO: create unit tests for this
// TODO: create integration tests for this
// TODO: create unit tests for this
func (sm *StateMachine) SetUniqueID(uniqueStateMachineID string) *StateMachine {
	sm.UniqueID = uniqueStateMachineID
	return sm
}

// TODO: create unit tests for this
// TODO: create integration tests for this
// SetLookupKey sets the lookup key for the state machine. Something like a user or account ID to lock on.
func (sm *StateMachine) SetLookupKey(lookUpKey string) *StateMachine {
	sm.LookupKey = lookUpKey
	return sm
}

// TODO: create unit tests for this
// TODO: create integration tests for this
// SetInitialState sets the force sets the state of the state machine without validation.
func (sm *StateMachine) SetState(state State) *StateMachine {
	sm.CurrentState = state
	return sm
}

// TODO: create unit tests for this
// TODO: create integration tests for this
// UpdateState attempts to update the state of the state machine, returning an error if the transition is invalid.
func (sm *StateMachine) UpdateState(newState State, event Event) error {
	if !IsValidTransition(sm.CurrentState, event, newState) {
		return fmt.Errorf("invalid state transition from %s to %s on event %v", sm.CurrentState, newState, event)
	}
	sm.CurrentState = newState
	return nil
}

// TODO: create unit tests for this
// TODO: create integration tests for this
// AddHandler adds a handler to the state machine.
func (sm *StateMachine) AddHandler(handler Handler, name string) *StateMachine {
	sm.Handlers = append(sm.Handlers, handler)
	return sm
}

// TODO: create unit tests for this
// TODO: create integration tests for this
// RegisterCallback registers a callback for a statemachine event
func (sm *StateMachine) RegisterEventCallback(state State, callbacks StateCallbacks) *StateMachine {
	if sm.Callbacks == nil {
		sm.Callbacks = make(map[string]StateCallbacks)
	}
	sm.Callbacks[string(state)] = callbacks
	return sm
}

// TODO: create unit tests for this
// TODO: create integration tests for this
// SaveState saves the state machine's state to the database.
func (sm *StateMachine) SaveState() error {
	// Refactored to use a more descriptive method name
	return sm.saveStateToDB()
}

// TODO: create unit tests for this
// TODO: create integration tests for this
// LoadStateMachine loads a StateMachine from the database.
func LoadStateMachine(name, id string, db *sql.DB) (*StateMachine, error) {
	sm, err := loadStateMachineFromDB(name, id, db)
	if err != nil {
		return nil, err
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

// TODO: create unit tests for this
// TODO: create integration tests for this
// Rollback rolls back the state machine to the previous state.
func (sm *StateMachine) Rollback() error {
	// Check if the transition from the current state should be to StateRollback or StateStartRollback and pick the event accordingly
	state := StateStartRollback
	if sm.CurrentState == StateRollback {
		state = StateRollback
	}

	// Check if the transition from the current state with OnRollback event is valid
	if !IsValidTransition(sm.CurrentState, OnRollback, state) {
		return fmt.Errorf("invalid state transition from %s to %s on event %s", sm.CurrentState, StateRollback, "OnRollback")
	}
	sm.CurrentState = StateRollback
	return sm.Run()
}

// TODO: create unit tests for this
// TODO: create integration tests for this
// Resume resumes the execution of the state machine.
func (sm *StateMachine) Resume() error {
	// Check if the transition from the current state with OnResume event is valid
	if !IsValidTransition(sm.CurrentState, OnResume, StateOpen) {
		return fmt.Errorf("invalid state transition from %s to %s on event %s", sm.CurrentState, StateOpen, "OnResume")
	}
	sm.CurrentState = StateOpen
	return sm.Run()
}

// TODO: create unit tests for this
// TODO: create integration tests for this
// ExitParkedState exits the parked state into the specified state.
func (sm *StateMachine) ExitParkedState(newState State) error {
	// Validate the transition out of StateParked
	if !IsValidTransition(StateParked, OnManualOverride, newState) {
		return fmt.Errorf("invalid state transition from %s to %s", StateParked, newState)
	}
	sm.CurrentState = newState
	return nil
}
