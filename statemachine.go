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
	StatePending State = "pending"

	// StateOpen is the state of a state machine when it is in progress and not locked.
	StateOpen State = "open"

	// StateInProgress is the state of a state machine when it is in progress and locked.
	StateInProgress State = "in_progress"

	// StateCompleted is the final state of a state machine.
	StateCompleted State = "completed"

	// StateFailed is the state of a state machine when it fails.
	StateFailed State = "failed"

	// StateRetry is the state of a state machine when it needs to retry later.
	StateRetry State = "retry"

	// StateFailed is the state of a state machine when it fails.
	StateRollback State = "rollback"

	// StateRollbackCompleted is the state of a state machine when its rollback is completed.
	StateRollbackCompleted State = "rollback_completed"

	// StatePaused is the state of a state machine when it is paused.
	StatePaused State = "paused"

	// StatePaused is the state of a state machine when it is paused.
	StateResume State = "resume"

	// StateCancelled is the state of a state machine when it is cancelled.
	StateCancelled State = "cancelled"

	// StateParked is the state of a state machine when it is parked because we don't know what to do with it.
	StateParked State = "parked"

	// AnyState is a special state that can be used to indicate that a transition can happen from any state.
	AnyState State = "any"
)

// StateMachine represents a state machine instance.
type StateMachine struct {
	Name                 string                    `json:"name"`
	ID                   string                    `json:"id"`
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
	// Additional field for storing arbitrary data
	CurrentArbitraryData map[string]interface{} `json:"currentArbitraryData"`
	CreatedTimestamp     time.Time              `json:"createdTimestamp"`
	UpdatedTimestamp     time.Time              `json:"updatedTimestamp"`
	UnlockedTimestamp    time.Time              `json:"unlockedTimestamp"`
	UsesGlobalLock       bool                   `json:"usesGlobalLock"`
	UsesLocalLock        bool                   `json:"usesLocalLock"`
	RetryCount           int                    `json:"retryCount"`
	RetryType            RetryType              `json:"retryType"`
	MaxTimeout           time.Duration          `json:"maxTimeout"`
	BaseDelay            time.Duration          `json:"baseDelay"`
	LastRetry            time.Time              `json:"lastRetry"`
	SerializedState      []byte                 `json:"-"`
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

var TerminalStates = map[State]bool{
	StateCompleted:         true,
	StateFailed:            true,
	StateRollbackCompleted: true,
	StateCancelled:         true,
}

func IsTerminalState(state State) bool {
	_, isTerminal := TerminalStates[state]
	return isTerminal
}

var ValidTransitions = map[State]map[Event][]State{
	StatePending: {
		OnSuccess: []State{StateOpen},
		OnFailed:  []State{StateFailed},
	},
	StateOpen: {
		OnSuccess:  []State{StateOpen, StateCompleted},
		OnFailed:   []State{StateFailed},
		OnPause:    []State{StatePaused},
		OnRollback: []State{StateRollback},
		OnRetry:    []State{StateRetry},

		OnUnknownSituation: []State{StateParked},
	},
	StateRetry: {
		OnSuccess:          []State{StateOpen},
		OnFailed:           []State{StateFailed},
		OnUnknownSituation: []State{StateParked},
	},
	StateRollback: {
		OnSuccess:          []State{StateRollbackCompleted},
		OnFailed:           []State{StateFailed},
		OnUnknownSituation: []State{StateParked},
	},
	StatePaused: {
		OnResume: []State{StateOpen},
	},
	StateParked: {
		OnManualOverride: []State{AnyState},
	},

	// Add other states and their corresponding events here
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

func (sm *StateMachine) CalculateNextRetryDelay() time.Duration {
	baseDelay := sm.BaseDelay // Starting delay of 1 second
	maxDelay := sm.MaxTimeout // Maximum delay of 60 seconds

	delay := time.Duration(math.Pow(2, float64(sm.RetryCount))) * baseDelay
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

func (sm *StateMachine) GetRemainingDelay() time.Duration {
	nextRetryTime := sm.LastRetry.Add(sm.CalculateNextRetryDelay())
	remainingDelay := time.Until(nextRetryTime)

	if remainingDelay < 0 {
		return 0
	}
	return remainingDelay
}

// Load the serialized JSON state data from the database
func loadStateMachineFromDB(stateMachineType string, id string, db *sql.DB) (*StateMachine, error) {
	// Load serialized state data from the database (replace with your database logic)
	sm := &StateMachine{
		ID:   id,
		Name: stateMachineType,
		DB:   db,
	}
	sm, err := loadAndLockStateMachine(sm)
	if err != nil {
		return nil, err
	}
	return sm, nil
}

// Start begins the execution of the state machine.
func (sm *StateMachine) Run() error {

	// Check for locks if necessary
	if sm.shouldCheckLocks() {
		// Lock checking logic here
		if err := sm.checkLocks(); err != nil {
			return err
		}
	}

	context := &Context{
		InputState:          sm.CurrentState,
		InputArbitraryData:  make(map[string]interface{}),
		OutputArbitraryData: make(map[string]interface{}),
		StepNumber:          sm.ResumeFromStep,
		TransitionHistory:   sm.History,
		StateMachine:        sm,
	}

	// Check that CurrentArbitraryData is not nil
	if sm.CurrentArbitraryData == nil {
		sm.CurrentArbitraryData = make(map[string]interface{})
	}

	if sm.Handlers == nil || len(sm.Handlers) == 0 {
		return fmt.Errorf("no handlers found")
	}

	handler := sm.Handlers[sm.ResumeFromStep]
	context.Handler = handler

	executionEvent, err := context.Handle()
	if err != nil {
		return err
	}

	err = sm.HandleEvent(context, executionEvent)
	if err != nil {
		return err
	}

	return nil
}

func (sm *StateMachine) HandleEvent(context *Context, event Event) error {
	return sm.handleTransition(context, event)
}

func (sm *StateMachine) handleTransition(context *Context, event Event) error {

	// Leave state callback (before changing the state)
	if callbacks, ok := sm.Callbacks[string(sm.CurrentState)]; ok {
		cb := callbacks.LeaveState
		if err := cb(sm, context); err != nil {
			return err
		}
	}

	sm.CurrentArbitraryData = context.OutputArbitraryData
	var newState State
	stepNumber := context.StepNumber
	// Update transition history
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

	var remainingDelay time.Duration
	var shouldRetry bool

	// Handle events and transitions using the handler's Execute methods
	switch event {
	case OnFailed:
		newState = StateFailed
	case OnSuccess:
		newState = StateOpen
		if stepNumber == len(sm.Handlers)-1 {
			sm.ResumeFromStep = stepNumber
			sm.CurrentState = StateCompleted
		} else {
			historyEntry.FromStep = stepNumber - 1
			sm.ResumeFromStep = stepNumber + 1
		}
	case OnResetTimeout:
		newState = StateRollback
		historyEntry.FromStep = stepNumber + 1
		if stepNumber == 0 {
			sm.ResumeFromStep = stepNumber
			sm.CurrentState = StateRollbackCompleted
		} else {
			sm.ResumeFromStep = stepNumber - 1
		}
	case OnPause:
		newState = StatePaused
	case OnAlreadyCompleted:
		newState = StateCompleted
	case OnRollback:
		newState = StateRollback
		historyEntry.FromStep = stepNumber + 1
		if stepNumber == 0 {
			sm.ResumeFromStep = stepNumber
			sm.CurrentState = StateRollbackCompleted
		} else {
			sm.ResumeFromStep = stepNumber - 1
		}
	case OnResume:
		newState = StateOpen
	case OnRetry:
		newState = StateRetry
		if newState == sm.CurrentState {
			sm.RetryCount++
		}
		sm.LastRetry = time.Now()
		remainingDelay = sm.GetRemainingDelay()
		shouldRetry = true

	case OnUnknownSituation:
		newState = StateParked
	default:
		newState = StateParked
		//TODO: Log Error
	}

	if !IsValidTransition(sm.CurrentState, event, newState) {
		return fmt.Errorf("invalid state transition from %s to %s on event %v", sm.CurrentState, newState, event)
	}

	// Update the state
	sm.CurrentState = newState
	historyEntry.ModifiedState = sm.CurrentState
	sm.History = append(sm.History, historyEntry)

	// Save state to MySQL or send Kafka event based on configuration.
	if sm.SaveAfterEachStep {
		if err := sm.saveStateToDB(); err != nil {
			// Handle state save error.
			return err
		}
	}

	// Enter state callback (after changing the state)
	if callbacks, ok := sm.Callbacks[string(sm.CurrentState)]; ok {
		cb := callbacks.EnterState
		if err := cb(sm, context); err != nil {
			return err
		}
	}

	// Handle retry logic
	if shouldRetry {
		if sm.ExecuteSynchronously {
			if remainingDelay > 0 {
				time.Sleep(remainingDelay)
			}
			return sm.Run()
		} else {
			// Send Kafka event if configured
			if sm.KafkaEventTopic != "" {
				if err := sm.sendKafkaEvent(); err != nil {
					return err
				}
			}
			return nil
		}
	}

	return nil
}

// shouldCheckLocks checks if the state machine should check locks based on its configuration.
func (sm *StateMachine) shouldCheckLocks() bool {
	return sm.UsesGlobalLock || sm.UsesLocalLock
}

// checkLocks checks for locks and handles locking logic.
func (sm *StateMachine) checkLocks() error {
	// Begin a new transaction for lock operations.
	tx, err := sm.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // Rollback the transaction if it's not committed.

	// Check if the state machine is configured for a global lock.
	if sm.UsesGlobalLock {
		if err := sm.checkAndObtainGlobalLock(tx); err != nil {
			return err
		}
	}

	// Check if the state machine is configured for a local lock.
	if sm.UsesLocalLock {
		if err := sm.checkAndObtainLocalLock(tx); err != nil {
			return err
		}
	}

	// Commit the transaction to confirm lock acquisition.
	if err := tx.Commit(); err != nil {
		return err
	}

	// If we reached this point, it means the state machine either doesn't
	// need to check locks or has successfully obtained the required locks.
	return nil
}

// checkAndObtainGlobalLock checks if a global lock exists for the state machine and obtains it if possible.
func (sm *StateMachine) checkAndObtainGlobalLock(tx *sql.Tx) error {
	// Check if a global lock exists for this state machine.
	lockExists, err := checkGlobalLockExists(tx, sm)
	if err != nil {
		return err
	}

	if lockExists {
		// A global lock exists for this state machine.
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

	return nil
}

// checkAndObtainLocalLock checks if a local lock exists for the state machine and obtains it if possible.
func (sm *StateMachine) checkAndObtainLocalLock(tx *sql.Tx) error {
	// Check if a local lock exists for this state machine.
	lockExists, err := checkLocalLockExists(tx, sm)
	if err != nil {
		return err
	}

	if lockExists {
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

	return nil
}

// sendKafkaEvent sends a Kafka event.
func (sm *StateMachine) sendKafkaEvent() error {
	// Implement logic to send Kafka event.
	return nil
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
		ID:                   config.UniqueStateMachineID,
		LookupKey:            config.LookupKey,
		DB:                   config.DB,
		KafkaProducer:        config.KafkaProducer,
		KafkaEventTopic:      config.KafkaEventTopic,
		ExecuteSynchronously: config.ExecuteSynchronously,
		CreatedTimestamp:     time.Now(),
		UpdatedTimestamp:     time.Now(),
		CurrentState:         StatePending,
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

func (sm *StateMachine) SetUniqueID(uniqueStateMachineID string) *StateMachine {
	sm.ID = uniqueStateMachineID
	return sm
}

// SetLookupKey sets the lookup key for the state machine. Something like a user or account ID to lock on.
func (sm *StateMachine) SetLookupKey(lookUpKey string) *StateMachine {
	sm.LookupKey = lookUpKey
	return sm
}

// SetInitialState sets the force sets the state of the state machine without validation.
func (sm *StateMachine) SetState(state State) *StateMachine {
	sm.CurrentState = state
	return sm
}

// UpdateState attempts to update the state of the state machine, returning an error if the transition is invalid.
func (sm *StateMachine) UpdateState(newState State, event Event) error {
	if !IsValidTransition(sm.CurrentState, event, newState) {
		return fmt.Errorf("invalid state transition from %s to %s on event %v", sm.CurrentState, newState, event)
	}
	sm.CurrentState = newState
	return nil
}

// AddHandler adds a handler to the state machine.
func (sm *StateMachine) AddHandler(handler Handler, name string) *StateMachine {
	sm.Handlers = append(sm.Handlers, handler)
	return sm
}

// RegisterCallback registers a callback for a statemachine event
func (sm *StateMachine) RegisterEventCallback(state State, callbacks StateCallbacks) *StateMachine {
	if sm.Callbacks == nil {
		sm.Callbacks = make(map[string]StateCallbacks)
	}
	sm.Callbacks[string(state)] = callbacks
	return sm
}

func LoadStateMachine(name, id string, db *sql.DB) (*StateMachine, error) {
	// Load serialized state data from the database
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
	sm.ID = id
	sm.DB = db
	sm.CurrentArbitraryData = resumedSMData

	return sm, nil
}

// Rollback rolls back the state machine to the previous state.
func (sm *StateMachine) Rollback() error {
	// Check if the transition from the current state with OnRollback event is valid
	if !IsValidTransition(sm.CurrentState, OnRollback, StateRollback) {
		return fmt.Errorf("invalid state transition from %s to %s on event %s", sm.CurrentState, StateRollback, "OnRollback")
	}
	sm.CurrentState = StateRollback
	return sm.Run()
}

// Resume resumes the execution of the state machine.
func (sm *StateMachine) Resume() error {
	// Check if the transition from the current state with OnResume event is valid
	if !IsValidTransition(sm.CurrentState, OnResume, StateOpen) {
		return fmt.Errorf("invalid state transition from %s to %s on event %s", sm.CurrentState, StateOpen, "OnResume")
	}
	sm.CurrentState = StateOpen
	return sm.Run()
}

// ExitParkedState exits the parked state into the specified state.
func (sm *StateMachine) ExitParkedState(newState State) error {
	// Validate the transition out of StateParked
	if !IsValidTransition(StateParked, OnManualOverride, newState) {
		return fmt.Errorf("invalid state transition from %s to %s", StateParked, newState)
	}
	sm.CurrentState = newState
	return nil
}
