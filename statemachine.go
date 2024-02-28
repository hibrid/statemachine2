package statemachine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Callback func(*StateMachine, *Context) error
type StateCallbacks struct {
	AfterAnEvent  Callback // callback executes immediately after the event is handled and before any state transition
	BeforeTheStep Callback // callback executes immediately after the state transition
	AfterTheStep  Callback // callback executes immediately before the state transition
}

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

	MachineLockInfo *StateMachineTypeLockInfo `json:"-"`
	Context         context.Context           `json:"-"`
	Log             *zap.Logger               `json:"-"`
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
	Context              context.Context
	Logger               *zap.Logger
}

// Save the state machine's serialized JSON to the database
func (sm *StateMachine) saveStateToDB() error {
	if err := updateStateMachineState(sm, sm.CurrentState); err != nil {
		return NewDatabaseOperationError("updateStateMachineState", err)
	}
	return nil
}

func (sm *StateMachine) serializeToJSON() ([]byte, error) {
	serialized, err := json.Marshal(sm)
	if err != nil {
		return nil, err
	}
	return serialized, nil
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
		completeHandler := &completeHandler{Logger: sm.Log}
		handler = NewStep("Default Completion Handler", sm.Log, completeHandler.ExecuteForward, completeHandler.ExecuteBackward, completeHandler.ExecutePause, completeHandler.ExecuteResume)
	} else {
		handler = sm.Handlers[sm.ResumeFromStep]
	}

	if err := sm.contextCancelled(); err != nil {
		//handler = &cancelHandler{Logger: sm.Log}
		cancelHandler := &cancelHandler{Logger: sm.Log}
		handler = NewStep("Default Cancellation Handler", sm.Log, cancelHandler.ExecuteForward, cancelHandler.ExecuteBackward, cancelHandler.ExecutePause, cancelHandler.ExecuteResume)
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
// this is not the same as context.Context. This is the context for the statemachine
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

	// this also updates the database if configured to do so
	// this should complete to record what just happened
	// any cancellation or failure after this point will be recorded in the next step
	// if this is a synchronous execution
	if err := sm.updateStateMachineState(context, newState, event); err != nil {
		return err
	}

	// let the previous step complete
	if err := sm.contextCancelled(); err != nil {
		if err := sm.updateStateMachineState(context, StateCancelled, OnCancelled); err != nil {
			return err
		}
		shouldRetry = false
		// continue with processing the state machine
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
	if !IsTerminalState(sm.CurrentState) && sm.ExecuteSynchronously && sm.CurrentState != StatePaused {
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

	// only states that have special logic are here. Everything else is just passed through without update the state
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

func (sm *StateMachine) contextCancelled() error {
	select {
	case <-sm.Context.Done():
		return sm.Context.Err()
	default:
		return nil
	}
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
			newState = StateStartRetry
		}
		shouldRetry = true
	case OnCancelled:
		newState = StateCancelled
	case OnLock:
		newState = StateLocked
	case OnUnknownSituation:
		newState = StateParked
	default:
		newState = StateParked
		//TODO: Log Error
	}

	if !IsValidTransition(sm.CurrentState, event, newState) {
		return StateUnknown, false, NewStateTransitionError(sm.CurrentState, newState, event,
			fmt.Errorf("invalid transition from %s to %s", sm.CurrentState, newState))
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
			return NewLockAlreadyHeldError(GlobalLock,
				fmt.Errorf("you can't acquire the global lock for this state machine and LookupKey because it's already held by another instance"))
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
			return NewLockAlreadyHeldError(GlobalLock,
				fmt.Errorf("you can't acquire the local lock for this state machine and LookupKey because a global lock exists and it's already held by another instance"))
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
			return NewLockAlreadyHeldError(LocalLock,
				fmt.Errorf("you can't acquire the local lock for this state machine because it's already held by another instance"))
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
	if sm.CurrentState == StateSleeping {
		// Logic to handle the sleeping state
		// This could involve queuing the state machine and periodically checking if the lock is lifted
		return NewSleepStateError(sm.MachineLockInfo.Start, sm.MachineLockInfo.End,
			fmt.Errorf("state machine is sleeping"))
	}

	if err := sm.validateHandlers(); err != nil {
		return err
	}

	if err := sm.checkAndAcquireLocks(); err != nil {
		return err
	}

	context := sm.createContext()

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

// NewStateMachineWithContext initializes a new StateMachine instance with the given config and context.Context
func NewStateMachineWithContext(context context.Context, config StateMachineConfig) (*StateMachine, error) {
	config.Context = context
	return NewStateMachine(config)
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

	err = CreateStateMachineLockTableIfNotExists(config.DB)
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

	currentTime := time.Now()
	locks, err := checkStateMachineTypeLock(config.DB, config.Name, currentTime)
	if err != nil {
		return nil, err
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
		Context:                       context.Background(),
		Log:                           zap.NewNop(), // you should set your own logger
	}

	if config.Logger != nil {
		sm.Log = config.Logger
	}

	if config.Context != nil {
		sm.Context = config.Context
	}

	for _, lock := range locks {
		sm.MachineLockInfo = &lock
		if lock.Type == MachineLockTypeSleepState {
			sm.CurrentState = StateSleeping

			break
		} else if lock.Type == MachineLockTypeImmediateReject {
			return nil, NewImmediateRejectionError(lock.Start, lock.End,
				fmt.Errorf("failed to save or process this machine due to active %s lock", MachineLockTypeImmediateReject))
		}
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
	uniqueID, err := uuid.NewV7() // Using UUID as an example
	if err != nil {
		panic(err)
	}

	sm.UniqueID = uniqueID.String()
	return uniqueID.String()
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
		return NewStateTransitionError(sm.CurrentState, newState, event,
			fmt.Errorf("failed to set the state because its an invalid transition from %s to %s", sm.CurrentState, newState))
	}
	sm.CurrentState = newState
	return nil
}

// AddStep adds a handler to the state machine.
func (sm *StateMachine) AddStep(handler StepHandler) *StateMachine {
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
		return NewStateTransitionError(sm.CurrentState, state, OnRollback,
			fmt.Errorf("failed to Rollback because transition from %s to %s is invalid", sm.CurrentState, state))
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
		return NewStateTransitionError(sm.CurrentState, StateOpen, OnResume,
			fmt.Errorf("failed to Resume because transition from %s to %s is invalid", sm.CurrentState, StateOpen))
	}
	sm.CurrentState = StateOpen
	return sm.Run()
}

// ExitParkedState exits the parked state into the specified state.
func (sm *StateMachine) ExitParkedState(newState State) error {
	// Validate the transition out of StateParked
	if !IsValidTransition(StateParked, OnManualOverride, newState) {
		return NewStateTransitionError(StateParked, newState, OnManualOverride,
			fmt.Errorf("failed to exit parked state because transition from %s to %s is invalid", StateParked, newState))
	}
	sm.CurrentState = newState
	return nil
}
