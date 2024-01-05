package statemachine

import (
	"fmt"
	"time"
)

type HaltAllStateMachinesByTypeError struct {
	StateMachineError
	LockType      MachineLockType
	StartDateTime time.Time
	EndDateTime   time.Time
	Err           error
}

func (e *HaltAllStateMachinesByTypeError) Error() string {
	return fmt.Sprintf("%s - lock active from %s to %s: %v", e.StateMachineError.Msg, e.StartDateTime.Format(time.RFC3339), e.EndDateTime.Format(time.RFC3339), e.Err)
}

func NewHaltAllStateMachinesByTypeError(startDateTime, endDateTime time.Time, err error) *HaltAllStateMachinesByTypeError {
	return &HaltAllStateMachinesByTypeError{
		StateMachineError: StateMachineError{Msg: "halt all state machines due to active lock"},
		LockType:          MachineLockTypeHaltAll,
		StartDateTime:     startDateTime,
		EndDateTime:       endDateTime,
		Err:               fmt.Errorf("halt all state machines: %w", err),
	}
}

type ImmediateRejectionError struct {
	StateMachineError
	LockType      MachineLockType
	StartDateTime time.Time
	EndDateTime   time.Time
	// Embed the error interface
	Err error
}

func (e *ImmediateRejectionError) Error() string {
	return fmt.Sprintf("%s - lock active from %s to %s: %v", e.StateMachineError.Msg, e.StartDateTime.Format(time.RFC3339), e.EndDateTime.Format(time.RFC3339), e.Err)
}

func NewImmediateRejectionError(startDateTime, endDateTime time.Time, err error) *ImmediateRejectionError {
	return &ImmediateRejectionError{
		StateMachineError: StateMachineError{Msg: fmt.Sprintf("immediate rejection due to active %s lock", MachineLockTypeImmediateReject)},
		LockType:          MachineLockTypeImmediateReject,
		StartDateTime:     startDateTime,
		EndDateTime:       endDateTime,
		Err:               fmt.Errorf("immediate rejection: %w", err), // Wrap the underlying error
	}
}

type SleepStateError struct {
	StateMachineError
	LockType      MachineLockType
	StartDateTime time.Time
	EndDateTime   time.Time
	Err           error
}

func (e *SleepStateError) Error() string {
	return fmt.Sprintf("%s - lock active from %s to %s: %v", e.StateMachineError.Msg, e.StartDateTime.Format(time.RFC3339), e.EndDateTime.Format(time.RFC3339), e.Err)
}

func NewSleepStateError(startDateTime, endDateTime time.Time, err error) *SleepStateError {
	return &SleepStateError{
		StateMachineError: StateMachineError{Msg: "sleep state due to active lock"},
		LockType:          MachineLockTypeSleepState,
		StartDateTime:     startDateTime,
		EndDateTime:       endDateTime,
		Err:               fmt.Errorf("sleep state: %w", err),
	}
}

type StateTransitionError struct {
	StateMachineError
	FromState State
	ToState   State
	Event     Event
	Err       error
}

func (e *StateTransitionError) Error() string {
	return fmt.Sprintf("%s - invalid transition from %s to %s on event %s: %v", e.StateMachineError.Msg, e.FromState, e.ToState, e.Event, e.Err)
}

func NewStateTransitionError(fromState State, toState State, event Event, err error) *StateTransitionError {
	return &StateTransitionError{
		StateMachineError: StateMachineError{Msg: "invalid state transition"},
		FromState:         fromState,
		ToState:           toState,
		Event:             event,
		Err:               fmt.Errorf("state transition error: %w", err),
	}
}

type DatabaseOperationError struct {
	StateMachineError
	Operation string
	Err       error
}

func (e *DatabaseOperationError) Error() string {
	return fmt.Sprintf("%s - error in operation %s: %v", e.StateMachineError.Msg, e.Operation, e.Err)
}

func NewDatabaseOperationError(operation string, err error) *DatabaseOperationError {
	return &DatabaseOperationError{
		StateMachineError: StateMachineError{Msg: "database operation error"},
		Operation:         operation,
		Err:               fmt.Errorf("database operation: %w", err),
	}
}

type LockAcquisitionError struct {
	StateMachineError
	LockType LockType
	Detail   string
	Err      error
}

func (e *LockAcquisitionError) Error() string {
	return fmt.Sprintf("%s - error acquiring %s lock: %s, detail: %v", e.StateMachineError.Msg, e.LockType, e.Detail, e.Err)
}

func NewLockAcquisitionError(lockType LockType, detail string, err error) *LockAcquisitionError {
	return &LockAcquisitionError{
		StateMachineError: StateMachineError{Msg: "lock acquisition error"},
		LockType:          lockType,
		Detail:            detail,
		Err:               fmt.Errorf("lock acquisition: %w", err),
	}
}

type LockAlreadyHeldError struct {
	StateMachineError
	LockType LockType
	Err      error
}

func (e *LockAlreadyHeldError) Error() string {
	return fmt.Sprintf("%s - lock of type %s is already held by another instance: %v", e.StateMachineError.Msg, e.LockType, e.Err)
}

func NewLockAlreadyHeldError(lockType LockType, err error) *LockAlreadyHeldError {
	return &LockAlreadyHeldError{
		StateMachineError: StateMachineError{Msg: "lock already held"},
		LockType:          lockType,
		Err:               fmt.Errorf("lock already held: %w", err),
	}
}
