package statemachine

type State string

func (s State) String() string {
	return string(s)
}

const (

	// StateSleep is the state of a state machine when it is sleeping.
	// ExecuteForward
	StateSleeping State = "sleeping"

	// StatePending is the initial state of a state machine waiting to be picked up.
	// ExecuteForward
	StatePending State = "pending"

	// StateOpen is the state of a state machine when it is in progress and not locked.
	// ExecuteForward
	StateOpen State = "open"

	// StateLocked is the state of a state machine when it is in progress and locked.
	// ExecuteForward
	StateLocked State = "locked"

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
