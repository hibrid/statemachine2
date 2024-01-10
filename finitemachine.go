package statemachine

import (
	"fmt"
)

// Context represents the context of the state machine.
type Context struct {
	InputState          State                     `json:"inputState"`
	EventEmitted        Event                     `json:"eventEmitted"`
	InputArbitraryData  map[string]interface{}    `json:"inputArbitraryData"`
	OutputArbitraryData map[string]interface{}    `json:"outputArbitraryData"`
	Handler             StepHandler               `json:"-"`
	Callbacks           map[string]StateCallbacks `json:"-"`
	StepNumber          int                       `json:"stepNumber"`
	TransitionHistory   []TransitionHistory       `json:"transitionHistory"`
	StateMachine        *StateMachine             `json:"-"`
}

// StateMachineError is a base struct for custom errors in the state machine package.
type StateMachineError struct {
	Msg string
}

func (e *StateMachineError) Error() string {
	return e.Msg
}

// UnexpectedEventTypeError indicates an unexpected event type was encountered.
type UnexpectedEventTypeError struct {
	StateMachineError
	EventType string
}

func NewUnexpectedEventTypeError(eventType string) *UnexpectedEventTypeError {
	return &UnexpectedEventTypeError{
		StateMachineError: StateMachineError{Msg: fmt.Sprintf("unexpected event type: %s", eventType)},
		EventType:         eventType,
	}
}

// RestartExecutionError indicates an error in restarting execution from a terminal state.
type RestartExecutionError struct {
	StateMachineError
	State State
}

func NewRestartExecutionError(state State) *RestartExecutionError {
	return &RestartExecutionError{
		StateMachineError: StateMachineError{Msg: fmt.Sprintf("cannot restart execution from terminal state: %s", state.String())},
		State:             state,
	}
}

// ExecutionActionError indicates an error in determining the execution action.
type ExecutionActionError struct {
	StateMachineError
}

func NewExecutionActionError(msg string) *ExecutionActionError {
	return &ExecutionActionError{
		StateMachineError: StateMachineError{Msg: fmt.Sprintf("Execution Action Error: %s", msg)},
	}
}

// HandlingContextError indicates an error in handling the context.
type HandlingContextError struct {
	StateMachineError
}

func NewHandlingContextError(msg string) *HandlingContextError {
	return &HandlingContextError{
		StateMachineError: StateMachineError{Msg: fmt.Sprintf("Handling Context Error: %s", msg)},
	}
}

func (smCtx *Context) finishHandlingContext(event Event, input, output map[string]interface{}) error {
	smCtx.EventEmitted = event
	smCtx.InputArbitraryData = input
	smCtx.OutputArbitraryData = output
	// Trigger after-event callback
	if callbacks, ok := smCtx.Callbacks[smCtx.InputState.String()]; ok {
		cb := callbacks.AfterAnEvent
		if err := cb(smCtx.StateMachine, smCtx); err != nil {
			return err
		}
	}
	return nil
}

func DetermineTerminalStateEvent(smCtx *Context) (executionEvent Event) {
	switch smCtx.InputState {
	case StateCompleted:
		executionEvent = OnAlreadyCompleted
		// Skip this step

	case StateFailed:
		executionEvent = OnFailed
		// Skip this step

	case StateRollbackCompleted:
		executionEvent = OnAlreadyRollbackCompleted
		// Skip this step

	case StateRollbackFailed:
		executionEvent = OnRollbackFailed
		// Skip this step

	case StateCancelled:
		executionEvent = OnCancelled
		// Skip this step
	}
	return executionEvent
}

// DetermineExecutionAction determines the execution action based on the input state.
func DetermineExecutionAction(inputArbitraryData map[string]interface{}, smCtx *Context) (convertedEvent Event, err error) {
	if IsTerminalState(smCtx.InputState) {
		return DetermineTerminalStateEvent(smCtx), nil
	}
	var outputData map[string]interface{}
	var executionEvent interface{}
	switch smCtx.InputState {
	case StateRetry:
		lastState := getLastNonRetryState(smCtx.TransitionHistory)
		return restartExecutionFromState(lastState, smCtx)
	case StateParked:
		// If parked, try to restart execution based on previous state
		lastState := getLastNonParkedState(smCtx.TransitionHistory)
		return restartExecutionFromState(lastState, smCtx)

	case StateSleeping, StatePending, StateOpen:
		executionEvent, outputData, err = smCtx.Handler.ExecuteForward(inputArbitraryData, smCtx.TransitionHistory)
		// assert that executionevent is a forwardEven
		if forwardEvent, ok := executionEvent.(ForwardEvent); ok {
			// forwardEvent is now the concrete type
			// Convert it to a generic event
			convertedEvent = forwardEvent.ToEvent()
		} else {
			return OnError, NewUnexpectedEventTypeError(fmt.Sprintf("%T", executionEvent))
		}
	case StatePaused:
		executionEvent, outputData, err = smCtx.Handler.ExecutePause(inputArbitraryData, smCtx.TransitionHistory)
		// assert that executionevent is a PauseEvent
		if pauseEvent, ok := executionEvent.(PauseEvent); ok {
			// pauseEvent is now the concrete type
			// Convert it to a generic event
			convertedEvent = pauseEvent.ToEvent()
		} else {
			// Handle the error or unexpected type
			return OnError, NewUnexpectedEventTypeError(fmt.Sprintf("%T", executionEvent))
		}
	case StateRollback:
		executionEvent, outputData, err = smCtx.Handler.ExecuteBackward(inputArbitraryData, smCtx.TransitionHistory)
		// assert that executionevent is a BackwardEvent
		if backwardEvent, ok := executionEvent.(BackwardEvent); ok {
			// backwardEvent is now the concrete type
			// Convert it to a generic event
			convertedEvent = backwardEvent.ToEvent()
		} else {
			// Handle the error or unexpected type
			return OnError, NewUnexpectedEventTypeError(fmt.Sprintf("%T", executionEvent))
		}
	case StateResume:
		executionEvent, outputData, err = smCtx.Handler.ExecuteResume(inputArbitraryData, smCtx.TransitionHistory)
		// assert that executionevent is a ResumeEvent
		if resumeEvent, ok := executionEvent.(ResumeEvent); ok {
			// resumeEvent is now the concrete type
			// Convert it to a generic event
			convertedEvent = resumeEvent.ToEvent()
		} else {
			// Handle the error or unexpected type
			return OnError, NewUnexpectedEventTypeError(fmt.Sprintf("%T", executionEvent))
		}

	default: // not sure what happened so let's park it
		convertedEvent = OnParked
	}
	smCtx.OutputArbitraryData = outputData
	return convertedEvent, err
}

func restartExecutionFromState(state State, smCtx *Context) (Event, error) {
	switch state {
	case StateSleeping, StatePending, StateOpen:
		executionEvent, _, err := smCtx.Handler.ExecuteForward(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		return executionEvent.ToEvent(), err
	case StatePaused:
		executionEvent, _, err := smCtx.Handler.ExecutePause(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		return executionEvent.ToEvent(), err
	case StateRollback:
		executionEvent, _, err := smCtx.Handler.ExecuteBackward(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		return executionEvent.ToEvent(), err
	case StateResume:
		executionEvent, _, err := smCtx.Handler.ExecuteResume(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		return executionEvent.ToEvent(), err
	default:
		if IsTerminalState(state) {
			return DetermineTerminalStateEvent(smCtx), NewRestartExecutionError(state)
		}
		return OnFailed, nil
	}
}

func getLastNonParkedState(history []TransitionHistory) State {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].ModifiedState != StateParked {
			return history[i].ModifiedState
		}
	}
	return StateUnknown // Or some default state
}

func getLastNonRetryState(history []TransitionHistory) State {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].ModifiedState != StateRetry {
			return history[i].ModifiedState
		}
	}
	return StateUnknown // Or some default state
}

func (smCtx *Context) Handle() (executionEvent Event, err error) {

	inputArbitraryData := CopyMap(smCtx.InputArbitraryData) // this will end up with the same value as out
	outputArbitraryData := inputArbitraryData

	defer func() {
		if r := recover(); r != nil {
			// Convert the panic to an error and wrap it
			err = fmt.Errorf("panic occurred in Handle: %v", r)
			executionEvent = OnError
		}

		if finishErr := smCtx.finishHandlingContext(executionEvent, smCtx.InputArbitraryData, outputArbitraryData); finishErr != nil {
			// Wrap the error from finishHandlingContext with custom error type
			err = NewHandlingContextError(finishErr.Error())
			executionEvent = OnError
		}
	}()

	executionEvent, err = DetermineExecutionAction(inputArbitraryData, smCtx)
	if err != nil {
		// Wrap the error from DetermineExecutionAction with custom error type
		err = NewExecutionActionError(err.Error())
		return executionEvent, err
	}
	outputArbitraryData = CopyMap(smCtx.OutputArbitraryData)

	return executionEvent, err
}
