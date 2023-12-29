package statemachine

import "errors"

// Context represents the context of the state machine.
type Context struct {
	InputState          State                     `json:"inputState"`
	EventEmitted        Event                     `json:"eventEmitted"`
	InputArbitraryData  map[string]interface{}    `json:"inputArbitraryData"`
	OutputArbitraryData map[string]interface{}    `json:"outputArbitraryData"`
	Handler             Handler                   `json:"-"`
	Callbacks           map[string]StateCallbacks `json:"-"`
	StepNumber          int                       `json:"stepNumber"`
	TransitionHistory   []TransitionHistory       `json:"transitionHistory"`
	StateMachine        *StateMachine             `json:"-"`
}

func (smCtx *Context) finishHandlingContext(event Event, input, output map[string]interface{}) error {
	smCtx.EventEmitted = event
	smCtx.InputArbitraryData = input
	smCtx.OutputArbitraryData = output
	// Trigger after-event callback
	if callbacks, ok := smCtx.Callbacks[smCtx.InputState.String()]; ok {
		cb := callbacks.AfterEvent
		if err := cb(smCtx.StateMachine, smCtx); err != nil {
			return err // OnError is a hypothetical event representing an error state
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
func DetermineExecutionAction(inputArbitraryData map[string]interface{}, smCtx *Context) (executionEvent Event, err error) {
	if IsTerminalState(smCtx.InputState) {
		return DetermineTerminalStateEvent(smCtx), nil
	}
	var outputData map[string]interface{}
	switch smCtx.InputState {
	case StateParked:
		// If parked, try to restart execution based on previous state
		lastState := getLastNonParkedState(smCtx.TransitionHistory)
		return restartExecutionFromState(lastState, smCtx)

	case StatePending, StateOpen:
		executionEvent, outputData, err = smCtx.Handler.ExecuteForward(inputArbitraryData, smCtx.TransitionHistory)

	case StatePaused:
		executionEvent, outputData, err = smCtx.Handler.ExecutePause(inputArbitraryData, smCtx.TransitionHistory)

	case StateRollback:
		executionEvent, outputData, err = smCtx.Handler.ExecuteBackward(inputArbitraryData, smCtx.TransitionHistory)

	case StateResume:
		executionEvent, outputData, err = smCtx.Handler.ExecuteResume(inputArbitraryData, smCtx.TransitionHistory)

	default: // not sure what happened so let's park it
		executionEvent = OnParked
	}
	smCtx.OutputArbitraryData = outputData
	return executionEvent, err
}

func getLastNonParkedState(history []TransitionHistory) State {
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].ModifiedState != StateParked {
			return history[i].ModifiedState
		}
	}
	return StateUnknown // Or some default state
}

func restartExecutionFromState(state State, smCtx *Context) (Event, error) {
	switch state {
	case StatePending, StateOpen:
		executionEvent, _, err := smCtx.Handler.ExecuteForward(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		return executionEvent, err
	case StatePaused:
		executionEvent, _, err := smCtx.Handler.ExecutePause(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		return executionEvent, err
	case StateRollback:
		executionEvent, _, err := smCtx.Handler.ExecuteBackward(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		return executionEvent, err
	case StateResume:
		executionEvent, _, err := smCtx.Handler.ExecuteResume(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		return executionEvent, err
	default:
		if IsTerminalState(state) {
			return DetermineTerminalStateEvent(smCtx), errors.New("cannot restart execution from terminal state")
		}
		return OnFailed, nil
	}
}

func (smCtx *Context) Handle() (executionEvent Event, err error) {

	inputArbitraryData := CopyMap(smCtx.InputArbitraryData) // this will end up with the same value as out
	outputArbitraryData := inputArbitraryData

	defer func() {
		err = smCtx.finishHandlingContext(executionEvent, smCtx.InputArbitraryData, outputArbitraryData)
		if err != nil {
			executionEvent = OnError
		}
	}()

	executionEvent, err = DetermineExecutionAction(inputArbitraryData, smCtx)
	outputArbitraryData = CopyMap(smCtx.OutputArbitraryData)

	return executionEvent, err
}

func CopyMap(m map[string]interface{}) map[string]interface{} {
	cp := make(map[string]interface{})
	for k, v := range m {
		vm, ok := v.(map[string]interface{})
		if ok {
			cp[k] = CopyMap(vm)
		} else {
			cp[k] = v
		}
	}

	return cp
}
