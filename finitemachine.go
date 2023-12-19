package statemachine

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

func (smCtx *Context) Handle() (executionEvent Event, err error) {

	var outputArbitraryData map[string]interface{}

	inputArbitraryData := smCtx.InputArbitraryData
	outputArbitraryData = smCtx.InputArbitraryData

	defer func() {
		err = smCtx.finishHandlingContext(executionEvent, inputArbitraryData, outputArbitraryData)
		if err != nil {
			executionEvent = OnError
		}
	}()

	// TODO: Add retry state

	if smCtx.InputState == StatePending || smCtx.InputState == StateOpen {
		executionEvent, outputArbitraryData, err = smCtx.Handler.ExecuteForward(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		_ = outputArbitraryData
		return executionEvent, err
	}

	if smCtx.InputState == StateCompleted {
		executionEvent = OnAlreadyCompleted
		return executionEvent, err // Skip this step
	}

	if smCtx.InputState == StateFailed {
		executionEvent = OnFailed
		return executionEvent, err // Skip this step
	}

	// other states
	if smCtx.InputState == StatePaused {
		executionEvent, outputArbitraryData, err = smCtx.Handler.ExecutePause(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		_ = outputArbitraryData
		return executionEvent, err
	}

	if smCtx.InputState == StateRollback {
		executionEvent, outputArbitraryData, err = smCtx.Handler.ExecuteBackward(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		_ = outputArbitraryData
		return executionEvent, err
	}

	if smCtx.InputState == StateOpen {
		executionEvent, outputArbitraryData, err = smCtx.Handler.ExecuteForward(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		_ = outputArbitraryData
		return executionEvent, err
	}

	if smCtx.InputState == StateResume {
		executionEvent, outputArbitraryData, err = smCtx.Handler.ExecuteResume(smCtx.InputArbitraryData, smCtx.TransitionHistory)
		_ = outputArbitraryData
		return executionEvent, err
	}

	return executionEvent, err
}
