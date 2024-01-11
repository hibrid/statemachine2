package statemachine

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

var ValidTransitions = map[State]map[Event][]State{
	StateSleeping: {
		OnWakeUp: []State{StatePending},
	},
	StatePending: {
		OnSuccess:   []State{StateOpen},
		OnFailed:    []State{StateFailed},
		OnCancelled: []State{StateCancelled},
		OnLock:      []State{StateLocked},
		OnPause:     []State{StatePaused},

		OnUnknownSituation: []State{StateParked},
	},
	StateOpen: {
		OnSuccess:   []State{StateOpen},
		OnCompleted: []State{StateCompleted},
		OnFailed:    []State{StateFailed},
		OnPause:     []State{StatePaused},
		OnRollback:  []State{StateStartRollback},
		OnRetry:     []State{StateRetry, StateStartRetry},
		OnLock:      []State{StateLocked},
		OnCancelled: []State{StateCancelled},

		OnUnknownSituation: []State{StateParked},
	},
	StateStartRetry: {
		OnSuccess: []State{StateRetry},
	},
	StateRetry: {
		OnSuccess:      []State{StateOpen},
		OnFailed:       []State{StateFailed},
		OnRetry:        []State{StateRetry},
		OnLock:         []State{StateLocked},
		OnCancelled:    []State{StateCancelled},
		OnResetTimeout: []State{StateFailed},

		OnUnknownSituation: []State{StateParked},
	},
	StateStartRollback: {
		OnSuccess: []State{StateRollback},
	},
	StateRollback: {
		OnSuccess:           []State{StateOpen},
		OnRetry:             []State{StateRetry, StateStartRetry},
		OnRollbackCompleted: []State{StateRollbackCompleted},
		OnRollbackFailed:    []State{StateRollbackFailed},
		OnRollback:          []State{StateRollback},
		OnCancelled:         []State{StateCancelled},
		OnLock:              []State{StateLocked},

		OnUnknownSituation: []State{StateParked},
	},
	StatePaused: {
		OnResume: []State{StateOpen},
		OnFailed: []State{StateFailed},
		OnLock:   []State{StateLocked},
	},
	StateParked: {
		OnLock:           []State{StateLocked},
		OnManualOverride: []State{AnyState},
	},
	StateLocked: {
		OnSuccess:   []State{StateOpen},
		OnCompleted: []State{StateCompleted},
		OnFailed:    []State{StateFailed},
		OnPause:     []State{StatePaused},
		OnRollback:  []State{StateStartRollback},
		OnRetry:     []State{StateRetry, StateStartRetry},
		OnLock:      []State{StateLocked},
		OnCancelled: []State{StateCancelled},

		OnUnknownSituation: []State{StateParked},
	},
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
