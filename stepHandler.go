package statemachine

type completeHandler struct {
}

func (handler *completeHandler) Name() string {
	return "CompleteHandler" // Provide a default name for the handler
}

func (handler *completeHandler) ExecuteForward(data map[string]interface{}, transitionHistory []TransitionHistory) (ForwardEvent, map[string]interface{}, error) {
	// Return the modified data
	return ForwardComplete, data, nil
}

func (dh *completeHandler) ExecuteBackward(data map[string]interface{}, transitionHistory []TransitionHistory) (BackwardEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return BackwardComplete, data, nil
}

func (dh *completeHandler) ExecutePause(data map[string]interface{}, transitionHistory []TransitionHistory) (PauseEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return PauseSuccess, data, nil
}

func (dh *completeHandler) ExecuteResume(data map[string]interface{}, transitionHistory []TransitionHistory) (ResumeEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return ResumeSuccess, data, nil
}
