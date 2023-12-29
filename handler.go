package statemachine

type completeHandler struct {
}

func (handler *completeHandler) Name() string {
	return "CompleteHandler" // Provide a default name for the handler
}

func (handler *completeHandler) ExecuteForward(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Return the modified data
	return OnCompleted, data, nil
}

func (dh *completeHandler) ExecuteBackward(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnSuccess, data, nil
}

func (dh *completeHandler) ExecutePause(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnSuccess, data, nil
}

func (dh *completeHandler) ExecuteResume(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnSuccess, data, nil
}
