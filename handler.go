package statemachine

type defaultHandler struct {
}

func (handler *defaultHandler) Name() string {
	return "DefaultHandler" // Provide a default name for the handler
}

func (handler *defaultHandler) ExecuteForward(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Return the modified data
	return OnSuccess, data, nil
}

func (dh *defaultHandler) ExecuteBackward(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnSuccess, data, nil
}

func (dh *defaultHandler) ExecutePause(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnFailed, data, nil
}

func (dh *defaultHandler) ExecuteResume(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnFailed, data, nil
}
