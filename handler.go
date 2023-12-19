package statemachine

type DefaultHandler struct {
}

func (handler *DefaultHandler) Name() string {
	return "DefaultHandler" // Provide a default name for the handler
}

func (handler *DefaultHandler) ExecuteForward(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Access and modify arbitrary data in the handler logic
	data["key1"] = "new value1"
	data["key3"] = 456

	// Return the modified data
	return OnSuccess, data, nil
}

func (dh *DefaultHandler) ExecuteBackward(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnSuccess, data, nil
}

func (dh *DefaultHandler) ExecutePause(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnFailed, data, nil
}

func (dh *DefaultHandler) ExecuteResume(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnFailed, data, nil
}
