package statemachine

// DefaultHandler is a sample implementation of the Handler interface.
type DefaultHandler struct {
	Name string
}

// ExecuteForward executes the forward action for the default handler.
func (handler *DefaultHandler) ExecuteForward(sm *StateMachine, data map[string]interface{}) (map[string]interface{}, error) {
	// Access and modify arbitrary data in the handler logic
	data["key1"] = "new value1"
	data["key3"] = 456

	// Return the modified data
	return data, nil
}

// ExecuteBackward executes the backward action for the default handler.
func (dh *DefaultHandler) ExecuteBackward(sm *StateMachine, data map[string]interface{}) (map[string]interface{}, error) {
	// Implement backward action logic here.
	return data, nil
}

// ExecuteBackward executes the backward action for the default handler.
func (dh *DefaultHandler) ExecutePause(sm *StateMachine, data map[string]interface{}) (map[string]interface{}, error) {
	// Implement backward action logic here.
	return data, nil
}
