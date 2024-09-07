package statemachine

import "go.uber.org/zap"

type ExecuteForwardFunc func(data map[string]interface{}, transitionHistory []TransitionHistory) (ForwardEvent, map[string]interface{}, error)
type ExecuteBackwardFunc func(data map[string]interface{}, transitionHistory []TransitionHistory) (BackwardEvent, map[string]interface{}, error)
type ExecutePauseFunc func(data map[string]interface{}, transitionHistory []TransitionHistory) (PauseEvent, map[string]interface{}, error)
type ExecuteResumeFunc func(data map[string]interface{}, transitionHistory []TransitionHistory) (ResumeEvent, map[string]interface{}, error)

// StepHandler defines the interface for state machine handlers.
type StepHandler interface {
	Name() string
	ExecuteForward(data map[string]interface{}, transitionHistory []TransitionHistory) (ForwardEvent, map[string]interface{}, error)
	ExecuteBackward(data map[string]interface{}, transitionHistory []TransitionHistory) (BackwardEvent, map[string]interface{}, error)
	ExecutePause(data map[string]interface{}, transitionHistory []TransitionHistory) (PauseEvent, map[string]interface{}, error)
	ExecuteResume(data map[string]interface{}, transitionHistory []TransitionHistory) (ResumeEvent, map[string]interface{}, error)
}

// completeHandler is a default handler that does nothing but complete the state machine.
type completeHandler struct {
	Logger *zap.Logger
	StepHandler
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

// completeHandler is a default handler that does nothing but complete the state machine.
type cancelHandler struct {
	Logger *zap.Logger
	StepHandler
}

func (handler *cancelHandler) Name() string {
	return "CompleteHandler" // Provide a default name for the handler
}

func (handler *cancelHandler) ExecuteForward(data map[string]interface{}, transitionHistory []TransitionHistory) (ForwardEvent, map[string]interface{}, error) {
	// Return the modified data
	return ForwardCancel, data, nil
}

func (dh *cancelHandler) ExecuteBackward(data map[string]interface{}, transitionHistory []TransitionHistory) (BackwardEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return BackwardCancel, data, nil
}

func (dh *cancelHandler) ExecutePause(data map[string]interface{}, transitionHistory []TransitionHistory) (PauseEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return PauseCancel, data, nil
}

func (dh *cancelHandler) ExecuteResume(data map[string]interface{}, transitionHistory []TransitionHistory) (ResumeEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return ResumeCancel, data, nil
}

type BaseStepHandler struct {
	Logger *zap.Logger
	ExecuteForwardFunc
	ExecuteBackwardFunc
	ExecutePauseFunc
	ExecuteResumeFunc
	NameFunc func() string
}

func (b *BaseStepHandler) Name() string {
	if b.NameFunc != nil {
		return b.NameFunc()
	}
	return "BaseStepHandler"
}

// Implement the StepHandler interface using the function fields
func (b *BaseStepHandler) ExecuteForward(data map[string]interface{}, transitionHistory []TransitionHistory) (ForwardEvent, map[string]interface{}, error) {
	return b.ExecuteForwardFunc(data, transitionHistory)
}

func (b *BaseStepHandler) ExecuteBackward(data map[string]interface{}, transitionHistory []TransitionHistory) (BackwardEvent, map[string]interface{}, error) {
	return b.ExecuteBackwardFunc(data, transitionHistory)
}

func (b *BaseStepHandler) ExecutePause(data map[string]interface{}, transitionHistory []TransitionHistory) (PauseEvent, map[string]interface{}, error) {
	return b.ExecutePauseFunc(data, transitionHistory)
}

func (b *BaseStepHandler) ExecuteResume(data map[string]interface{}, transitionHistory []TransitionHistory) (ResumeEvent, map[string]interface{}, error) {
	return b.ExecuteResumeFunc(data, transitionHistory)
}

func (b *BaseStepHandler) SetLogger(logger *zap.Logger) {
	b.Logger = logger
}

// NewStep is a constructor function for BaseStepHandler. It initializes a new BaseStepHandler with the provided
// name, logger, and function implementations for executing forward, backward, pause, and resume actions.
//
// Parameters:
// - name: A string representing the name of the step.
// - logger: A *zap.Logger instance for logging purposes.
// - executeForward: A function implementing the ExecuteForwardFunc type.
// - executeBackward: A function implementing the ExecuteBackwardFunc type.
// - executePause: A function implementing the ExecutePauseFunc type.
// - executeResume: A function implementing the ExecuteResumeFunc type.
//
// Returns:
// - A pointer to an initialized BaseStepHandler instance.
//
// Example usage:
//
//	handler := &Step1{}
//	logger := zap.NewNop()
//	stepHandler := NewStep(
//	    handler.Name(),
//	    logger,
//	    handler.ExecuteForward,
//	    handler.ExecuteBackward,
//	    handler.ExecutePause,
//	    handler.ExecuteResume,
//	)
func NewStep(
	name string,
	logger *zap.Logger,
	executeForward ExecuteForwardFunc,
	executeBackward ExecuteBackwardFunc,
	executePause ExecutePauseFunc,
	executeResume ExecuteResumeFunc,
) *BaseStepHandler {
	return &BaseStepHandler{
		NameFunc:            func() string { return name },
		Logger:              logger,
		ExecuteForwardFunc:  executeForward,
		ExecuteBackwardFunc: executeBackward,
		ExecutePauseFunc:    executePause,
		ExecuteResumeFunc:   executeResume,
	}
}

// NewStepFromHandler initializes a new BaseStepHandler with the provided handler and logger.
//
// Parameters:
// - handler: An interface that implements the required methods (Name, ExecuteForward, ExecuteBackward, ExecutePause, ExecuteResume).
// - logger: A *zap.Logger instance for logging purposes.
//
// Returns:
// - A pointer to an initialized BaseStepHandler instance.
//
// Example usage:
//
//	handler := &Step1{}
//	logger := zap.NewNop()
//	stepHandler := NewStepFromHandler(handler, logger)
func NewStepFromHandler(handler interface {
	Name() string
	ExecuteForward(data map[string]interface{}, transitionHistory []TransitionHistory) (ForwardEvent, map[string]interface{}, error)
	ExecuteBackward(data map[string]interface{}, transitionHistory []TransitionHistory) (BackwardEvent, map[string]interface{}, error)
	ExecutePause(data map[string]interface{}, transitionHistory []TransitionHistory) (PauseEvent, map[string]interface{}, error)
	ExecuteResume(data map[string]interface{}, transitionHistory []TransitionHistory) (ResumeEvent, map[string]interface{}, error)
}, logger *zap.Logger) *BaseStepHandler {
	return &BaseStepHandler{
		NameFunc:            handler.Name,
		Logger:              logger,
		ExecuteForwardFunc:  handler.ExecuteForward,
		ExecuteBackwardFunc: handler.ExecuteBackward,
		ExecutePauseFunc:    handler.ExecutePause,
		ExecuteResumeFunc:   handler.ExecuteResume,
	}
}
