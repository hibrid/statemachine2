package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	statemachine "github.com/hibrid/statemachine2"
)

type Step1 struct {
}

func (handler *Step1) Name() string {
	return "TestHandler" // Provide a default name for the handler
}

func (handler *Step1) ExecuteForward(data map[string]interface{}, transitionHistory []statemachine.TransitionHistory) (statemachine.ForwardEvent, map[string]interface{}, error) {
	// Access and modify arbitrary data in the handler logic
	data["key1"] = "new value1"
	data["key3"] = 456

	// Return the modified data
	return statemachine.ForwardSuccess, data, nil
}

func (handler *Step1) ExecuteBackward(data map[string]interface{}, transitionHistory []statemachine.TransitionHistory) (statemachine.BackwardEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return statemachine.BackwardSuccess, data, nil
}

func (handler *Step1) ExecutePause(data map[string]interface{}, transitionHistory []statemachine.TransitionHistory) (statemachine.PauseEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return statemachine.PauseSuccess, data, nil
}

func (handler *Step1) ExecuteResume(data map[string]interface{}, transitionHistory []statemachine.TransitionHistory) (statemachine.ResumeEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return statemachine.ResumeSuccess, data, nil
}

func (handler *Step1) ExecuteCancel(data map[string]interface{}, transitionHistory []statemachine.TransitionHistory) (statemachine.CancelEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return statemachine.CancelSuccess, data, nil
}

type Step2 struct {
}

func (handler *Step2) Name() string {
	return "RetryTestHandler" // Provide a default name for the handler
}

func (handler *Step2) ExecuteForward(data map[string]interface{}, transitionHistory []statemachine.TransitionHistory) (statemachine.ForwardEvent, map[string]interface{}, error) {
	// Access and modify arbitrary data in the handler logic
	data["key1"] = "new value2"
	data["key3"] = 457
	fmt.Println("RetryTestHandler - ExecuteForward")
	// Return the modified data
	return statemachine.ForwardRetry, data, nil
}

func (handler *Step2) ExecuteBackward(data map[string]interface{}, transitionHistory []statemachine.TransitionHistory) (statemachine.BackwardEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return statemachine.BackwardSuccess, data, nil
}

func (handler *Step2) ExecutePause(data map[string]interface{}, transitionHistory []statemachine.TransitionHistory) (statemachine.PauseEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return statemachine.PauseSuccess, data, nil
}

func (handler *Step2) ExecuteResume(data map[string]interface{}, transitionHistory []statemachine.TransitionHistory) (statemachine.ResumeEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return statemachine.ResumeSuccess, data, nil
}

func (handler *Step2) ExecuteCancel(data map[string]interface{}, transitionHistory []statemachine.TransitionHistory) (statemachine.CancelEvent, map[string]interface{}, error) {
	// Implement backward action logic here.
	return statemachine.CancelSuccess, data, nil
}

func afterEventCallback(sm *statemachine.StateMachine, ctx *statemachine.Context) error {
	// Logic for after event callback
	fmt.Println(fmt.Printf("After event callback triggered for event %s and step %s", ctx.EventEmitted.String(), ctx.Handler.Name()))
	return nil
}

func enterStateCallback(sm *statemachine.StateMachine, ctx *statemachine.Context) error {
	// Logic for enter state callback
	fmt.Println(fmt.Printf("Enter state callback triggered for state %s and step %s", sm.CurrentState, ctx.Handler.Name()))
	return nil
}

func leaveStateCallback(sm *statemachine.StateMachine, ctx *statemachine.Context) error {
	// Logic for leave state callback
	fmt.Println(fmt.Printf("Leave state callback triggered for state %s and step %s", sm.CurrentState, ctx.Handler.Name()))
	return nil
}

func main() {
	// create a mysql instance
	db, err := sql.Open("mysql", "root:killbill@tcp(127.0.0.1:3306)/statemachine")
	if err != nil {
		panic(err)
	}

	//define retry policy (optional)
	retryPolicy := statemachine.RetryPolicy{
		MaxTimeout: 10 * time.Second,
		BaseDelay:  1 * time.Second,
		RetryType:  statemachine.ExponentialBackoff,
	}

	config := statemachine.StateMachineConfig{
		Name: "testing",
		//UniqueStateMachineID: "test1",
		LookupKey:            "5",
		DB:                   db,
		Handlers:             nil,
		ExecuteSynchronously: true,
		RetryPolicy:          retryPolicy,
		LockType:             statemachine.LocalLock,
	}

	sm, err := statemachine.NewStateMachine(config)
	if err != nil {
		panic(err)
	}
	sm.AddStateCallbacks(statemachine.StatePending, statemachine.StateCallbacks{
		AfterAnEvent:  afterEventCallback,
		BeforeTheStep: enterStateCallback,
		AfterTheStep:  leaveStateCallback,
	})

	sm.AddStateCallbacks(statemachine.StateOpen, statemachine.StateCallbacks{
		AfterAnEvent:  afterEventCallback,
		BeforeTheStep: enterStateCallback,
		AfterTheStep:  leaveStateCallback,
	})
	Step1 := &Step1{}
	sm.AddStep(Step1, Step1.Name())
	step2 := &Step2{}
	sm.AddStep(step2, step2.Name())

	err = sm.Run()
	if err != nil {
		panic(err)
	}

}
