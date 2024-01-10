package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"

	statemachine "github.com/hibrid/statemachine2"
)

type Step1 struct {
}

func (handler *Step1) Name() string {
	return "Step1 - The First Step" // Provide a default name for the handler
}

func (handler *Step1) ExecuteForward(data map[string]interface{}, transitionHistory []statemachine.TransitionHistory) (statemachine.ForwardEvent, map[string]interface{}, error) {
	// Access and modify arbitrary data in the handler logic
	data["RemoteID"] = "externalidentifier"
	data["Provider"] = "someprovider"

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
	return "Step 2 - The Second One" // Provide a default name for the handler
}

func (handler *Step2) GetLogger() *zap.Logger {
	return nil
}

func (handler *Step2) ExecuteForward(data map[string]interface{}, transitionHistory []statemachine.TransitionHistory) (statemachine.ForwardEvent, map[string]interface{}, error) {
	// Access and modify arbitrary data in the handler logic
	data["IpAddress"] = "192.168.0.1"

	// Return the modified data
	return statemachine.ForwardSuccess, data, nil
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

func leaveStateCallback2(sm *statemachine.StateMachine, ctx *statemachine.Context) error {
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
	context, _ := context.WithCancel(context.Background())
	config := statemachine.StateMachineConfig{
		Name: "testing",
		//UniqueStateMachineID: "test1",
		LookupKey:            "5",
		DB:                   db,
		Handlers:             nil,
		ExecuteSynchronously: true, // Will execute the state machine steps synchronously using recursion and will block until the state machine completes
		RetryPolicy:          retryPolicy,
		LockType:             statemachine.LocalLock,
		Context:              context,
	}

	sm, err := statemachine.NewStateMachine(config)
	if err != nil {
		var stateTransitionErr *statemachine.StateTransitionError
		var dbOpErr *statemachine.DatabaseOperationError
		var lockAcquisitionErr *statemachine.LockAcquisitionError
		var lockAlreadyHeldErr *statemachine.LockAlreadyHeldError

		switch {
		case errors.As(err, &stateTransitionErr):
			fmt.Printf("State Transition Error: %s\n", stateTransitionErr.Error())
		case errors.As(err, &dbOpErr):
			fmt.Printf("Database Operation Error: %s\n", dbOpErr.Error())
		case errors.As(err, &lockAcquisitionErr):
			fmt.Printf("Lock Acquisition Error: %s\n", lockAcquisitionErr.Error())
		case errors.As(err, &lockAlreadyHeldErr):
			fmt.Printf("Lock Already Held Error: %s\n", lockAlreadyHeldErr.Error())
		default:
			fmt.Printf("Unknown Error: %s\n", err.Error())
		}

		panic(err)
	}

	leaveStateCallback := func(sm *statemachine.StateMachine, ctx *statemachine.Context) error {
		//cancel()
		return nil
	}

	sm.AddStateCallbacks(statemachine.StatePending, statemachine.StateCallbacks{
		AfterAnEvent:  afterEventCallback,
		BeforeTheStep: enterStateCallback,
		AfterTheStep:  leaveStateCallback,
	})

	sm.AddStateCallbacks(statemachine.StateOpen, statemachine.StateCallbacks{
		AfterAnEvent:  afterEventCallback,
		BeforeTheStep: enterStateCallback,
		AfterTheStep:  leaveStateCallback2,
	})
	step1Handler := &Step1{}
	step1 := statemachine.NewStep(step1Handler.Name(), zap.NewNop(), step1Handler.ExecuteForward, step1Handler.ExecuteBackward, step1Handler.ExecutePause, step1Handler.ExecuteResume)
	sm.AddStep(*step1, step1Handler.Name())
	step2Handler := &Step2{}
	//sm.AddStep(step2Handler, step2Handler.Name())
	step2 := statemachine.NewStep(step2Handler.Name(), zap.NewNop(), step2Handler.ExecuteForward, step2Handler.ExecuteBackward, step2Handler.ExecutePause, step2Handler.ExecuteResume)
	sm.AddStep(*step2, step2Handler.Name())
	//cancel()
	err = sm.Run()
	if err != nil {
		var stateTransitionErr *statemachine.StateTransitionError
		var dbOpErr *statemachine.DatabaseOperationError
		var lockAcquisitionErr *statemachine.LockAcquisitionError
		var lockAlreadyHeldErr *statemachine.LockAlreadyHeldError

		switch {
		case errors.As(err, &stateTransitionErr):
			fmt.Printf("State Transition Error: %s\n", stateTransitionErr.Error())
		case errors.As(err, &dbOpErr):
			fmt.Printf("Database Operation Error: %s\n", dbOpErr.Error())
		case errors.As(err, &lockAcquisitionErr):
			fmt.Printf("Lock Acquisition Error: %s\n", lockAcquisitionErr.Error())
		case errors.As(err, &lockAlreadyHeldErr):
			fmt.Printf("Lock Already Held Error: %s\n", lockAlreadyHeldErr.Error())
		default:
			fmt.Printf("Unknown Error: %s\n", err.Error())
		}

		panic(err)
	}

	if !sm.DidStateMachineComplete() {
		if sm.DidStateMachineFail() {
			fmt.Println("State machine failed")
		}

		if sm.DidStateMachineRollback() {
			fmt.Println("State machine rolled back")
		}

		if sm.DidStateMachinePause() {
			fmt.Println("State machine paused")
		}

		if sm.IsTheStateMachineInATerminalState() {
			fmt.Println("State machine is in a terminal state")
		}
	} else if sm.DidStateMachineComplete() {
		fmt.Println("State machine completed successfully")
		ipAddress := sm.GetCurrentArbitraryData()["IpAddress"]
		fmt.Println("The IP Address Assigned is: ", ipAddress.(string))
	}

}
