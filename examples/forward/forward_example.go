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

type Step2 struct {
}

func (handler *Step2) Name() string {
	return "Step 2 - The Second One" // Provide a default name for the handler
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
		ExecuteSynchronously: true, // Will execute the state machine steps synchronously using recursion and will block until the state machine completes
		RetryPolicy:          retryPolicy,
		LockType:             statemachine.LocalLock,
	}

	sm, err := statemachine.NewStateMachine(config)
	if err != nil {
		switch e := err.(type) {
		case *statemachine.StateTransitionError:
			fmt.Printf("State Transition Error: %s\n", e.Error())
		case *statemachine.DatabaseOperationError:
			fmt.Printf("Database Operation Error: %s\n", e.Error())
		case *statemachine.LockAcquisitionError:
			fmt.Printf("Lock Acquisition Error: %s\n", e.Error())
		case *statemachine.LockAlreadyHeldError:
			fmt.Printf("Lock Already Held Error: %s\n", e.Error())
		default:
			fmt.Printf("Unknown Error: %s\n", e.Error())
		}
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
	step1Handler := &Step1{}
	sm.AddStep(step1Handler, step1Handler.Name())
	step2Handler := &Step2{}
	sm.AddStep(step2Handler, step2Handler.Name())

	err = sm.Run()
	if err != nil {
		switch e := err.(type) {
		case *statemachine.StateTransitionError:
			fmt.Printf("State Transition Error: %s\n", e.Error())
		case *statemachine.DatabaseOperationError:
			fmt.Printf("Database Operation Error: %s\n", e.Error())
		case *statemachine.LockAcquisitionError:
			fmt.Printf("Lock Acquisition Error: %s\n", e.Error())
		case *statemachine.LockAlreadyHeldError:
			fmt.Printf("Lock Already Held Error: %s\n", e.Error())
		default:
			fmt.Printf("Unknown Error: %s\n", e.Error())
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
