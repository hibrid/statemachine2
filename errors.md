## Custom Error Types

In the state machine package, we have defined several custom error types to handle specific scenarios more effectively. These error types provide additional context and make error handling more consistent and informative.

### 1. `StateMachineError`

**Definition:**
```go
type StateMachineError struct {
    Msg string
}
```
A base struct for custom errors in the state machine package. It contains a message describing the error.

### 2. `StateTransitionError`

**Definition:**
```go
type StateTransitionError struct {
    StateMachineError
    FromState State
    ToState   State
    Event     Event
}
```
Indicates an error during a state transition. It includes information about the source state, target state, and the event that triggered the transition.

**Constructor:**
```go
func NewStateTransitionError(fromState State, toState State, event Event) *StateTransitionError
```

### 3. `DatabaseOperationError`

**Definition:**
```go
type DatabaseOperationError struct {
    StateMachineError
    Operation string
}
```
Represents errors that occur during database operations. It includes details about the specific operation (e.g., "updateStateMachineState") that failed.

**Constructor:**
```go
func NewDatabaseOperationError(operation string, err error) *DatabaseOperationError
```

### 4. `LockAcquisitionError`

**Definition:**
```go
type LockAcquisitionError struct {
    StateMachineError
    LockType LockType
    Detail   string
}
```
Used when there is an error in acquiring a lock (either global or local). It provides details about the type of lock and the specific part of the lock acquisition process that failed.

**Constructor:**
```go
func NewLockAcquisitionError(lockType LockType, detail string, err error) *LockAcquisitionError
```

### 5. `LockAlreadyHeldError`

**Definition:**
```go
type LockAlreadyHeldError struct {
    StateMachineError
    LockType LockType
}
```
Indicates that a lock (global or local) is already held by another instance. This is a common scenario in distributed systems or concurrent processing.

**Constructor:**
```go
func NewLockAlreadyHeldError(lockType LockType) *LockAlreadyHeldError
```

---

These custom error types are used throughout the state machine package to provide more descriptive and structured error handling, enhancing the maintainability and debuggability of the code.