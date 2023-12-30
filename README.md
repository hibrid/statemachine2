State Machine Management:

Define and manage state machines with unique names and IDs.
Set the initial state for a state machine.
Add custom handlers to a state machine.
Execution Control:

Execute state machine handlers in a specific order.
Support forward and backward movement through the state machine.
Pause and resume execution of a state machine.
Control whether to execute the next step immediately.
Database Integration:

Store and retrieve state machines and their states in a MySQL database.
Create tables for each state machine type if they don't exist.
Insert and update state machines in the database.
Query state machines by type.
Kafka Integration:

Send Kafka events for state changes if configured.
Locking Mechanism:

Implement locking for state machines based on global and local configurations.
Ensure only one state machine is processed at a time per user/account.
Serialization:

Serialize and deserialize state machines for storage and resumption.
Error Handling:

Handle errors gracefully during execution and database interactions.
Transition History:

Maintain a history of executed transitions with details on handler names, initial and modified states, and arbitrary data.
Arbitrary Data:

Support the storage and propagation of arbitrary data across handlers and transitions.
Readme Documentation:

Provide clear usage examples and instructions in a readme.md file.



### Global and Local Locks in the StateMachine System

#### Overview

The StateMachine system utilizes two types of locking mechanisms, Global Locks and Local Locks, to manage data consistency and prevent concurrent modifications in a distributed environment. These locks are essential for controlling access and operations on state machine instances.

#### Global Locks

Global Locks are used to ensure that when a particular operation is in progress, no other operations (across all types of state machines) should take place using the same global identifier.

- **Database Representation**: In the `GLOBAL_LOCK` table, a global lock is represented by:
  - `LookupKey`: A global identifier used to lock across all state machine types.
  - `UnlockTimestamp`: Indicates when the lock is released. A `NULL` or future timestamp signifies that the lock is currently held.

- **Locked State**: A global lock is considered active (locked) if there is a record with a `NULL` or future `UnlockTimestamp` for a given `LookupKey`.

- **Unlocked State**: The lock is considered inactive (unlocked) if the `UnlockTimestamp` is in the past or if no record exists for the specified `LookupKey`.

- **Use Case Example**: A global lock is ideal in situations where an operation, such as a system-wide maintenance task, must prevent all other operations from occurring until it is complete.

#### Local Locks

Local Locks are designed for scenarios where multiple instances of a state machine can exist simultaneously, but each instance requires independent locking to avoid concurrent modifications.

- **Database Representation**: In the state machine's specific table, a local lock is represented by:
  - `ID`: The unique identifier of the state machine instance.
  - `LookupKey`: A key used to identify the lock within the context of the state machine.
  - `UsesLocalLock`: A boolean flag indicating if the local lock mechanism is active.
  - `UnlockedTimestamp`: Indicates when the lock is released.

- **Locked State**: A local lock is considered active if there is a record with `UsesLocalLock` set to `true` and a `NULL` or future `UnlockedTimestamp` for a given `LookupKey`.

- **Unlocked State**: The lock is inactive if `UsesLocalLock` is `false`, the `UnlockedTimestamp` is in the past, or no record exists for the specified `LookupKey`.

- **Use Case Example**: Local locks are suitable for scenarios where different instances of a state machine manage separate entities (e.g., individual user accounts), and each instance needs to be locked independently to prevent concurrent updates.

#### Relationship Between Global and Local Locks

- Both lock types can be used together to provide a comprehensive locking strategy. For example, a global lock can prevent any state machine operation system-wide, while local locks manage concurrency for individual instances.
- Application logic should be designed to respect both locking mechanisms, ensuring that a global lock is not overridden by a local lock and vice versa.

#### Examples in the Database

- **Global Lock (Locked State)**:
  ```
  | LookupKey | UnlockTimestamp |
  |-----------|-----------------|
  | key123    | NULL            |
  ```

- **Local Lock (Locked State)**:
  ```
  | ID   | LookupKey | UsesLocalLock | UnlockedTimestamp |
  |------|-----------|---------------|-------------------|
  | 6789 | acct456   | true          | NULL              |
  ```

- **No Lock or Unlocked State**: The absence of a record with the relevant `LookupKey` or a record with a past `UnlockTimestamp` indicates no lock or an unlocked state.




State Machine Table (for tracking state machines):

ID: Primary key, unique identifier for each state machine instance.
Name: The name or type of the state machine.
LookupKey: User or account identifier associated with the state machine.
CurrentState: Current state of the state machine.
ResumeFromStep: The step to resume from when recreated.
SaveAfterStep: Flag to indicate whether to save after each step.
KafkaEventTopic: Kafka topic for events (nullable if not used).
SerializedState: JSON serialized state data (to store arbitrary data).
CreatedTimestamp: Timestamp when the state machine was created.
UpdatedTimestamp: Timestamp when the state machine was last updated.
UsesGlobalLock: Flag to indicate if the state machine has a global lock.
UsesLocalLock: Flag to indicate if the state machine has a local lock.
UnlockedTimestamp: When the lock was removed
LastRetryTimestamp: The last time a retry was attempted (does not include the original attempt)


Global Lock Table (for tracking global locks):

ID: Primary key, unique identifier for each global lock instance.
StateMachineType: The state machine name so we know where this lock originated
StateMachineID: The unique ID for the state machine within the type of state machine
LookupKey: The identifier that we're locking across all state machine types
LockTimestamp: Timestamp when the global lock was acquired.
UnlockTimestamp: Timestamp when the glocal lock was removed