package statemachine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// StateMachine represents a state machine instance.
type StateMachine struct {
	Name            string
	ID              string
	HandlerInfoList []HandlerInfo // List of handlers with names
	CurrentState    string
	Direction       string // "forward" or "backward"
	DB              *sql.DB
	KafkaProducer   *kafka.Producer
	mu              sync.Mutex
	paused          bool
	LookupKey       string // User or account ID for locking
	ResumeFromStep  int    // Step to resume from when recreated
	SaveAfterStep   bool   // Flag to save state after each step
	KafkaEventTopic string // Kafka topic to send events
	History         []TransitionHistory
	ExecuteNextStep bool // Flag to control whether to execute the next step immediately
	// Additional field for storing arbitrary data
	CurrentArbitraryData map[string]interface{}
	CreatedTimestamp     time.Time
	UpdatedTimestamp     time.Time
	IsGlobalLock         bool
	IsLocalLock          bool
}

type HandlerInfo struct {
	Handler Handler // Handler instance
	Name    string  // Name of the handler
}

// TransitionHistory stores information about executed transitions.
type TransitionHistory struct {
	FromStep      int                    // Index of the "from" step
	ToStep        int                    // Index of the "to" step
	HandlerName   string                 // Name of the handler
	InitialState  string                 // Initial state
	ModifiedState string                 // Modified state
	ArbitraryData map[string]interface{} // Arbitrary data associated with the transition
}

// Handler defines the interface for state machine handlers.
type Handler interface {
	ExecuteForward(sm *StateMachine, data map[string]interface{}) (map[string]interface{}, error)
	ExecuteBackward(sm *StateMachine, data map[string]interface{}) (map[string]interface{}, error)
	ExecutePause(sm *StateMachine, data map[string]interface{}) (map[string]interface{}, error)
}

func deserializeFromJSON(data []byte) (*StateMachine, error) {
	var sm StateMachine
	if err := json.Unmarshal(data, &sm); err != nil {
		return nil, err
	}
	return &sm, nil
}

func (sm *StateMachine) serializeToJSON() ([]byte, error) {
	serialized, err := json.Marshal(sm)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

// Save the state machine's serialized JSON to the database
func (sm *StateMachine) saveStateToDB() error {
	if err := insertStateMachine(sm.DB, sm); err != nil {
		return err
	}

	return nil
}

// Load the serialized JSON state data from the database
func loadSerializedStateFromDB(stateMachineType string, id string, db *sql.DB) ([]byte, error) {
	// Load serialized state data from the database (replace with your database logic)
	data, err := loadFromDB(stateMachineType, id, db)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Start begins the execution of the state machine.
func (sm *StateMachine) Start() error {

	// Check for locks if necessary
	if sm.shouldCheckLocks() {
		// Lock checking logic here
		if err := sm.checkLocks(); err != nil {
			return err
		}
	}

	sm.paused = false
	sm.CurrentArbitraryData = make(map[string]interface{})

	for i, handlerInfo := range sm.HandlerInfoList {
		handler := handlerInfo.Handler
		handlerName := handlerInfo.Name

		if sm.paused {
			var err error
			sm.CurrentArbitraryData, err = handler.ExecutePause(sm, sm.CurrentArbitraryData)
			if err != nil {
				return err
			}
			continue
		}

		fromStep := sm.CurrentState // Record the "from" step
		toStep := ""                // Initialize the "to" step

		if sm.Direction == "forward" {
			if i >= sm.ResumeFromStep {
				var err error
				sm.CurrentArbitraryData, err = handler.ExecuteForward(sm, sm.CurrentArbitraryData)
				if err != nil {
					return err
				}
			}
		} else if sm.Direction == "backward" {
			// When moving backward, always consider ResumeFromStep
			if i <= sm.ResumeFromStep {
				var err error
				sm.CurrentArbitraryData, err = handler.ExecuteBackward(sm, sm.CurrentArbitraryData)
				if err != nil {
					return err
				}
			}
		}

		toStep = sm.CurrentState // Record the "to" step

		// Save state to MySQL or send Kafka event based on configuration.
		if sm.SaveAfterStep {
			if err := sm.saveStateToDB(); err != nil {
				// Handle state save error.
				return err
			}
			// Send Kafka event if configured
			if sm.KafkaEventTopic != "" {
				if err := sm.sendKafkaEvent(); err != nil {
					// Handle Kafka event sending error.
					return err
				}
			}
		}

		// Update transition history
		sm.History = append(sm.History, TransitionHistory{
			FromStep:      i,
			ToStep:        i + 1,
			HandlerName:   handlerName,
			InitialState:  fromStep,
			ModifiedState: toStep,
			ArbitraryData: sm.CurrentArbitraryData, // Include arbitrary data in transition history
		})

		if !sm.ExecuteNextStep {
			// If ExecuteNextStep is false, stop execution
			return nil
		}
	}
	return nil
}

// shouldCheckLocks checks if the state machine should check locks based on its configuration.
func (sm *StateMachine) shouldCheckLocks() bool {
	return sm.IsGlobalLock || sm.IsLocalLock
}

// checkLocks checks for locks and handles locking logic.
func (sm *StateMachine) checkLocks() error {
	// Begin a new transaction for lock operations.
	tx, err := sm.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // Rollback the transaction if it's not committed.

	// Check if the state machine is configured for a global lock.
	if sm.IsGlobalLock {
		if err := sm.checkAndObtainGlobalLock(tx); err != nil {
			return err
		}
	}

	// Check if the state machine is configured for a local lock.
	if sm.IsLocalLock {
		if err := sm.checkAndObtainLocalLock(tx); err != nil {
			return err
		}
	}

	// Commit the transaction to confirm lock acquisition.
	if err := tx.Commit(); err != nil {
		return err
	}

	// If we reached this point, it means the state machine either doesn't
	// need to check locks or has successfully obtained the required locks.
	return nil
}

// checkAndObtainGlobalLock checks if a global lock exists for the state machine and obtains it if possible.
func (sm *StateMachine) checkAndObtainGlobalLock(tx *sql.Tx) error {
	// Check if a global lock exists for this state machine.
	lockExists, err := checkGlobalLockExists(tx, sm)
	if err != nil {
		return err
	}

	if lockExists {
		// A global lock exists for this state machine.
		// Check if this instance owns the lock.
		if ownedByThisInstance, err := isGlobalLockOwnedByThisInstance(tx, sm); err != nil {
			return err
		} else if ownedByThisInstance {
			// This instance already owns the lock, so proceed.
			return nil
		}
		// Another instance owns the lock; do not proceed.
		return fmt.Errorf("another instance holds the global lock")
	}

	// No global lock exists; attempt to obtain it.
	if err := obtainGlobalLock(tx, sm); err != nil {
		// Failed to obtain the global lock.
		return err
	}

	return nil
}

// checkAndObtainLocalLock checks if a local lock exists for the state machine and obtains it if possible.
func (sm *StateMachine) checkAndObtainLocalLock(tx *sql.Tx) error {
	// Check if a local lock exists for this state machine.
	lockExists, err := checkLocalLockExists(tx, sm)
	if err != nil {
		return err
	}

	if lockExists {
		// A local lock exists for this state machine.
		// Check if this instance owns the lock.
		if ownedByThisInstance, err := isLocalLockOwnedByThisInstance(tx, sm); err != nil {
			return err
		} else if ownedByThisInstance {
			// This instance already owns the lock, so proceed.
			return nil
		}
		// Another instance owns the lock; do not proceed.
		return fmt.Errorf("another instance holds the local lock")
	}

	// No local lock exists; attempt to obtain it.
	if err := obtainLocalLock(tx, sm); err != nil {
		// Failed to obtain the local lock.
		return err
	}

	return nil
}

// sendKafkaEvent sends a Kafka event.
func (sm *StateMachine) sendKafkaEvent() error {
	// Implement logic to send Kafka event.
	return nil
}

// NewStateMachine initializes a new StateMachine instance.
func NewStateMachine(name, id, userID string, db *sql.DB, kafkaProducer *kafka.Producer, kafkaEventTopic string, executeNextStep bool) *StateMachine {
	err := CreateGlobalLockTableIfNotExists(db)
	if err != nil {
		panic(err)
	}

	err = CreateStateMachineTableIfNotExists(db, name)
	if err != nil {
		panic(err)
	}

	sm := &StateMachine{
		Name:             name,
		ID:               id,
		LookupKey:        userID,
		DB:               db,
		KafkaProducer:    kafkaProducer,
		KafkaEventTopic:  kafkaEventTopic,
		ExecuteNextStep:  executeNextStep,
		CreatedTimestamp: time.Now(),
		UpdatedTimestamp: time.Now(),
	}
	sm.Direction = "forward"

	return sm
}

func LoadStateMachine(name, id string, db *sql.DB) (*StateMachine, error) {
	// Load serialized state data from the database
	serializedData, err := loadSerializedStateFromDB(name, id, db)
	if err != nil {
		return nil, err
	}

	// Deserialize the state machine from the loaded data
	resumedSM, err := deserializeFromJSON(serializedData)
	if err != nil {
		return nil, err
	}

	// Initialize the resumed state machine with additional information as needed
	resumedSM.Name = name
	resumedSM.ID = id
	resumedSM.DB = db

	return resumedSM, nil
}

// SetInitialState sets the initial state of the state machine.
func (sm *StateMachine) SetInitialState(state string) {
	sm.CurrentState = state
}

// AddHandler adds a handler to the state machine.
func (sm *StateMachine) AddHandler(handler Handler, name string) {
	handlerInfo := HandlerInfo{
		Handler: handler,
		Name:    name,
	}
	sm.HandlerInfoList = append(sm.HandlerInfoList, handlerInfo)
}

// Rollback rolls back the state machine to the previous state.
func (sm *StateMachine) Rollback() error {
	sm.Direction = "backward"
	return sm.Start()
}

// Pause pauses the execution of the state machine.
func (sm *StateMachine) Pause() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.paused = true
	// Perform actions when the state machine enters a paused state.
}

// Resume resumes the execution of the state machine.
func (sm *StateMachine) Resume() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.paused = false
	// Perform actions when the state machine resumes from a paused state.
}
