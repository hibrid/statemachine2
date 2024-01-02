package statemachine

import (
	"database/sql"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type TestHandler struct {
}

func (handler *TestHandler) Name() string {
	return "TestHandler" // Provide a default name for the handler
}

func (handler *TestHandler) ExecuteForward(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Access and modify arbitrary data in the handler logic
	data["key1"] = "new value1"
	data["key3"] = 456

	// Return the modified data
	return OnSuccess, data, nil
}

func (handler *TestHandler) ExecuteBackward(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnSuccess, data, nil
}

func (handler *TestHandler) ExecutePause(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnFailed, data, nil
}

func (handler *TestHandler) ExecuteResume(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnFailed, data, nil
}

type TestHandler2 struct {
}

func (handler *TestHandler2) Name() string {
	return "TestHandler2" // Provide a default name for the handler
}

func (handler *TestHandler2) ExecuteForward(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Access and modify arbitrary data in the handler logic
	data["key1"] = "new value2"
	data["key3"] = 457

	// Return the modified data
	return OnSuccess, data, nil
}

func (handler *TestHandler2) ExecuteBackward(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnSuccess, data, nil
}

func (handler *TestHandler2) ExecutePause(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnFailed, data, nil
}

func (handler *TestHandler2) ExecuteResume(data map[string]interface{}, transitionHistory []TransitionHistory) (Event, map[string]interface{}, error) {
	// Implement backward action logic here.
	return OnFailed, data, nil
}

func TestEvent_String(t *testing.T) {
	tests := []struct {
		event    Event
		expected string
	}{
		{OnSuccess, "OnSuccess"},
		{OnFailed, "OnFailed"},
		{OnAlreadyCompleted, "OnAlreadyCompleted"},
		{OnPause, "OnPause"},
		{OnResume, "OnResume"},
		{OnRollback, "OnRollback"},
		{OnRetry, "OnRetry"},
		{OnResetTimeout, "OnResetTimeout"},
		{OnUnknownSituation, "OnUnknownSituation"},
		{OnManualOverride, "OnManualOverride"},
		{OnError, "OnError"},
		{OnBeforeEvent, "OnBeforeEvent"},
		{OnAfterEvent, "OnAfterEvent"},
		{OnCompleted, "OnCompleted"},
		{OnRollbackCompleted, "OnRollbackCompleted"},
		{OnAlreadyRollbackCompleted, "OnAlreadyRollbackCompleted"},
		{OnRollbackFailed, "OnRollbackFailed"},
		{OnCancelled, "OnCancelled"},
		{OnParked, "OnParked"},
		{Event(999), "UnknownEvent(999)"},
	}

	for _, test := range tests {
		if result := test.event.String(); result != test.expected {
			t.Errorf("Event.String() for %v = %v, want %v", test.event, result, test.expected)
		}
	}
}

func TestLockType_String(t *testing.T) {
	tests := []struct {
		lockType LockType
		expected string
	}{
		{NoLock, "NoLock"},
		{GlobalLock, "GlobalLock"},
		{LocalLock, "LocalLock"},
		// Test case for the default scenario
		{LockType(999), "UnknownLockType(999)"},
	}

	for _, test := range tests {
		if result := test.lockType.String(); result != test.expected {
			t.Errorf("LockType.String() for %v = %v, want %v", test.lockType, result, test.expected)
		}
	}
}

func TestLoadStateMachineSQL(t *testing.T) {
	tableName := "test_table"
	expectedSQL := "SELECT * FROM test_table WHERE ID = ?;"
	sql := loadStateMachineSQL(tableName)
	if sql != expectedSQL {
		t.Errorf("Expected %s, got %s", expectedSQL, sql)
	}
}

func TestLockStateMachineForTransactionSQL(t *testing.T) {
	tableName := "test_table"
	expectedSQL := "SELECT ID FROM test_table WHERE ID = ? FOR UPDATE;"
	sql := lockStateMachineForTransactionSQL(tableName)
	if sql != expectedSQL {
		t.Errorf("Expected %s, got %s", expectedSQL, sql)
	}
}

func TestLoadAndLockStateMachineSQL(t *testing.T) {
	tableName := "test_table"
	expectedSQL := "UPDATE test_table SET CurrentState = 'in_progress', UpdatedTimestamp = NOW() WHERE ID = ?;"
	sql := loadAndLockStateMachineSQL(tableName, StateInProgress)
	if sql != expectedSQL {
		t.Errorf("Expected %s, got %s", expectedSQL, sql)
	}
}

func TestLoadStateMachine(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	// Set up your expectations
	columns := []string{
		"ID",
		"CurrentState",
		"LookupKey",
		"ResumeFromStep",
		"SaveAfterStep",
		"KafkaEventTopic",
		"SerializedState",
		"CreatedTimestamp",
		"UpdatedTimestamp",
		"UsesGlobalLock",
		"UsesLocalLock",
		"UnlockedTimestamp",
		"LastRetryTimestamp"}
	mock.ExpectQuery("SELECT \\* FROM test_table WHERE ID = \\?").
		WithArgs("test_id").
		WillReturnRows(sqlmock.NewRows(columns).
			AddRow("test_id", "test_state", "test_key", 1, true, "test_topic", "{}", "2021-01-01 00:00:00", "2021-01-01 00:00:00", true, false, nil, nil))

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	sm := &StateMachine{Name: "test_table", UniqueID: "test_id", DB: db}

	loadedSM, err := loadStateMachine(tx, sm)
	if err != nil {
		t.Errorf("loadStateMachine() error = %v, wantErr %v", err, false)
	}
	if loadedSM == nil {
		t.Errorf("loadStateMachine() = %v, want non-nil", loadedSM)
	}

	// Assert that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestLoadAndLockStateMachine(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	sm := &StateMachine{
		Name:         "test_table",
		UniqueID:     "test_id",
		CurrentState: StateOpen, // Example state, adjust as needed
		DB:           db,
		LookupKey:    "test_key",
	}

	// Begin transaction
	mock.ExpectBegin()

	// Lock the state machine
	lockSQL := "SELECT ID FROM test_table WHERE ID = \\? FOR UPDATE;"
	mock.ExpectExec(lockSQL).WithArgs(sm.UniqueID).WillReturnResult(sqlmock.NewResult(0, 0))

	columns := []string{
		"ID",
		"CurrentState",
		"LookupKey",
		"ResumeFromStep",
		"SaveAfterStep",
		"KafkaEventTopic",
		"SerializedState",
		"CreatedTimestamp",
		"UpdatedTimestamp",
		"UsesGlobalLock",
		"UsesLocalLock",
		"UnlockedTimestamp",
		"LastRetryTimestamp"}
	mock.ExpectQuery("SELECT \\* FROM test_table WHERE ID = \\?").
		WithArgs("test_id").
		WillReturnRows(sqlmock.NewRows(columns).
			AddRow("test_id", "test_state", "test_key", 1, true, "test_topic", "{}", "2021-01-01 00:00:00", "2021-01-01 00:00:00", true, false, nil, nil))

	// Update the state machine
	updateSQL := "UPDATE test_table SET CurrentState = 'in_progress', UpdatedTimestamp = NOW\\(\\) WHERE ID = \\?"
	mock.ExpectExec(updateSQL).WithArgs(sm.UniqueID).WillReturnResult(sqlmock.NewResult(1, 1))

	// Commit transaction
	mock.ExpectCommit()

	loadedSM, err := loadAndLockStateMachine(sm)
	if err != nil {
		t.Errorf("loadAndLockStateMachine() error = %v, wantErr %v", err, false)
	}
	if loadedSM == nil {
		t.Errorf("loadAndLockStateMachine() = %v, want non-nil", loadedSM)
	}

	// Assert that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestStateMachine_determineNewState(t *testing.T) {
	tests := []struct {
		name          string
		currentState  State
		event         Event
		expectedState State
		expectedRetry bool
	}{
		{"OnCompleted", StateOpen, OnCompleted, StateCompleted, false},
		{"OnRollbackCompleted", StateRollback, OnRollbackCompleted, StateRollbackCompleted, false},
		{"OnFailed", StateOpen, OnFailed, StateFailed, false},
		{"OnSuccess", StateOpen, OnSuccess, StateOpen, false},
		{"OnResetTimeout", StateOpen, OnResetTimeout, StateRollback, false},
		{"OnPause", StateOpen, OnPause, StatePaused, false},
		{"OnAlreadyCompleted", StateCompleted, OnAlreadyCompleted, StateCompleted, false},
		{"OnRollback", StateOpen, OnRollback, StateStartRollback, false},
		{"OnRollbackAlreadyInRollback", StateRollback, OnRollback, StateRollback, false},
		{"OnResume", StatePaused, OnResume, StateOpen, false},
		{"OnRetry", StateRetry, OnRetry, StateRetry, true},
		{"OnUnknownSituation", StateOpen, OnUnknownSituation, StateParked, false},
		{"DefaultCase", StateOpen, Event(999), StateParked, false}, // Testing default case
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &StateMachine{CurrentState: tt.currentState}
			newState, shouldRetry, err := sm.determineNewState(&Context{}, tt.event)
			if err != nil {
				t.Errorf("determineNewState() error = %v, wantErr %v", err, false)
			}
			if newState != tt.expectedState {
				t.Errorf("determineNewState() newState = %v, want %v", newState, tt.expectedState)
			}
			if shouldRetry != tt.expectedRetry {
				t.Errorf("determineNewState() shouldRetry = %v, want %v", shouldRetry, tt.expectedRetry)
			}
		})
	}
}

func TestLoadStateMachineFromDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	stateMachineType := "test_table"
	id := "test_id"

	// Set up mock expectations for loadAndLockStateMachine
	// Begin transaction
	mock.ExpectBegin()

	// Lock the state machine
	lockSQL := "SELECT ID FROM .* WHERE ID = \\? FOR UPDATE;"
	mock.ExpectExec(lockSQL).WithArgs(id).WillReturnResult(sqlmock.NewResult(0, 0))

	columns := []string{
		"ID",
		"CurrentState",
		"LookupKey",
		"ResumeFromStep",
		"SaveAfterStep",
		"KafkaEventTopic",
		"SerializedState",
		"CreatedTimestamp",
		"UpdatedTimestamp",
		"UsesGlobalLock",
		"UsesLocalLock",
		"UnlockedTimestamp",
		"LastRetryTimestamp"}
	mock.ExpectQuery("SELECT \\* FROM test_table WHERE ID = \\?").
		WithArgs("test_id").
		WillReturnRows(sqlmock.NewRows(columns).
			AddRow("test_id", "test_state", "test_key", 1, true, "test_topic", "{}", "2021-01-01 00:00:00", "2021-01-01 00:00:00", true, false, nil, nil))

	// Update the state machine
	updateSQL := "UPDATE .* SET CurrentState = 'in_progress', UpdatedTimestamp = NOW\\(\\) WHERE ID = \\?"
	mock.ExpectExec(updateSQL).WithArgs(id).WillReturnResult(sqlmock.NewResult(1, 1))

	// Commit transaction
	mock.ExpectCommit()

	// Call the function
	loadedSM, err := loadStateMachineFromDB(stateMachineType, id, db)
	if err != nil {
		t.Errorf("loadStateMachineFromDB() error = %v, wantErr %v", err, false)
	}
	if loadedSM == nil {
		t.Errorf("loadStateMachineFromDB() = %v, want non-nil", loadedSM)
	}

	// Assert that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestStateMachine_executeLeaveStateCallback(t *testing.T) {
	sm := &StateMachine{
		CurrentState: StateOpen,
		Callbacks: map[string]StateCallbacks{
			"open": {
				LeaveState: func(sm *StateMachine, ctx *Context) error {
					// Mock callback logic
					return nil
				},
			},
		},
	}
	context := &Context{} // Mock context as needed

	err := sm.executeLeaveStateCallback(context)
	if err != nil {
		t.Errorf("executeLeaveStateCallback() error = %v, wantErr %v", err, false)
	}

}

func TestStateMachine_CalculateNextRetryDelay(t *testing.T) {
	tests := []struct {
		name          string
		retryCount    int
		baseDelay     time.Duration
		maxTimeout    time.Duration
		expectedDelay time.Duration
	}{
		{
			name:          "First retry",
			retryCount:    0,
			baseDelay:     1 * time.Second,
			maxTimeout:    60 * time.Second,
			expectedDelay: 1 * time.Second,
		},
		{
			name:          "Second retry",
			retryCount:    1,
			baseDelay:     1 * time.Second,
			maxTimeout:    60 * time.Second,
			expectedDelay: 2 * time.Second,
		},
		{
			name:          "Exceeds max timeout",
			retryCount:    10,
			baseDelay:     1 * time.Second,
			maxTimeout:    10 * time.Second,
			expectedDelay: 10 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &StateMachine{
				RetryCount: tt.retryCount,
				BaseDelay:  tt.baseDelay,
				MaxTimeout: tt.maxTimeout,
			}

			got := sm.CalculateNextRetryDelay()
			if got != tt.expectedDelay {
				t.Errorf("CalculateNextRetryDelay() = %v, want %v", got, tt.expectedDelay)
			}
		})
	}
}

func TestStateMachine_GetRemainingDelay(t *testing.T) {
	now := time.Now()
	twentySecondsAgo := now.Add(-20 * time.Second)
	tests := []struct {
		name          string
		lastRetry     *time.Time
		retryCount    int
		baseDelay     time.Duration
		expectedDelay time.Duration
		maxDelay      time.Duration
	}{
		{
			name:          "LastRetry is nil",
			lastRetry:     nil,
			expectedDelay: 0,
			maxDelay:      60 * time.Second,
		},
		{
			name:          "Next retry time in the future",
			lastRetry:     &now,
			retryCount:    1,
			baseDelay:     10 * time.Second,
			expectedDelay: 20 * time.Second,
			maxDelay:      60 * time.Second,
		},
		{
			name:          "Next retry time in the past",
			lastRetry:     &twentySecondsAgo,
			retryCount:    1,
			baseDelay:     10 * time.Second,
			expectedDelay: 0,
			maxDelay:      60 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := &StateMachine{
				LastRetry:  tt.lastRetry,
				BaseDelay:  tt.baseDelay,
				RetryCount: tt.retryCount,
				MaxTimeout: tt.maxDelay,
			}

			got := sm.GetRemainingDelay()
			// Allow a small margin of error for time calculations
			if got < tt.expectedDelay-time.Second || got > tt.expectedDelay+time.Second {
				t.Errorf("GetRemainingDelay() = %v, want %v", got, tt.expectedDelay)
			}
		})
	}
}

func TestIsTerminalState(t *testing.T) {
	tests := []struct {
		state    State
		expected bool
	}{
		{StateCompleted, true},
		{StateFailed, true},
		{StateOpen, false},
		// Add more test cases for different states
	}

	for _, test := range tests {
		result := IsTerminalState(test.state)
		if result != test.expected {
			t.Errorf("IsTerminalState(%s) = %v; want %v", test.state, result, test.expected)
		}
	}
}

func TestIsValidTransition(t *testing.T) {
	tests := []struct {
		currentState State
		event        Event
		newState     State
		expected     bool
	}{
		{StateOpen, OnSuccess, StateCompleted, true},
		{StateOpen, OnFailed, StateFailed, true},
		{StateOpen, OnPause, StatePaused, true},
		// TODO: Add more test cases for different combinations
	}

	for _, test := range tests {
		result := IsValidTransition(test.currentState, test.event, test.newState)
		if result != test.expected {
			t.Errorf("IsValidTransition(%s, %s, %s) = %v; want %v", test.currentState, test.event, test.newState, result, test.expected)
		}
	}
}

func TestStateMachine_Run_Integration(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	// Initialize StateMachine with necessary configuration
	config := StateMachineConfig{
		Name:                 "testing",
		UniqueStateMachineID: "test1",
		LookupKey:            "5",
		DB:                   db,
		Handlers:             []Handler{&TestHandler{}, &TestHandler2{}},
		ExecuteSynchronously: true,
		RetryPolicy: RetryPolicy{
			MaxTimeout: 10 * time.Second,
			BaseDelay:  1 * time.Second,
			RetryType:  ExponentialBackoff,
		},
		SaveAfterEachStep: true,
		LockType:          LocalLock,
	}

	// Set up expectations for CreateGlobalLockTableIfNotExists
	createTableSQL := escapeRegexChars(`CREATE TABLE IF NOT EXISTS GLOBAL_LOCK (
        ID INT NOT NULL AUTO_INCREMENT,
        StateMachineType VARCHAR(255),
        StateMachineID VARCHAR(255),
        LookupKey VARCHAR(255),
        LockTimestamp TIMESTAMP,
        UnlockTimestamp TIMESTAMP NULL,
        PRIMARY KEY (ID),
        INDEX (StateMachineType),
        INDEX (LookupKey),
        INDEX (UnlockTimestamp)
    );`)
	mock.ExpectExec(createTableSQL).WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectExec(escapeRegexChars(createStateMachineTableIfNotExistsSQL(config.Name))).WillReturnResult(sqlmock.NewResult(0, 0))

	var usesGlobalLock, usesLocalLock bool
	if config.LockType == GlobalLock {
		usesGlobalLock = true
	}
	if config.LockType == LocalLock {
		usesLocalLock = true
	}

	mock.ExpectExec(escapeRegexChars(insertStateMachineSQL(config.Name))).WithArgs(
		config.UniqueStateMachineID,
		StatePending,
		config.LookupKey,
		0,
		config.SaveAfterEachStep,
		config.KafkaEventTopic,
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		sqlmock.AnyArg(),
		nil,
		nil,
		usesGlobalLock,
		usesLocalLock).WillReturnResult(sqlmock.NewResult(1, 1))

	// Obtain the local lock with a global lock
	mock.ExpectBegin()
	mock.ExpectQuery(escapeRegexChars(checkGlobalLockExistsSQL())).WithArgs(config.LookupKey).WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(1))
	mock.ExpectQuery(escapeRegexChars(isGlobalLockOwnedByThisInstanceSQL())).WithArgs(config.Name, config.UniqueStateMachineID, config.LookupKey).WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(1))
	mock.ExpectQuery(escapeRegexChars(checkLocalLockExistsSQL(config.Name))).WithArgs(config.LookupKey).WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(1))
	mock.ExpectQuery(escapeRegexChars(isLocalLockOwnedByThisInstanceSQL(config.Name))).WithArgs(config.UniqueStateMachineID, config.LookupKey).WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(1))

	mock.ExpectRollback()

	// Update after Handler 1
	mock.ExpectBegin()
	mock.ExpectExec(escapeRegexChars(updateStateMachineStateSQL(config.Name))).WithArgs(StateOpen, sqlmock.AnyArg(), 1, sqlmock.AnyArg(), config.UniqueStateMachineID).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Obtain the local lock without a global lock
	mock.ExpectBegin()
	mock.ExpectQuery(escapeRegexChars(checkGlobalLockExistsSQL())).WithArgs(config.LookupKey).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(escapeRegexChars(checkLocalLockExistsSQL(config.Name))).WithArgs(config.LookupKey).WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(1))
	mock.ExpectQuery(escapeRegexChars(isLocalLockOwnedByThisInstanceSQL(config.Name))).WithArgs(config.UniqueStateMachineID, config.LookupKey).WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(1))
	mock.ExpectRollback()

	// Update after Handler 2
	mock.ExpectBegin()
	mock.ExpectExec(escapeRegexChars(updateStateMachineStateSQL(config.Name))).WithArgs(StateOpen, sqlmock.AnyArg(), 2, sqlmock.AnyArg(), config.UniqueStateMachineID).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Obtain the local lock without a global lock
	mock.ExpectBegin()
	mock.ExpectQuery(escapeRegexChars(checkGlobalLockExistsSQL())).WithArgs(config.LookupKey).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(escapeRegexChars(checkLocalLockExistsSQL(config.Name))).WithArgs(config.LookupKey).WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(1))
	mock.ExpectQuery(escapeRegexChars(isLocalLockOwnedByThisInstanceSQL(config.Name))).WithArgs(config.UniqueStateMachineID, config.LookupKey).WillReturnRows(sqlmock.NewRows([]string{"ID"}).AddRow(1))

	mock.ExpectRollback()

	// Update to record the completion of the state machine
	mock.ExpectBegin()
	mock.ExpectExec(escapeRegexChars(updateStateMachineStateSQL(config.Name))).WithArgs(StateCompleted, sqlmock.AnyArg(), 2, sqlmock.AnyArg(), config.UniqueStateMachineID).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	sm, err := NewStateMachine(config)
	if err != nil {
		t.Fatalf("error creating StateMachine: %v", err)
	}

	// Execute the Run method
	err = sm.Run()
	if err != nil {
		t.Errorf("Run() resulted in an error: %v", err)
	}

	// Assert that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}

	if sm.CurrentState != StateCompleted {
		t.Errorf("Run() did not result in a completed state machine")
	}

	if sm.LastRetry != nil {
		t.Errorf("Run() should not have set LastRetry")
	}

	if sm.ResumeFromStep != 2 {
		t.Errorf("Run() should have set ResumeFromStep to 2")
	}

}

func TestCreateGlobalLockTableIfNotExists(t *testing.T) {
	// Create a new mock database connection
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	// Define the expected SQL statement
	createTableSQL := `
    CREATE TABLE IF NOT EXISTS GLOBAL_LOCK \(
        ID INT NOT NULL AUTO_INCREMENT,
        StateMachineType VARCHAR\(255\),
        StateMachineID VARCHAR\(255\),
        LookupKey VARCHAR\(255\),
        LockTimestamp TIMESTAMP,
        UnlockTimestamp TIMESTAMP NULL,
        PRIMARY KEY \(ID\),
        INDEX \(StateMachineType\),
        INDEX \(LookupKey\),
        INDEX \(UnlockTimestamp\)
    \);`

	// Set up the expectation for the SQL execution
	mock.ExpectExec(createTableSQL).WillReturnResult(sqlmock.NewResult(0, 0))

	// Call the function
	err = CreateGlobalLockTableIfNotExists(db)
	if err != nil {
		t.Errorf("CreateGlobalLockTableIfNotExists() returned an error: %v", err)
	}

	// Assert that the expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestDeserializeFromJSON(t *testing.T) {
	validJSON := []byte(`{"key1": "value1", "key2": 2}`)
	invalidJSON := []byte(`{"key1": "value1", "key2": 2`)

	// Test with valid JSON
	result, err := deserializeFromJSON(validJSON)
	if err != nil {
		t.Errorf("deserializeFromJSON() with valid JSON returned an error: %v", err)
	}
	if result["key1"] != "value1" || result["key2"] != float64(2) {
		t.Errorf("deserializeFromJSON() with valid JSON returned incorrect data: %v", result)
	}

	// Test with invalid JSON
	_, err = deserializeFromJSON(invalidJSON)
	if err == nil {
		t.Errorf("deserializeFromJSON() with invalid JSON should return an error")
	}
}

func TestUpdateStateMachineState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	sm := &StateMachine{
		DB:           db,
		CurrentState: StateOpen,
		// Initialize other necessary fields, e.g., Name, UniqueID, ResumeFromStep
	}
	sm.SetUniqueID("test_id")

	// Set up the expected SQL query
	tableName := normalizeTableName(sm.Name)
	updateSQL := fmt.Sprintf("UPDATE %s SET CurrentState = \\?, SerializedState = \\?, UpdatedTimestamp = NOW\\(\\), ResumeFromStep = \\?, UnlockedTimestamp = \\? WHERE ID = \\?;", tableName)

	// Define the expected behavior of the mock
	mock.ExpectBegin()
	mock.ExpectExec(updateSQL).WithArgs(sm.CurrentState, sqlmock.AnyArg(), sm.ResumeFromStep, sqlmock.AnyArg(), "test_id").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Call the function
	err = updateStateMachineState(sm, sm.CurrentState)
	if err != nil {
		t.Errorf("updateStateMachineState() returned an error: %v", err)
	}

	// Assert that the expected interactions with the mock occurred
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

/*
func TestStateMachine_CalculateNextRetryDelay(t *testing.T) {
	sm := &StateMachine{
		RetryCount: 2,
		BaseDelay:  1 * time.Second,
		MaxTimeout: 60 * time.Second,
	}

	expectedDelay := 4 * time.Second // 2^2 * 1 second
	actualDelay := sm.CalculateNextRetryDelay()
	if actualDelay != expectedDelay {
		t.Errorf("CalculateNextRetryDelay() = %v; want %v", actualDelay, expectedDelay)
	}
}
*/

func escapeRegexChars(input string) string {
	return regexp.QuoteMeta(input)
}
