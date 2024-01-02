package statemachine

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"
)

func normalizeTableName(stateMachineName string) string {
	// Remove any non-alphanumeric characters and convert to lowercase
	return strings.ToLower(
		// Replace non-alphanumeric characters with underscores
		regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(stateMachineName, "_"),
	)
}

func createGlobalLockTableIfNotExistsSQL() string {
	return `
	CREATE TABLE IF NOT EXISTS GLOBAL_LOCK (
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
	);`
}

func CreateGlobalLockTableIfNotExists(db *sql.DB) error {

	// Execute the SQL statement to create the table
	_, err := db.Exec(createGlobalLockTableIfNotExistsSQL())
	if err != nil {
		return err
	}

	return nil
}

// checkLockExists checks if a lock exists based on provided query and arguments.
func checkLockExists(tx *sql.Tx, query string, args ...interface{}) (bool, error) {
	var lockID int
	err := tx.QueryRow(query, args...).Scan(&lockID)
	if err == nil {
		// A lock exists based on the provided query and arguments.
		return true, nil
	} else if err == sql.ErrNoRows {
		// No lock exists based on the provided query and arguments.
		return false, nil
	}
	// Handle other potential errors.
	return false, err
}

func isGlobalLockOwnedByThisInstanceSQL() string {
	return "SELECT StateMachineID FROM GLOBAL_LOCK WHERE StateMachineType = ? AND StateMachineID = ? AND LookupKey = ? AND (UnlockTimestamp IS NULL OR UnlockTimestamp > NOW()) FOR UPDATE;"
}

func isGlobalLockOwnedByThisInstance(tx *sql.Tx, sm *StateMachine) (bool, error) {
	return checkLockExists(tx, isGlobalLockOwnedByThisInstanceSQL(), sm.Name, sm.UniqueID, sm.LookupKey)
}

func checkGlobalLockExistsSQL() string {
	return "SELECT StateMachineID FROM GLOBAL_LOCK WHERE LookupKey = ? AND (UnlockTimestamp IS NULL OR UnlockTimestamp > NOW()) FOR UPDATE;"
}

// checkGlobalLockExists checks if a global lock exists for the given state machine and custom lookup key.
// by definition, a global lock is for the lookup key (eg, user ID) regardless of the state machine type
func checkGlobalLockExists(tx *sql.Tx, sm *StateMachine) (bool, error) {
	return checkLockExists(tx, checkGlobalLockExistsSQL(), sm.LookupKey)
}

func obtainGlobalLockSQL() string {
	return "INSERT INTO GLOBAL_LOCK (StateMachineType, StateMachineID, LookupKey, LockTimestamp) VALUES (?, ?, ?, NOW());"
}

// obtainGlobalLock attempts to obtain a global lock for a specific type of state machine instance with a custom lookup key.
func obtainGlobalLock(tx *sql.Tx, sm *StateMachine) error {

	// Insert a new global lock record into the GLOBAL_LOCK table with the type, instance, and custom lookup key.
	_, err := tx.Exec(obtainGlobalLockSQL(), sm.Name, sm.UniqueID, sm.LookupKey)
	if err != nil {
		return err
	}

	// Lock obtained successfully.
	return nil
}

func createStateMachineTableIfNotExistsSQL(stateMachineName string) string {
	return fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		ID VARCHAR(255) PRIMARY KEY,
		CurrentState VARCHAR(255),
		LookupKey VARCHAR(255),
		ResumeFromStep INT,
		SaveAfterStep BOOLEAN,
		KafkaEventTopic VARCHAR(255),
		SerializedState JSON,
		CreatedTimestamp TIMESTAMP,
		UpdatedTimestamp TIMESTAMP,
		UsesGlobalLock BOOLEAN,
		UsesLocalLock BOOLEAN,
		UnlockedTimestamp TIMESTAMP NULL,  
		LastRetryTimestamp TIMESTAMP NULL,  
		INDEX (LookupKey)
	);`, normalizeTableName(stateMachineName))
}

func CreateStateMachineTableIfNotExists(db *sql.DB, stateMachineName string) error {
	// Execute the SQL statement to create the table
	_, err := db.Exec(createStateMachineTableIfNotExistsSQL(stateMachineName))
	return err
}

func checkLockStatusSQL(tableName, condition string) string {
	return fmt.Sprintf("SELECT ID FROM %s WHERE %s AND (UnlockTimestamp IS NULL OR UnlockTimestamp > NOW()) FOR UPDATE;", normalizeTableName(tableName), condition)
}

// checkLockStatus checks if a lock exists based on provided conditions.
func checkLockStatus(tx *sql.Tx, query string, args ...interface{}) (bool, error) {
	var lockID int
	err := tx.QueryRow(query, args...).Scan(&lockID)
	if err == nil {
		// A lock exists based on the provided conditions.
		return true, nil
	} else if err == sql.ErrNoRows {
		// No lock exists based on the provided conditions.
		return false, nil
	}
	// Handle other potential errors.
	return false, err
}

func checkLocalLockExistsSQL(tableName string) string {
	return checkLockStatusSQL(tableName, "LookupKey = ?")
}
func checkLocalLockExists(tx *sql.Tx, sm *StateMachine) (bool, error) {
	return checkLockStatus(tx, checkLocalLockExistsSQL(sm.Name), sm.LookupKey)
}

func isLocalLockOwnedByThisInstanceSQL(tableName string) string {
	return checkLockStatusSQL(tableName, "ID = ? AND LookupKey = ?")
}
func isLocalLockOwnedByThisInstance(tx *sql.Tx, sm *StateMachine) (bool, error) {
	return checkLockStatus(tx, isLocalLockOwnedByThisInstanceSQL(sm.Name), sm.UniqueID, sm.LookupKey)
}

func obtainLocalLockSQL(tableName string) string {
	return fmt.Sprintf(`
	INSERT INTO %s (ID, CurrentState, LookupKey, ResumeFromStep, SaveAfterStep, KafkaEventTopic, SerializedState, CreatedTimestamp, UpdatedTimestamp, UnlockedTimestamp, LastRetryTimestamp, UsesGlobalLock, UsesLocalLock)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
	UsesLocalLock = VALUES(UsesLocalLock),
	SerializedState = VALUES(SerializedState),
	UpdatedTimestamp = VALUES(UpdatedTimestamp),
	UnlockedTimestamp = VALUES(UnlockedTimestamp),
	LastRetryTimestamp = VALUES(LastRetryTimestamp);`, normalizeTableName(tableName))
}

// obtainLocalLock attempts to obtain a local lock for a specific type of state machine instance with a custom lookup key
// it will update an existing statemachine if it exists to use the local lock
func obtainLocalLock(tx *sql.Tx, sm *StateMachine) error {

	// Serialize the StateMachine to JSON
	serializedState, err := sm.serializeToJSON()
	if err != nil {
		return err
	}

	var usesGlobalLock, usesLocalLock bool
	if sm.LockType == GlobalLock {
		usesGlobalLock = true
	} else if sm.LockType == LocalLock {
		usesLocalLock = true
	} else {
		return fmt.Errorf("state machine is not configured to use local lock")
	}

	// Convert zero time.Time to nil for SQL insertion
	var unlockedTimestamp interface{}
	if sm.UnlockedTimestamp == nil {
		unlockedTimestamp = nil
	} else {
		unlockedTimestamp = sm.UnlockedTimestamp.UTC()
	}

	var lastRetry interface{}
	if sm.LastRetry == nil {
		lastRetry = nil
	} else {
		lastRetry = sm.LastRetry.UTC()
	}

	// Insert or update the local lock record
	_, err = tx.Exec(obtainLocalLockSQL(sm.Name),
		sm.UniqueID, sm.CurrentState, sm.LookupKey, sm.ResumeFromStep, sm.SaveAfterEachStep, sm.KafkaEventTopic, string(serializedState), sm.CreatedTimestamp.UTC(), sm.UpdatedTimestamp.UTC(), unlockedTimestamp, lastRetry, usesGlobalLock, usesLocalLock)

	if err != nil {
		return err
	}

	// Lock obtained successfully.
	return nil
}

func insertStateMachineSQL(tableName string) string {
	return fmt.Sprintf(`
	INSERT INTO %s (ID, CurrentState, LookupKey, ResumeFromStep, SaveAfterStep, KafkaEventTopic, SerializedState, CreatedTimestamp, UpdatedTimestamp, UnlockedTimestamp, LastRetryTimestamp, UsesGlobalLock, UsesLocalLock)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`, tableName)
}
func insertStateMachine(sm *StateMachine) error {
	tableName := normalizeTableName(sm.Name)

	serializedState, err := sm.serializeToJSON()
	if err != nil {
		return err
	}

	var usesGlobalLock, usesLocalLock bool
	if sm.LockType == GlobalLock {
		usesGlobalLock = true
	}
	if sm.LockType == LocalLock {
		usesLocalLock = true
	}

	// Convert zero time.Time to nil for SQL insertion
	var unlockedTimestamp interface{}
	if sm.UnlockedTimestamp == nil {
		unlockedTimestamp = nil
	} else {
		unlockedTimestamp = sm.UnlockedTimestamp.UTC()
	}

	var lastRetry interface{}
	if sm.LastRetry == nil {
		lastRetry = nil
	} else {
		lastRetry = sm.LastRetry.UTC()
	}

	// Execute the SQL statement within the transaction
	_, err = sm.DB.Exec(insertStateMachineSQL(tableName), sm.UniqueID, sm.CurrentState, sm.LookupKey, sm.ResumeFromStep, sm.SaveAfterEachStep, sm.KafkaEventTopic, string(serializedState), sm.CreatedTimestamp.UTC(), sm.UpdatedTimestamp.UTC(), unlockedTimestamp, lastRetry, usesGlobalLock, usesLocalLock)

	return err
}

func queryStateMachinesByType(db *sql.DB, stateMachineName string) ([]StateMachine, error) {
	tableName := stateMachineName
	querySQL := fmt.Sprintf("SELECT * FROM %s;", tableName)

	// Execute the SQL query
	rows, err := db.Query(querySQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Parse the query results into StateMachine structs
	var stateMachines []StateMachine
	var serializedState string
	for rows.Next() {
		var sm StateMachine
		var usesGlobalLock, usesLocalLock sql.NullBool

		// Scan row data into sm fields
		err := rows.Scan(&sm.UniqueID, &sm.CurrentState, &sm.ResumeFromStep, &sm.SaveAfterEachStep, &sm.KafkaEventTopic, serializedState, &sm.CreatedTimestamp, &sm.UpdatedTimestamp, &usesGlobalLock, &usesLocalLock)
		if err != nil {
			return nil, err
		}
		if usesGlobalLock.Valid && usesGlobalLock.Bool {
			sm.LockType = GlobalLock
		}
		if usesLocalLock.Valid && usesLocalLock.Bool {
			sm.LockType = LocalLock
		}
		stateMachines = append(stateMachines, sm)
	}

	return stateMachines, nil
}

func parseTimestamp(timestamp string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05", timestamp)
}

func loadStateMachineWithNoLock(sm *StateMachine) (*StateMachine, error) {
	tableName := normalizeTableName(sm.Name)
	querySQL := fmt.Sprintf("SELECT * FROM %s WHERE ID = ?;", tableName)

	// Execute the SQL query
	rows, err := sm.DB.Query(querySQL, sm.UniqueID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Parse the query results into StateMachine structs
	var serializedState string
	var createdTimestampStr, updatedTimestampStr, unlockedTimestampStr sql.NullString
	for rows.Next() {
		var usesGlobalLock, usesLocalLock sql.NullBool
		// Scan row data into sm fields
		err := rows.Scan(&sm.UniqueID, &sm.CurrentState, &sm.ResumeFromStep, &sm.SaveAfterEachStep, &sm.KafkaEventTopic, serializedState, &sm.CreatedTimestamp, &sm.UpdatedTimestamp, &usesGlobalLock, &usesLocalLock)
		if err != nil {
			return nil, err
		}

		if usesGlobalLock.Valid && usesGlobalLock.Bool {
			sm.LockType = GlobalLock
		}
		if usesLocalLock.Valid && usesLocalLock.Bool {
			sm.LockType = LocalLock
		}

		if createdTimestampStr.Valid {
			sm.CreatedTimestamp, err = parseTimestamp(createdTimestampStr.String)
			if err != nil {
				return nil, err
			}
		}

		if updatedTimestampStr.Valid {
			sm.UpdatedTimestamp, err = parseTimestamp(updatedTimestampStr.String)
			if err != nil {
				return nil, err
			}
		}

		if unlockedTimestampStr.Valid {
			unlockedTimestamp, err := parseTimestamp(unlockedTimestampStr.String)
			if err != nil {
				return nil, err
			}
			sm.UnlockedTimestamp = &unlockedTimestamp
		}
	}

	return sm, nil
}

func loadStateMachineSQL(tableName string) string {
	return fmt.Sprintf("SELECT * FROM %s WHERE ID = ?;", normalizeTableName(tableName))
}

func loadStateMachine(tx *sql.Tx, sm *StateMachine) (*StateMachine, error) {

	var loadedSM StateMachine
	var serializedState []byte
	var createdTimestampStr, updatedTimestampStr, unlockedTimestampStr, lastRetryTimestamp sql.NullString
	var usesGlobalLock, usesLocalLock sql.NullBool

	err := tx.QueryRow(loadStateMachineSQL(sm.Name), sm.UniqueID).Scan(
		&loadedSM.UniqueID,
		&loadedSM.CurrentState,
		&loadedSM.LookupKey,
		&loadedSM.ResumeFromStep,
		&loadedSM.SaveAfterEachStep,
		&loadedSM.KafkaEventTopic,
		&serializedState,
		&createdTimestampStr,
		&updatedTimestampStr,
		&usesGlobalLock,
		&usesLocalLock,
		&unlockedTimestampStr,
		&lastRetryTimestamp)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("state machine not found")
		}
		return nil, err
	}

	if usesGlobalLock.Valid && usesGlobalLock.Bool {
		loadedSM.LockType = GlobalLock
	}
	if usesLocalLock.Valid && usesLocalLock.Bool {
		loadedSM.LockType = LocalLock
	}

	if createdTimestampStr.Valid {
		loadedSM.CreatedTimestamp, err = parseTimestamp(createdTimestampStr.String)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	if updatedTimestampStr.Valid {
		loadedSM.UpdatedTimestamp, err = parseTimestamp(updatedTimestampStr.String)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	if unlockedTimestampStr.Valid {
		unlockedTimestamp, err := parseTimestamp(unlockedTimestampStr.String)
		if err != nil {
			return nil, err
		}
		loadedSM.UnlockedTimestamp = &unlockedTimestamp
	}

	loadedSM.SerializedState = serializedState
	return &loadedSM, nil
}

func lockStateMachineForTransactionSQL(tableName string) string {
	return fmt.Sprintf("SELECT ID FROM %s WHERE ID = ? FOR UPDATE;", normalizeTableName(tableName))
}

func lockStateMachineForTransaction(tx *sql.Tx, sm *StateMachine) error {
	_, err := tx.Exec(lockStateMachineForTransactionSQL(sm.Name), sm.UniqueID)
	return err
}

func loadAndLockStateMachineSQL(tableName string, stateInProgress State) string {
	return fmt.Sprintf("UPDATE %s SET CurrentState = '%s', UpdatedTimestamp = NOW() WHERE ID = ?;", tableName, StateInProgress)
}

func loadAndLockStateMachine(sm *StateMachine) (*StateMachine, error) {
	tableName := normalizeTableName(sm.Name)
	tx, err := sm.DB.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Lock the state machine
	err = lockStateMachineForTransaction(tx, sm)
	if err != nil {
		return nil, err
	}

	// Load the state machine
	loadedSM, err := loadStateMachine(tx, sm)
	if err != nil {
		return nil, err
	}

	if IsTerminalState(sm.CurrentState) {
		// Row exists, but the state machine is in a terminal state
		return loadedSM, fmt.Errorf("state machine is in a terminal state")
	}

	// Set state to in_progress and update the database
	_, err = tx.Exec(loadAndLockStateMachineSQL(tableName, StateInProgress), sm.UniqueID)
	if err != nil {
		return nil, err
	}

	tx.Commit()
	return loadedSM, nil
}

// TODO: use the new unique id generator and test the local and global lock logic
// Test: create a state machine with a local lock
// Test: Create a state machine with a global lock
// Test: Create a state machine with a local lock and then update it to use a global lock
// Test: Create a state machine with a global lock and then update it to use a local lock
// Test: Create a state machine without a local lock and then update it to use a local lock
// Test: Create a state machine without a global lock and then update it to use a global lock
// Test: Forward execution to completion
// Test: Forward execution to a terminal state
// Test: Forward execution to a terminal state and then restart execution
// Test: Forward execution to a terminal state and then restart execution from a previous state
// Test: Forward execution cases without a lock
// Test: Forward execution cases with a local lock
// Test: Forward execution cases with a global lock
// Test: Entering a terminal state with a local lock
// Test: Entering a terminal state with a global lock
// Test: Entering a terminal state with no lock
// Test: Entering a terminal state with a local lock and then restarting execution
// Test: Entering a terminal state with a global lock and then restarting execution
// Test: Entering a terminal state with no lock and then restarting execution
// Test: Entering a terminal state with a local lock and then restarting execution from a previous state
// Test: Entering a terminal state with a global lock and then restarting execution from a previous state
// Test: Pausing execution with a local lock
// Test: Pausing execution with a global lock
// Test: Pausing execution with no lock
// Test: Pausing execution with a local lock and then restarting execution
// Test: Pausing execution with a global lock and then restarting execution
// Test: Pausing execution with no lock and then restarting execution
// Test: Pausing execution with a local lock and then restarting execution from a previous state
// TODO: update for completed state for lock
// TODO: remove lock from state machine
// TODO: remove lock from global lock table
// TODO: Update the lock logic to make sure it's not considering machine's that are in a terminal state or don't have a lock

func updateStateMachineStateSQL(tableName string) string {
	return fmt.Sprintf("UPDATE %s SET CurrentState = ?, SerializedState = ?, UpdatedTimestamp = NOW(), ResumeFromStep = ? WHERE ID = ?;", tableName)
}

func updateStateMachineState(sm *StateMachine, newState State) error {
	tableName := normalizeTableName(sm.Name)

	tx, err := sm.DB.Begin()
	if err != nil {
		return err
	}

	newSerializedState, err := sm.serializeToJSON()
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(updateStateMachineStateSQL(tableName), newState, newSerializedState, sm.ResumeFromStep, sm.UniqueID)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
