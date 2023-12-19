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

func CreateGlobalLockTableIfNotExists(db *sql.DB) error {
	// Define the SQL statement to create the global lock table
	createTableSQL := `
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

	// Execute the SQL statement to create the table
	_, err := db.Exec(createTableSQL)
	if err != nil {
		return err
	}

	return nil
}

func isGlobalLockOwnedByThisInstance(tx *sql.Tx, sm *StateMachine) (bool, error) {
	// Query the GLOBAL_LOCK table to check if the global lock is owned by this instance.
	var ownerStateMachineID string
	err := tx.QueryRow("SELECT StateMachineID FROM GLOBAL_LOCK WHERE StateMachineType = ? AND StateMachineID = ? AND (UnlockTimestamp IS NULL OR UnlockTimestamp > NOW()) FOR UPDATE;", sm.Name, sm.ID).Scan(&ownerStateMachineID)
	if err == nil {
		// The global lock is owned by this instance.
		return true, nil
	} else if err == sql.ErrNoRows {
		// The global lock is not owned by this instance.
		return false, nil
	}
	// Handle other potential errors.
	return false, err
}

// checkGlobalLockExists checks if a global lock exists for the specified state machine type.
func checkGlobalLockExists(tx *sql.Tx, sm *StateMachine) (bool, error) {
	// Check if any open global lock exists for the same type.
	var existingLockID int
	err := tx.QueryRow("SELECT ID FROM GLOBAL_LOCK WHERE StateMachineType = ? AND (UnlockTimestamp IS NULL OR UnlockTimestamp > NOW()) FOR UPDATE;", sm.Name).Scan(&existingLockID)
	if err == nil {
		// A global lock with the same type is active.
		return true, nil
	} else if err == sql.ErrNoRows {
		// No global lock exists.
		return false, nil
	}
	// Handle other potential errors.
	return false, err
}

// obtainGlobalLock attempts to obtain a global lock for a specific type of state machine instance with a custom lookup key.
func obtainGlobalLock(tx *sql.Tx, sm *StateMachine) error {
	// Check if a global lock with the same type exists.
	globalLockExists, err := checkGlobalLockExists(tx, sm)
	if err != nil {
		return err
	}

	if globalLockExists {
		// A global lock with the same type is active, return an error.
		return fmt.Errorf("failed to obtain global lock: another instance holds the lock (Type: %s, ID: %s)", sm.Name, sm.ID)
	}

	// Insert a new global lock record into the GLOBAL_LOCK table with the type, instance, and custom lookup key.
	_, err = tx.Exec("INSERT INTO GLOBAL_LOCK (StateMachineType, StateMachineID, LookupKey, LockTimestamp) VALUES (?, ?, ?, NOW());", sm.Name, sm.ID, sm.LookupKey)
	if err != nil {
		return err
	}

	// Lock obtained successfully.
	return nil
}

func CreateStateMachineTableIfNotExists(db *sql.DB, stateMachineName string) error {
	// Define the SQL statement to create the table for the state machine
	createTableSQL := fmt.Sprintf(`
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
			UnlockTimestamp TIMESTAMP NULL,  
			INDEX (LookupKey)
            -- Add other columns as needed
        );`, normalizeTableName(stateMachineName))

	// Execute the SQL statement to create the table
	_, err := db.Exec(createTableSQL)
	return err
}

// checkLocalLockExists checks if a local lock exists for the given state machine and custom lookup key.
func checkLocalLockExists(tx *sql.Tx, sm *StateMachine) (bool, error) {
	var existingLockID int
	err := tx.QueryRow(fmt.Sprintf("SELECT ID FROM %s WHERE LookupKey = ? AND UnlockTimestamp IS NULL FOR UPDATE;", normalizeTableName(sm.Name)), sm.LookupKey).Scan(&existingLockID)
	if err == nil {
		// A local lock with the same lookup key already exists.
		return true, nil
	} else if err == sql.ErrNoRows {
		// No local lock with the same lookup key exists.
		return false, nil
	}
	// Handle other potential errors.
	return false, err
}

// isLocalLockOwnedByThisInstance checks if the local lock is owned by this state machine instance.
func isLocalLockOwnedByThisInstance(tx *sql.Tx, sm *StateMachine) (bool, error) {
	var existingLockID int
	err := tx.QueryRow(fmt.Sprintf("SELECT ID FROM %s WHERE ID = ? AND LookupKey = ? AND UnlockTimestamp IS NULL FOR UPDATE;", normalizeTableName(sm.Name)), sm.ID, sm.LookupKey).Scan(&existingLockID)
	if err == nil {
		// The local lock is owned by this instance.
		return true, nil
	} else if err == sql.ErrNoRows {
		// The local lock is not owned by this instance.
		return false, nil
	}
	// Handle other potential errors.
	return false, err
}

func obtainLocalLock(tx *sql.Tx, sm *StateMachine) error {
	// Check if a local lock with the given lookup key already exists.
	lockExists, err := checkLocalLockExists(tx, sm)
	if err != nil {
		return err
	}

	if lockExists {
		// A local lock with the same lookup key already exists, return an error.
		return fmt.Errorf("failed to obtain local lock: another instance holds the lock")
	}

	// Serialize the StateMachine to JSON
	serializedState, err := sm.serializeToJSON()
	if err != nil {
		return err
	}

	// Insert a new local lock record into the state machine's table with the custom lookup key.
	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s (ID, CurrentState, LookupKey, ResumeFromStep, SaveAfterStep, KafkaEventTopic, SerializedState, CreatedTimestamp, UpdatedTimestamp, UsesGlobalLock, UsesLocalLock, UnlockTimestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL);", normalizeTableName(sm.Name)),
		sm.ID, sm.CurrentState, sm.LookupKey, sm.ResumeFromStep, sm.SaveAfterStep, sm.KafkaEventTopic, string(serializedState), sm.CreatedTimestamp, sm.UpdatedTimestamp, sm.UsesGlobalLock, sm.UsesLocalLock)

	if err != nil {
		return err
	}

	// Lock obtained successfully.
	return nil
}

func insertStateMachine(sm *StateMachine) error {
	tableName := normalizeTableName(sm.Name)
	insertSQL := fmt.Sprintf(`
        INSERT INTO %s (ID, CurrentState, LookupKey, ResumeFromStep, SaveAfterStep, KafkaEventTopic, SerializedState, CreatedTimestamp, UpdatedTimestamp, UsesGlobalLock, UsesLocalLock)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`, tableName)

	serializedState, err := sm.serializeToJSON()
	if err != nil {
		return err
	}

	// Execute the SQL statement within the transaction
	_, err = sm.DB.Exec(insertSQL, sm.ID, sm.CurrentState, sm.LookupKey, sm.ResumeFromStep, sm.SaveAfterStep, sm.KafkaEventTopic, string(serializedState), sm.CreatedTimestamp.UTC(), sm.UpdatedTimestamp.UTC(), sm.UsesGlobalLock, sm.UsesLocalLock)

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
		// Scan row data into sm fields
		err := rows.Scan(&sm.ID, &sm.CurrentState, &sm.ResumeFromStep, &sm.SaveAfterStep, &sm.KafkaEventTopic, serializedState, &sm.CreatedTimestamp, &sm.UpdatedTimestamp, &sm.UsesGlobalLock, &sm.UsesLocalLock)
		if err != nil {
			return nil, err
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
	rows, err := sm.DB.Query(querySQL, sm.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Parse the query results into StateMachine structs
	var serializedState string
	var createdTimestampStr, updatedTimestampStr, unlockedTimestampStr sql.NullString
	for rows.Next() {
		// Scan row data into sm fields
		err := rows.Scan(&sm.ID, &sm.CurrentState, &sm.ResumeFromStep, &sm.SaveAfterStep, &sm.KafkaEventTopic, serializedState, &sm.CreatedTimestamp, &sm.UpdatedTimestamp, &sm.UsesGlobalLock, &sm.UsesLocalLock)
		if err != nil {
			return nil, err
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
			sm.UnlockedTimestamp, err = parseTimestamp(unlockedTimestampStr.String)
			if err != nil {
				return nil, err
			}
		}
	}

	return sm, nil
}

func loadStateMachine(tx *sql.Tx, sm *StateMachine) (*StateMachine, error) {
	tableName := normalizeTableName(sm.Name)
	querySQL := fmt.Sprintf("SELECT * FROM %s WHERE ID = ?;", tableName)

	var loadedSM StateMachine
	var serializedState []byte
	var createdTimestampStr, updatedTimestampStr, unlockedTimestampStr sql.NullString

	err := tx.QueryRow(querySQL, sm.ID).Scan(&loadedSM.ID, &loadedSM.CurrentState, &loadedSM.LookupKey, &loadedSM.ResumeFromStep, &loadedSM.SaveAfterStep, &loadedSM.KafkaEventTopic, &serializedState, &createdTimestampStr, &updatedTimestampStr, &loadedSM.UsesGlobalLock, &loadedSM.UsesLocalLock, &unlockedTimestampStr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("state machine not found")
		}
		return nil, err
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
		loadedSM.UnlockedTimestamp, err = parseTimestamp(unlockedTimestampStr.String)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
	}

	loadedSM.SerializedState = serializedState
	return &loadedSM, nil
}

func lockStateMachine(tx *sql.Tx, sm *StateMachine) error {
	tableName := normalizeTableName(sm.Name)
	lockSQL := fmt.Sprintf("SELECT ID FROM %s WHERE ID = ? FOR UPDATE;", tableName)

	_, err := tx.Exec(lockSQL, sm.ID)
	return err
}

func loadAndLockStateMachine(sm *StateMachine) (*StateMachine, error) {
	tableName := normalizeTableName(sm.Name)
	tx, err := sm.DB.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Lock the state machine
	err = lockStateMachine(tx, sm)
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
	updateSQL := fmt.Sprintf("UPDATE %s SET CurrentState = '%s', UpdatedTimestamp = NOW() WHERE ID = ?;", tableName, StateInProgress)
	_, err = tx.Exec(updateSQL, sm.ID)
	if err != nil {
		return nil, err
	}

	tx.Commit()
	return loadedSM, nil
}

func updateStateMachineState(sm *StateMachine, newState State) error {
	tableName := normalizeTableName(sm.Name)
	updateSQL := fmt.Sprintf("UPDATE %s SET CurrentState = ?, SerializedState = ?, UpdatedTimestamp = NOW() WHERE ID = ?;", tableName)

	tx, err := sm.DB.Begin()
	if err != nil {
		return err
	}

	newSerializedState, err := sm.serializeToJSON()
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec(updateSQL, newState, newSerializedState, sm.ID)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
