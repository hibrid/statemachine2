package statemachine

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
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
            ID VARCHAR(36) PRIMARY KEY,
            CurrentState VARCHAR(255),
			LookupKey VARCHAR(255),
            Direction VARCHAR(10),
            ResumeFromStep INT,
            SaveAfterStep BOOLEAN,
            KafkaEventTopic VARCHAR(255),
            SerializedState JSON,
            LockTimestamp TIMESTAMP,
            UpdatedTimestamp TIMESTAMP,
            IsGlobalLock BOOLEAN,
            IsLocalLock BOOLEAN,
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
	_, err = tx.Exec(fmt.Sprintf("INSERT INTO %s (ID, CurrentState, LookupKey, Direction, ResumeFromStep, SaveAfterStep, KafkaEventTopic, SerializedState, CreatedTimestamp, UpdatedTimestamp, IsGlobalLock, IsLocalLock, UnlockTimestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL);", normalizeTableName(sm.Name)),
		sm.ID, sm.CurrentState, sm.LookupKey, sm.Direction, sm.ResumeFromStep, sm.SaveAfterStep, sm.KafkaEventTopic, string(serializedState), sm.CreatedTimestamp, sm.UpdatedTimestamp, sm.IsGlobalLock, sm.IsLocalLock)

	if err != nil {
		return err
	}

	// Lock obtained successfully.
	return nil
}

// Insert a state machine into the database
func insertStateMachine(db *sql.DB, sm *StateMachine) error {
	// Construct the SQL statement with the appropriate table name
	tableName := sm.Name
	insertSQL := fmt.Sprintf(`
        INSERT INTO %s (ID, CurrentState, LookupKey, Direction, ResumeFromStep, SaveAfterStep, KafkaEventTopic, SerializedState, CreatedTimestamp, UpdatedTimestamp, IsGlobalLock, IsLocalLock)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`, normalizeTableName(tableName))

	serializedState, err := sm.serializeToJSON()
	if err != nil {
		return err
	}

	// Execute the SQL statement
	_, err = db.Exec(insertSQL, sm.ID, sm.CurrentState, sm.Direction, sm.ResumeFromStep, sm.SaveAfterStep, sm.KafkaEventTopic, string(serializedState), sm.CreatedTimestamp, sm.UpdatedTimestamp, sm.IsGlobalLock, sm.IsLocalLock)

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
		err := rows.Scan(&sm.ID, &sm.CurrentState, &sm.Direction, &sm.ResumeFromStep, &sm.SaveAfterStep, &sm.KafkaEventTopic, serializedState, &sm.CreatedTimestamp, &sm.UpdatedTimestamp, &sm.IsGlobalLock, &sm.IsLocalLock)
		if err != nil {
			return nil, err
		}
		stateMachines = append(stateMachines, sm)
	}

	return stateMachines, nil
}

func updateStateMachineState(db *sql.DB, sm *StateMachine) error {
	tableName := sm.Name
	updateSQL := fmt.Sprintf("UPDATE %s SET CurrentState = ? WHERE ID = ?;", tableName)

	_, err := db.Exec(updateSQL, sm.CurrentState, sm.ID)
	return err
}

// loadFromDB loads data from the database for a state machine by type and ID.
func loadFromDB(typeName, id string, db *sql.DB) ([]byte, error) {
	// Define the SQL query to select data based on type and ID.
	query := fmt.Sprintf("SELECT SerializedState FROM %s WHERE ID = ?", normalizeTableName(typeName))

	// Query the database to retrieve the serialized state.
	row := db.QueryRow(query, id)

	var serializedState []byte
	if err := row.Scan(&serializedState); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Handle case where no data is found for the given ID
			return nil, nil
		}
		return nil, err
	}

	return serializedState, nil
}
