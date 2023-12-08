package statemachine

import (
	"database/sql"
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
            StateMachineID VARCHAR(255),
			LookupKey VARCHAR(255),
            LockTimestamp TIMESTAMP,
			UnlockTimestamp TIMESTAMP NULL,
			PRIMARY KEY (ID),
			UNIQUE (StateMachineID, LookupKey),
			INDEX (LookupKey)
        );`

	// Execute the SQL statement to create the table
	_, err := db.Exec(createTableSQL)
	if err != nil {
		return err
	}

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
            CreatedTimestamp TIMESTAMP,
            UpdatedTimestamp TIMESTAMP,
            IsGlobalLock BOOLEAN,
            IsLocalLock BOOLEAN,
			INDEX (LookupKey)
            -- Add other columns as needed
        );`, normalizeTableName(stateMachineName))

	// Execute the SQL statement to create the table
	_, err := db.Exec(createTableSQL)
	return err
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
