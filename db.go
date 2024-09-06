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

func removeGlobalLockOwnedByThisInstanceSQL() string {
	return "UPDATE GLOBAL_LOCK SET UnlockTimestamp = NOW() WHERE StateMachineType = ? AND StateMachineID = ? AND LookupKey = ? AND (UnlockTimestamp IS NULL OR UnlockTimestamp > NOW());"
}
func removeGlobalLockOwnedByThisInstance(tx *sql.Tx, sm *StateMachine) error {
	_, err := tx.Exec(removeGlobalLockOwnedByThisInstanceSQL(), sm.Name, sm.UniqueID, sm.LookupKey)
	if err != nil {
		return err
	}
	return nil
}

func removeGlobalLockOwnedByThisMachineTypeSQL() string {
	return "UPDATE GLOBAL_LOCK SET UnlockTimestamp = NOW() WHERE StateMachineType = ? AND LookupKey = ? AND (UnlockTimestamp IS NULL OR UnlockTimestamp > NOW());"
}
func removeGlobalLockOwnedByThisMachineType(tx *sql.Tx, sm *StateMachine) error {
	_, err := tx.Exec(removeGlobalLockOwnedByThisMachineTypeSQL(), sm.Name, sm.LookupKey)
	if err != nil {
		return err
	}
	return nil
}

func removeAllGlobalLocksSQL() string {
	return "UPDATE GLOBAL_LOCK SET UnlockTimestamp = NOW() WHERE AND LookupKey = ? AND (UnlockTimestamp IS NULL OR UnlockTimestamp > NOW());"
}
func removeAllGlobalLocks(tx *sql.Tx, sm *StateMachine) error {
	_, err := tx.Exec(removeAllGlobalLocksSQL(), sm.Name, sm.LookupKey)
	if err != nil {
		return err
	}
	return nil
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
	return fmt.Sprintf("SELECT ID FROM %s WHERE %s AND CurrentState = '%s' AND (UnlockedTimestamp IS NULL OR UnlockedTimestamp > NOW()) FOR UPDATE;", normalizeTableName(tableName), condition, StateLocked)
}

// checkLockStatus checks if a lock exists based on provided conditions.
func checkLockStatus(tx *sql.Tx, query string, args ...interface{}) (bool, error) {
	var lockID string
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
	CurrentState = VALUES(CurrentState),
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

	var lastRetry interface{}
	if sm.LastRetry == nil {
		lastRetry = nil
	} else {
		lastRetry = sm.LastRetry.UTC()
	}
	localSQL := obtainLocalLockSQL(sm.Name)
	// Insert or update the local lock record
	_, err = tx.Exec(localSQL,
		sm.UniqueID, StateLocked, sm.LookupKey, sm.ResumeFromStep, sm.SaveAfterEachStep, sm.KafkaEventTopic, string(serializedState), sm.CreatedTimestamp.UTC(), sm.UpdatedTimestamp.UTC(), unlockedTimestamp, lastRetry, usesGlobalLock, usesLocalLock)

	if err != nil {
		return err
	}

	// Lock obtained successfully.
	return nil
}

func insertStateMachineSQL(tableName string) string {
	return fmt.Sprintf(`
	INSERT INTO %s (ID, 
		CurrentState, 
		LookupKey, 
		ResumeFromStep, 
		SaveAfterStep, 
		KafkaEventTopic, 
		SerializedState, 
		CreatedTimestamp, 
		UpdatedTimestamp, 
		UnlockedTimestamp, 
		LastRetryTimestamp, 
		UsesGlobalLock, 
		UsesLocalLock)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`, normalizeTableName(tableName))
}
func insertStateMachine(sm *StateMachine) error {

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
	_, err = sm.DB.Exec(insertStateMachineSQL(sm.Name), sm.UniqueID, sm.CurrentState, sm.LookupKey, sm.ResumeFromStep, sm.SaveAfterEachStep, sm.KafkaEventTopic, string(serializedState), sm.CreatedTimestamp.UTC(), sm.UpdatedTimestamp.UTC(), unlockedTimestamp, lastRetry, usesGlobalLock, usesLocalLock)

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

// TODO: create unit tests for this
// Load the serialized JSON state data from the database
func loadStateMachineFromDB(stateMachineType string, id string, db *sql.DB) (*StateMachine, error) {
	// Load serialized state data from the database (replace with your database logic)
	sm := &StateMachine{
		UniqueID: id,
		Name:     stateMachineType,
		DB:       db,
	}
	sm, err := loadAndLockStateMachine(sm)
	if err != nil {
		return nil, NewDatabaseOperationError("loadAndLockStateMachine", err)
	}
	return sm, nil
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
	return fmt.Sprintf("UPDATE %s SET CurrentState = '%s', UpdatedTimestamp = NOW() WHERE ID = ?;", tableName, StateLocked)
}

func loadAndLockStateMachine(sm *StateMachine) (*StateMachine, error) {
	tableName := normalizeTableName(sm.Name)

	// check if there is machine type lock
	locks, err := checkStateMachineTypeLock(sm.DB, sm.Name, time.Now())
	if err != nil {
		return nil, err
	}

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
		return nil, NewDatabaseOperationError("loadAndLockStateMachine",
			fmt.Errorf("state machine is in a terminal state %s", sm.CurrentState))
	}

	if len(locks) > 0 {
		// there is a machine type lock
		sm.MachineLockInfo = &locks[0]
		// check if the state machine is sleeping
		if loadedSM.CurrentState == StateSleeping {
			return loadedSM, NewSleepStateError(locks[0].Start, locks[0].End,
				fmt.Errorf("state machine can't be locked and loaded because it's sleeping"))
		}
		// check if there is a lock for this machine type that halts all executions
		for _, lock := range locks {
			if lock.Type == MachineLockTypeHaltAll {
				return loadedSM, NewHaltAllStateMachinesByTypeError(locks[0].Start, locks[0].End,
					fmt.Errorf("state machine can't be locked and loaded because everything has been halted"))
			}
		}
	}

	// Set state to locked and update the database
	_, err = tx.Exec(loadAndLockStateMachineSQL(tableName, StateLocked), sm.UniqueID)
	if err != nil {
		return nil, err
	}

	tx.Commit()
	return loadedSM, nil
}

func removeLocalLockSQL(tableName string) string {
	return fmt.Sprintf("UPDATE %s UnlockedTimestamp = NOW() WHERE ID = ?;", tableName)
}

func removeLocalLock(tx *sql.Tx, sm *StateMachine) error {
	_, err := tx.Exec(lockStateMachineForTransactionSQL(sm.Name), sm.UniqueID)
	return err
}

func updateStateMachineStateSQL(tableName string) string {
	return fmt.Sprintf("UPDATE %s SET CurrentState = ?, SerializedState = ?, UpdatedTimestamp = NOW(), ResumeFromStep = ?, UnlockedTimestamp = ? WHERE ID = ?;", tableName)
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
	var unlockedTimestamp interface{}
	if sm.UnlockedTimestamp != nil {
		unlockedTimestamp = sm.UnlockedTimestamp.UTC()
	}

	_, err = tx.Exec(updateStateMachineStateSQL(tableName), newState, newSerializedState, sm.ResumeFromStep, unlockedTimestamp, sm.UniqueID)
	if err != nil {
		tx.Rollback()
		return err
	}

	if !sm.UnlockGlobalLockAfterEachStep || IsTerminalState(newState) {
		// remove global lock
		removeGlobalLockOwnedByThisInstance(tx, sm)
	}

	return tx.Commit()
}

func createStateMachineLockTableIfNotExistsSQL() string {
	return `
	CREATE TABLE IF NOT EXISTS STATE_MACHINE_TYPE_LOCK (
		StateMachineType VARCHAR(255),
		LockType VARCHAR(50),
		StartTimestamp TIMESTAMP NULL,
		EndTimestamp TIMESTAMP NULL,
		RecurInterval VARCHAR(10), -- 'None', 'Daily', 'Weekly', 'Monthly'
		IntervalPeriod INT NULL, -- Period Length for the RecurInterval unit. E.g.: 2 IntervalPeriod and Weekly RecurInterval means every other week
		DayOfWeek INT NULL, -- 1 = Monday, 7 = Sunday, used for weekly recurrence
		DayOfMonth INT NULL, -- 1-31, used for monthly recurrence
		RecurStartDate TIMESTAMP NULL, -- Start date for recurrence
		RecurEndDate TIMESTAMP NULL, -- End date for recurrence
		RecurStartTime TIME NULL, -- Time when the recurrence starts each day
		RecurEndTime TIME NULL -- Time when the recurrence ends each day
	);`
}

func CreateStateMachineLockTableIfNotExists(db *sql.DB) error {
	_, err := db.Exec(createStateMachineLockTableIfNotExistsSQL())
	if err != nil {
		return err
	}

	return nil
}

func checkStateMachineTypeLockSQL() string {
	return `SELECT LockType, StartTimestamp, EndTimestamp, RecurInterval, IntervalPeriod, DayOfWeek, DayOfMonth, RecurStartDate, RecurEndDate, RecurStartTime, RecurEndTime 
	FROM STATE_MACHINE_TYPE_LOCK 
	WHERE StateMachineType = ? AND StartTimestamp <= ? AND EndTimestamp >= ?
	ORDER BY StartTimestamp ASC`
}

func checkStateMachineTypeLock(db *sql.DB, stateMachineType string, queryTime time.Time) ([]StateMachineTypeLockInfo, error) {
	var locks []StateMachineTypeLockInfo

	rows, err := db.Query(checkStateMachineTypeLockSQL(), stateMachineType, &queryTime, &queryTime)
	if err != nil {
		if err == sql.ErrNoRows {
			return locks, nil
		}
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var lockTypeStr, recurInterval string
		var startTimestamp, endTimestamp time.Time
		var intervalPeriod, dayOfWeek, dayOfMonth int
		var recurStartDate, recurEndDate, recurStartTime, recurEndTime time.Time

		if err := rows.Scan(&lockTypeStr, &startTimestamp, &endTimestamp, &recurInterval, &intervalPeriod, &dayOfWeek, &dayOfMonth, &recurStartDate, &recurEndDate, &recurStartTime, &recurEndTime); err != nil {
			return nil, err
		}

		lockType := ParseMachineLockType(lockTypeStr)

		lockInfo := StateMachineTypeLockInfo{
			StateMachineType: stateMachineType,
			Type:             lockType,
			Start:            startTimestamp,
			End:              endTimestamp,
			RecurInterval:    recurInterval,
			IntervalPeriod:   intervalPeriod,
			DayOfWeek:        dayOfWeek,
			DayOfMonth:       dayOfMonth,
			RecurStartDate:   recurStartDate,
			RecurEndDate:     recurEndDate,
			RecurStartTime:   recurStartTime,
			RecurEndTime:     recurEndTime,
		}

		// Check if the current time is within the recurrence interval
		if recurInterval != "None" && isWithinRecurringSchedule(recurInterval, intervalPeriod, recurStartDate, recurEndDate, recurStartTime, recurEndTime, queryTime, dayOfWeek, dayOfMonth) {
			locks = append(locks, lockInfo)
		} else if recurInterval == "None" && queryTime.After(startTimestamp) && queryTime.Before(endTimestamp) {
			// Check for one-time lock
			locks = append(locks, lockInfo)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return locks, nil
}

// SQL Queries as separate functions
func createLockQuery() string {
	return `INSERT INTO STATE_MACHINE_TYPE_LOCK 
			(StateMachineType, LockType, StartTimestamp, EndTimestamp, RecurInterval, IntervalPeriod, DayOfWeek, DayOfMonth, RecurStartDate, RecurEndDate, RecurStartTime, RecurEndTime) 
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
}

func updateLockQuery() string {
	return `UPDATE STATE_MACHINE_TYPE_LOCK 
			SET LockType = ?, StartTimestamp = ?, EndTimestamp = ?, RecurInterval = ?, IntervalPeriod = ?, DayOfWeek = ?, DayOfMonth = ?, RecurStartDate = ?, RecurEndDate = ?, RecurStartTime = ?, RecurEndTime = ?
			WHERE StateMachineType = ?`
}

func deleteLockQuery() string {
	return `DELETE FROM STATE_MACHINE_TYPE_LOCK WHERE StateMachineType = ? AND LockType = ?`
}

// Database functions using the query functions
func createLock(db *sql.DB, lock StateMachineTypeLockInfo) error {
	_, err := db.Exec(createLockQuery(), lock.StateMachineType, lock.Type, lock.Start.UTC(), lock.End.UTC(), lock.RecurInterval, lock.IntervalPeriod, lock.DayOfWeek, lock.DayOfMonth, lock.RecurStartDate.UTC(), lock.RecurEndDate.UTC(), lock.RecurStartTime.UTC(), lock.RecurEndTime.UTC())
	return err
}

func updateLock(db *sql.DB, lock StateMachineTypeLockInfo) error {
	_, err := db.Exec(updateLockQuery(), lock.Type, lock.Start.UTC(), lock.End.UTC(), lock.RecurInterval, lock.IntervalPeriod, lock.DayOfWeek, lock.DayOfMonth, lock.RecurStartDate.UTC(), lock.RecurEndDate.UTC(), lock.RecurStartTime.UTC(), lock.RecurEndTime.UTC(), lock.StateMachineType)
	return err
}

func deleteLock(db *sql.DB, stateMachineType string, lockType MachineLockType) error {
	_, err := db.Exec(deleteLockQuery(), stateMachineType, lockType)
	return err
}
