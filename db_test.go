package statemachine

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRemoveGlobalLockOwnedByThisMachineType(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	stateMachine := &StateMachine{
		Name:      "TestMachine",
		LookupKey: "TestKey",
	}

	mock.ExpectBegin()

	// Start a mock transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when starting a transaction", err)
	}

	// Mock the SQL execution
	mock.ExpectExec(escapeRegexChars(removeGlobalLockOwnedByThisMachineTypeSQL())).
		WithArgs(stateMachine.Name, stateMachine.LookupKey).
		WillReturnResult(sqlmock.NewResult(0, 1)) // Mocking one record updated

	// Call the function
	if err := removeGlobalLockOwnedByThisMachineType(tx, stateMachine); err != nil {
		t.Errorf("error was not expected while removing global lock: %s", err)
	}

	mock.ExpectCommit()

	// Commit the transaction
	tx.Commit()

	// Check if the expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestDeleteLock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	stateMachineType := "TestMachine"

	mock.ExpectExec(escapeRegexChars(deleteLockQuery())).
		WithArgs(stateMachineType, MachineLockTypeImmediateReject).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := deleteLock(db, stateMachineType, MachineLockTypeImmediateReject); err != nil {
		t.Errorf("error was not expected while deleting lock: %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestUpdateLock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	lock := StateMachineTypeLockInfo{
		StateMachineType: "TestMachine",
		Type:             "TestLock",
		Start:            time.Now(),
		End:              time.Now().Add(time.Hour),
		RecurInterval:    "None",
		DayOfWeek:        0,
		DayOfMonth:       0,
		RecurStartTime:   time.Now(),
		RecurEndTime:     time.Now().Add(time.Hour),
	}

	mock.ExpectExec(escapeRegexChars(updateLockQuery())).
		WithArgs(lock.Type, lock.Start.UTC(), lock.End.UTC(), lock.RecurInterval, lock.DayOfWeek, lock.DayOfMonth, lock.RecurStartTime.UTC(), lock.RecurEndTime.UTC(), lock.StateMachineType).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := updateLock(db, lock); err != nil {
		t.Errorf("error was not expected while updating lock: %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateLock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	lock := StateMachineTypeLockInfo{
		StateMachineType: "TestMachine",
		Type:             "TestLock",
		Start:            time.Now(),
		End:              time.Now().Add(time.Hour),
		RecurInterval:    "None",
		DayOfWeek:        0,
		DayOfMonth:       0,
		RecurStartTime:   time.Now(),
		RecurEndTime:     time.Now().Add(time.Hour),
	}

	mock.ExpectExec(escapeRegexChars(createLockQuery())).
		WithArgs(lock.StateMachineType, lock.Type, lock.Start.UTC(), lock.End.UTC(), lock.RecurInterval, lock.DayOfWeek, lock.DayOfMonth, lock.RecurStartTime.UTC(), lock.RecurEndTime.UTC()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := createLock(db, lock); err != nil {
		t.Errorf("error was not expected while updating stats: %s", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCheckStateMachineTypeLock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	stateMachineType := "TestMachine"
	// January 1, 2024 is a Monday
	referenceTime := time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC)

	// Mock rows to be returned by the query
	rows := sqlmock.NewRows([]string{"LockType", "StartTimestamp", "EndTimestamp", "RecurInterval", "DayOfWeek", "DayOfMonth", "RecurStartTime", "RecurEndTime"}).
		AddRow("Matches", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "None", 0, 0, referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)).                              // Matches
		AddRow("NoMatch", referenceTime.Add(-2*time.Hour), referenceTime.Add(-time.Hour), "None", 0, 0, referenceTime.Add(-2*time.Hour), referenceTime.Add(-time.Hour)).                        // Does not match
		AddRow("NoMatch", referenceTime.Add(time.Hour), referenceTime.Add(2*time.Hour), "None", 0, 0, referenceTime.Add(time.Hour), referenceTime.Add(2*time.Hour)).                            // Does not match
		AddRow("Matches", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Daily", 0, 0, referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)).                             // Matches
		AddRow("Matches", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Weekly", int(referenceTime.Weekday()), 0, referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)). // Matches
		AddRow("Matches", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Monthly", 0, referenceTime.Day(), referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)).         // Matches
		AddRow("NoMatch", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Weekly", 5, 0, referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)).                            // Does not match (Friday)
		AddRow("NoMatch", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Monthly", 0, 2, referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour))                            // Does not match (2nd of the month)

	mock.ExpectQuery(escapeRegexChars(checkStateMachineTypeLockSQL())).
		WithArgs(stateMachineType, referenceTime, referenceTime).
		WillReturnRows(rows)

	locks, err := checkStateMachineTypeLock(db, stateMachineType, referenceTime)
	if err != nil {
		t.Errorf("error was not expected while checking locks: %s", err)
	}

	expectedLockCount := 4 // Only the first, fourth, fifth, and sixth rows match

	if len(locks) != expectedLockCount {
		t.Errorf("expected %d locks, got %d", expectedLockCount, len(locks))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
