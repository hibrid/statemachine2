package statemachine

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

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
	queryTime := time.Now()

	// Mock rows to be returned by the query
	rows := sqlmock.NewRows([]string{"LockType", "StartTimestamp", "EndTimestamp", "RecurInterval", "DayOfWeek", "DayOfMonth", "RecurStartTime", "RecurEndTime"}).
		AddRow("TestLock", queryTime.Add(-time.Hour), queryTime.Add(time.Hour), "None", 0, 0, queryTime.Add(-time.Hour), queryTime.Add(time.Hour)).                          // Matches
		AddRow("TestLock", queryTime.Add(-2*time.Hour), queryTime.Add(-time.Hour), "None", 0, 0, queryTime.Add(-2*time.Hour), queryTime.Add(-time.Hour)).                    // Does not match
		AddRow("TestLock", queryTime.Add(time.Hour), queryTime.Add(2*time.Hour), "None", 0, 0, queryTime.Add(time.Hour), queryTime.Add(2*time.Hour)).                        // Does not match
		AddRow("TestLock", queryTime.Add(-time.Hour), queryTime.Add(time.Hour), "Daily", 0, 0, queryTime.Add(-time.Hour), queryTime.Add(time.Hour)).                         // Matches
		AddRow("TestLock", queryTime.Add(-time.Hour), queryTime.Add(time.Hour), "Weekly", int(queryTime.Weekday()), 0, queryTime.Add(-time.Hour), queryTime.Add(time.Hour)). // Matches
		AddRow("TestLock", queryTime.Add(-time.Hour), queryTime.Add(time.Hour), "Monthly", 0, queryTime.Day(), queryTime.Add(-time.Hour), queryTime.Add(time.Hour)).         // Matches
		AddRow("TestLock", queryTime.Add(-time.Hour), queryTime.Add(time.Hour), "Weekly", 5, 0, queryTime.Add(-time.Hour), queryTime.Add(time.Hour)).                        // Matches (Friday)
		AddRow("TestLock", queryTime.Add(-time.Hour), queryTime.Add(time.Hour), "Monthly", 0, 1, queryTime.Add(-time.Hour), queryTime.Add(time.Hour))                        // Matches (1st of the month)

	mock.ExpectQuery(escapeRegexChars(checkStateMachineTypeLockSQL())).
		WithArgs(stateMachineType, queryTime, queryTime).
		WillReturnRows(rows)

	locks, err := checkStateMachineTypeLock(db, stateMachineType, queryTime)
	if err != nil {
		t.Errorf("error was not expected while checking locks: %s", err)
	}

	// Determine the expected number of matching locks
	expectedLockCount := 4             // One-time lock and daily recurring lock always match
	if int(queryTime.Weekday()) == 5 { // Add weekly lock if today is the correct weekday
		expectedLockCount++
	}
	if queryTime.Day() == 1 { // Add monthly lock if today is the correct day of the month
		expectedLockCount++
	}

	// Check if the returned locks slice contains the expected data
	if len(locks) != expectedLockCount {
		t.Errorf("expected %d locks, got %d", expectedLockCount, len(locks))
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
