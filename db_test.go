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

	// Use separate time objects for each field
	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	startTime := baseTime
	endTime := baseTime.Add(time.Hour)
	recurStartTime := baseTime
	recurEndTime := baseTime.Add(time.Hour)
	recurStartDate := baseTime
	recurEndDate := baseTime.Add(24 * time.Hour)

	lock := StateMachineTypeLockInfo{
		StateMachineType: "TestMachine",
		Type:             "TestLock",
		Start:            startTime,
		End:              endTime,
		RecurInterval:    "None",
		DayOfWeek:        0,
		DayOfMonth:       0,
		RecurStartTime:   recurStartTime,
		RecurEndTime:     recurEndTime,
		IntervalPeriod:   1,
		RecurStartDate:   recurStartDate,
		RecurEndDate:     recurEndDate,
	}

	mock.ExpectExec(escapeRegexChars(updateLockQuery())).
		WithArgs(lock.Type, lock.Start.UTC(), lock.End.UTC(), lock.RecurInterval, lock.IntervalPeriod, lock.DayOfWeek, lock.DayOfMonth, lock.RecurStartDate.UTC(), lock.RecurEndDate.UTC(), lock.RecurStartTime.UTC(), lock.RecurEndTime.UTC(), lock.StateMachineType).
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

	// Use a fixed time for all time fields to avoid comparison issues
	baseTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	startTime := baseTime
	endTime := baseTime.Add(time.Hour)
	recurStartTime := baseTime
	recurEndTime := baseTime.Add(time.Hour)
	recurStartDate := baseTime
	recurEndDate := baseTime.Add(24 * time.Hour)

	lock := StateMachineTypeLockInfo{
		StateMachineType: "TestMachine",
		Type:             "TestLock",
		Start:            startTime,
		End:              endTime,
		RecurInterval:    "None",
		IntervalPeriod:   1,
		DayOfWeek:        0,
		DayOfMonth:       0,
		RecurStartDate:   recurStartDate,
		RecurEndDate:     recurEndDate,
		RecurStartTime:   recurStartTime,
		RecurEndTime:     recurEndTime,
	}

	mock.ExpectExec(escapeRegexChars(createLockQuery())).
		WithArgs(lock.StateMachineType, lock.Type, lock.Start.UTC(), lock.End.UTC(), lock.RecurInterval, lock.IntervalPeriod, lock.DayOfWeek, lock.DayOfMonth, lock.RecurStartDate.UTC(), lock.RecurEndDate.UTC(), lock.RecurStartTime.UTC(), lock.RecurEndTime.UTC()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := createLock(db, lock); err != nil {
		t.Errorf("error was not expected while creating lock: %s", err)
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

	testCases := []struct {
		name             string
		queryTime        time.Time
		expectedLockType string
		rows             *sqlmock.Rows
		expectedCount    int
	}{
		{
			name:             "Basic locks",
			queryTime:        referenceTime,
			expectedLockType: "ImmediateReject",
			rows: sqlmock.NewRows([]string{"LockType", "StartTimestamp", "EndTimestamp", "RecurInterval", "IntervalPeriod", "DayOfWeek", "DayOfMonth", "RecurStartDate", "RecurEndDate", "RecurStartTime", "RecurEndTime"}).
				AddRow("ImmediateReject", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "None", 0, 0, 0, referenceTime.Add(-24*time.Hour), referenceTime.Add(24*time.Hour), referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)).
				AddRow("NoMatch", referenceTime.Add(-2*time.Hour), referenceTime.Add(-time.Hour), "None", 0, 0, 0, referenceTime.Add(-24*time.Hour), referenceTime.Add(24*time.Hour), referenceTime.Add(-2*time.Hour), referenceTime.Add(-time.Hour)).
				AddRow("NoMatch", referenceTime.Add(time.Hour), referenceTime.Add(2*time.Hour), "None", 0, 0, 0, referenceTime.Add(-24*time.Hour), referenceTime.Add(24*time.Hour), referenceTime.Add(time.Hour), referenceTime.Add(2*time.Hour)).
				AddRow("ImmediateReject", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Daily", 1, 0, 0, referenceTime.Add(-24*time.Hour), referenceTime.Add(24*time.Hour), referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)).
				AddRow("ImmediateReject", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Weekly", 1, int(referenceTime.Weekday()), 0, referenceTime.Add(-7*24*time.Hour), referenceTime.Add(7*24*time.Hour), referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)).
				AddRow("ImmediateReject", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Monthly", 1, 0, referenceTime.Day(), referenceTime.Add(-30*24*time.Hour), referenceTime.Add(30*24*time.Hour), referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)).
				AddRow("NoMatch", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Weekly", 1, 5, 0, referenceTime.Add(-7*24*time.Hour), referenceTime.Add(7*24*time.Hour), referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)).
				AddRow("NoMatch", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Monthly", 1, 0, 2, referenceTime.Add(-30*24*time.Hour), referenceTime.Add(30*24*time.Hour), referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)),
			expectedCount: 4,
		},
		{
			name:             "Bi-weekly lock",
			queryTime:        time.Date(2024, time.January, 15, 12, 0, 0, 0, time.UTC),
			expectedLockType: "ImmediateReject",
			rows: sqlmock.NewRows([]string{"LockType", "StartTimestamp", "EndTimestamp", "RecurInterval", "IntervalPeriod", "DayOfWeek", "DayOfMonth", "RecurStartDate", "RecurEndDate", "RecurStartTime", "RecurEndTime"}).
				AddRow("ImmediateReject", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Weekly", 2, 1, 0, referenceTime.Add(-30*24*time.Hour), referenceTime.Add(30*24*time.Hour), referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)),
			expectedCount: 1,
		},
		{
			name:             "Tri-daily lock",
			queryTime:        time.Date(2024, time.January, 4, 12, 0, 0, 0, time.UTC),
			expectedLockType: "ImmediateReject",
			rows: sqlmock.NewRows([]string{"LockType", "StartTimestamp", "EndTimestamp", "RecurInterval", "IntervalPeriod", "DayOfWeek", "DayOfMonth", "RecurStartDate", "RecurEndDate", "RecurStartTime", "RecurEndTime"}).
				AddRow("ImmediateReject", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Daily", 3, 0, 0, referenceTime.Add(-30*24*time.Hour), referenceTime.Add(30*24*time.Hour), referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)),
			expectedCount: 1,
		},
		{
			name:             "Bi-monthly lock",
			queryTime:        time.Date(2024, time.March, 1, 12, 0, 0, 0, time.UTC),
			expectedLockType: "ImmediateReject",
			rows: sqlmock.NewRows([]string{"LockType", "StartTimestamp", "EndTimestamp", "RecurInterval", "IntervalPeriod", "DayOfWeek", "DayOfMonth", "RecurStartDate", "RecurEndDate", "RecurStartTime", "RecurEndTime"}).
				AddRow("ImmediateReject", referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour), "Monthly", 2, 0, 1, referenceTime.Add(-60*24*time.Hour), referenceTime.Add(60*24*time.Hour), referenceTime.Add(-time.Hour), referenceTime.Add(time.Hour)),
			expectedCount: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mock.ExpectQuery(escapeRegexChars(checkStateMachineTypeLockSQL())).
				WithArgs(stateMachineType, tc.queryTime, tc.queryTime).
				WillReturnRows(tc.rows)

			locks, err := checkStateMachineTypeLock(db, stateMachineType, tc.queryTime)
			if err != nil {
				t.Errorf("error was not expected while checking locks: %s", err)
			}

			if len(locks) != tc.expectedCount {
				t.Errorf("expected %d locks, got %d", tc.expectedCount, len(locks))
			}

			for i, lock := range locks {
				if lock.StateMachineType != stateMachineType {
					t.Errorf("lock %d: expected StateMachineType %s, got %s", i, stateMachineType, lock.StateMachineType)
				}
				if lock.Type != "ImmediateReject" {
					t.Errorf("lock %d: expected Type '%s', got %s", i, tc.expectedLockType, lock.Type)
				}
			}
		})
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
