package statemachine

import (
	"fmt"
	"time"
)

type MachineLockType string

const (
	MachineLockTypeImmediateReject MachineLockType = "ImmediateReject"
	MachineLockTypeSleepState      MachineLockType = "SleepState"
	MachineLockTypeHaltAll         MachineLockType = "HaltAll"
	MachineLockTypeNone            MachineLockType = "None" // Default type when no lock is active
)

// String returns the string representation of the MachineLockType.
func (lt MachineLockType) String() string {
	return string(lt)
}

// ParseMachineLockType converts a string to a MachineLockType.
func ParseMachineLockType(s string) MachineLockType {
	switch s {
	case "ImmediateReject":
		return MachineLockTypeImmediateReject
	case "SleepState":
		return MachineLockTypeSleepState
	case "HaltAll":
		return MachineLockTypeHaltAll
	default:
		return MachineLockTypeNone
	}
}

type LockType int

const (
	NoLock LockType = iota
	GlobalLock
	LocalLock
)

func (lt LockType) String() string {
	switch lt {
	case NoLock:
		return "NoLock"
	case GlobalLock:
		return "GlobalLock"
	case LocalLock:
		return "LocalLock"
	default:
		return fmt.Sprintf("UnknownLockType(%d)", lt)
	}
}

type StateMachineTypeLockInfo struct {
	StateMachineType string          // Type of the state machine
	Type             MachineLockType // Type of the lock
	Start            time.Time       // Start time for one-time locks
	End              time.Time       // End time for one-time locks
	RecurInterval    string          // Recurrence interval ('None', 'Daily', 'Weekly', 'Monthly')
	DayOfWeek        int             // Day of the week for weekly recurrence (1 = Monday, 7 = Sunday)
	DayOfMonth       int             // Day of the month for monthly recurrence (1-31)
	RecurStartTime   time.Time       // Start time for recurring locks
	RecurEndTime     time.Time       // End time for recurring locks
}

func isWithinRecurringSchedule(interval string, dayOfWeek, dayOfMonth int, startTime, endTime, queryTime time.Time) bool {
	currentTime := queryTime

	// Check if current time is within the start and end times
	if !isTimeWithinInterval(startTime, endTime, currentTime) {
		return false
	}

	switch interval {
	case "Daily":
		return true // Every day within the time interval
	case "Weekly":
		return int(currentTime.Weekday()) == dayOfWeek
	case "Monthly":
		return currentTime.Day() == dayOfMonth
	}

	return false
}

func isTimeWithinInterval(startTime, endTime, currentTime time.Time) bool {
	// Normalize times to the same date for comparison
	year, month, day := currentTime.Date()
	normalizedStartTime := time.Date(year, month, day, startTime.Hour(), startTime.Minute(), startTime.Second(), startTime.Nanosecond(), startTime.Location())
	normalizedEndTime := time.Date(year, month, day, endTime.Hour(), endTime.Minute(), endTime.Second(), endTime.Nanosecond(), endTime.Location())

	// Check if current time is within the start and end times
	return currentTime.After(normalizedStartTime) && currentTime.Before(normalizedEndTime)
}
