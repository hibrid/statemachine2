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
	IntervalPeriod   int             // Period length for the RecurInterval unit (e.g., 2 IntervalPeriod and Weekly RecurInterval means every other week)
	DayOfWeek        int             // Day of the week for weekly recurrence (1 = Monday, 7 = Sunday)
	DayOfMonth       int             // Day of the month for monthly recurrence (1-31)
	RecurStartDate   time.Time       // Start date for recurring locks (defines the date from when the recurrence begins)
	RecurEndDate     time.Time       // End date for recurring locks (defines the date when the recurrence ends)
	RecurStartTime   time.Time       // Start time for recurring locks (defines the time of day recurrence starts)
	RecurEndTime     time.Time       // End time for recurring locks (defines the time of day recurrence ends)
}

func isWithinRecurringSchedule(interval string, intervalLength int, startDate, endDate, startTime, endTime, queryTime time.Time, dayOfWeek, dayOfMonth int) bool {
	currentTime := queryTime

	// Check if the query time is within the overall startDate and endDate range
	if !isDateWithinRange(startDate, endDate, currentTime) {
		return false
	}

	// Check if current time is within the start and end times for the given day
	if !isTimeWithinInterval(startTime, endTime, currentTime) {
		return false
	}

	switch interval {
	case "Daily":
		return isWithinInterval(currentTime, startDate, intervalLength, "day")
	case "Weekly":
		scheduleDayOfWeek := int(currentTime.Weekday())
		if scheduleDayOfWeek != dayOfWeek {
			return false
		}
		return isWithinInterval(currentTime, startDate, intervalLength, "week")
	case "Monthly":
		if currentTime.Day() != dayOfMonth {
			return false
		}
		return isWithinInterval(currentTime, startDate, intervalLength, "month")
	}

	return false
}

func isWithinInterval(currentTime, startDate time.Time, intervalLength int, unit string) bool {
	diff := currentTime.Sub(startDate)

	switch unit {
	case "day":
		// Every N days from the startDate
		return int(diff.Hours()/24)%intervalLength == 0
	case "week":
		// Every N weeks from the startDate
		return int(diff.Hours()/(24*7))%intervalLength == 0
	case "month":
		// Every N months from the startDate
		startYear, startMonth, _ := startDate.Date()
		currentYear, currentMonth, _ := currentTime.Date()
		monthDiff := (currentYear-startYear)*12 + int(currentMonth-startMonth)
		return monthDiff%intervalLength == 0
	}

	return false
}

func isDateWithinRange(startDate, endDate, currentTime time.Time) bool {
	return (currentTime.Equal(startDate) || currentTime.After(startDate)) && (currentTime.Equal(endDate) || currentTime.Before(endDate))
}

func isTimeWithinInterval(startTime, endTime, currentTime time.Time) bool {
	// Normalize times to the same date for comparison
	year, month, day := currentTime.Date()
	normalizedStartTime := time.Date(year, month, day, startTime.Hour(), startTime.Minute(), startTime.Second(), startTime.Nanosecond(), startTime.Location())
	normalizedEndTime := time.Date(year, month, day, endTime.Hour(), endTime.Minute(), endTime.Second(), endTime.Nanosecond(), endTime.Location())

	// Check if current time is within the start and end times
	return currentTime.After(normalizedStartTime) && currentTime.Before(normalizedEndTime)
}
