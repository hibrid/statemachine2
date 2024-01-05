package statemachine

import "fmt"

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
