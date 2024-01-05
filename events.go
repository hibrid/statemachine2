package statemachine

import "fmt"

type ForwardEvent int

const (
	ForwardSuccess ForwardEvent = iota
	ForwardFail
	ForwardComplete
	ForwardPause
	ForwardRollback
	ForwardRetry
	ForwardLock
	ForwardCancel
	ForwardUnknown
)

func (e ForwardEvent) String() string {
	switch e {
	case ForwardSuccess:
		return "ForwardSuccess"
	case ForwardFail:
		return "ForwardFail"
	case ForwardComplete:
		return "ForwardComplete"
	case ForwardPause:
		return "ForwardPause"
	case ForwardRollback:
		return "ForwardRollback"
	case ForwardRetry:
		return "ForwardRetry"
	case ForwardLock:
		return "ForwardLock"
	case ForwardCancel:
		return "ForwardCancel"
	default:
		return fmt.Sprintf("UnknownForwardEvent(%d)", e)
	}
}

// convert the ForwardEvent to a Event
func (e ForwardEvent) ToEvent() Event {
	switch e {
	case ForwardSuccess:
		return OnSuccess
	case ForwardFail:
		return OnFailed
	case ForwardComplete:
		return OnCompleted
	case ForwardPause:
		return OnPause
	case ForwardRollback:
		return OnRollback
	case ForwardRetry:
		return OnRetry
	case ForwardLock:
		return OnLock
	case ForwardCancel:
		return OnCancelled
	default:
		return OnUnknownSituation
	}
}

type BackwardEvent int

const (
	BackwardSuccess BackwardEvent = iota
	BackwardComplete
	BackwardFail
	BackwardRollback
	BackwardCancel
	BackwardRetry
	BackwardUnknown
)

func (e BackwardEvent) String() string {
	switch e {
	case BackwardSuccess:
		return "BackwardSuccess"
	case BackwardComplete:
		return "BackwardComplete"
	case BackwardFail:
		return "BackwardFail"
	case BackwardRollback:
		return "BackwardRollback"
	case BackwardCancel:
		return "BackwardCancel"
	case BackwardRetry:
		return "BackwardRetry"
	default:
		return fmt.Sprintf("UnknownBackwardEvent(%d)", e)
	}
}

// convert the BackwardEvent to a Event
func (e BackwardEvent) ToEvent() Event {
	switch e {
	case BackwardSuccess:
		return OnRollback
	case BackwardComplete:
		return OnRollbackCompleted
	case BackwardFail:
		return OnFailed
	case BackwardRollback:
		return OnRollback
	case BackwardCancel:
		return OnCancelled
	case BackwardRetry:
		return OnRetry
	default:
		return OnUnknownSituation
	}
}

type PauseEvent int

const (
	PauseSuccess PauseEvent = iota
	PauseFail
	PauseUnknown
)

func (e PauseEvent) String() string {
	switch e {
	case PauseSuccess:
		return "PauseSuccess"
	case PauseFail:
		return "PauseFail"
	default:
		return fmt.Sprintf("UnknownPauseEvent(%d)", e)
	}
}

// convert the PauseEvent to a Event
func (e PauseEvent) ToEvent() Event {
	switch e {
	case PauseSuccess:
		return OnPause
	case PauseFail:
		return OnFailed
	default:
		return OnUnknownSituation
	}
}

type RetryEvent int

const (
	RetrySuccess RetryEvent = iota
	RetryFail
	RetryRetry
	RetryLock
	RetryCancel
	RetryResetTimeout
	RetryUnknown
)

func (e RetryEvent) String() string {
	switch e {
	case RetrySuccess:
		return "RetrySuccess"
	case RetryFail:
		return "RetryFail"
	case RetryRetry:
		return "RetryRetry"
	case RetryLock:
		return "RetryLock"
	case RetryCancel:
		return "RetryCancel"
	case RetryResetTimeout:
		return "RetryResetTimeout"
	default:
		return fmt.Sprintf("UnknownRetryEvent(%d)", e)
	}
}

// convert the RetryEvent to a Event
func (e RetryEvent) ToEvent() Event {
	switch e {
	case RetrySuccess:
		return OnSuccess
	case RetryFail:
		return OnFailed
	case RetryRetry:
		return OnRetry
	case RetryLock:
		return OnLock
	case RetryCancel:
		return OnCancelled
	case RetryResetTimeout:
		return OnResetTimeout
	default:
		return OnUnknownSituation
	}
}

type ManualOverrideEvent int

const (
	ManualOverrideAny ManualOverrideEvent = iota
	ManualOverrideUnknown
)

func (e ManualOverrideEvent) String() string {
	switch e {
	case ManualOverrideAny:
		return "ManualOverrideAny"
	default:
		return fmt.Sprintf("UnknownManualOverrideEvent(%d)", e)
	}
}

// convert the ManualOverrideEvent to a Event
func (e ManualOverrideEvent) ToEvent() Event {
	switch e {
	case ManualOverrideAny:
		return OnManualOverride
	default:
		return OnUnknownSituation
	}
}

type ResumeEvent int

const (
	ResumeSuccess ResumeEvent = iota
	ResumeFail
)

func (e ResumeEvent) String() string {
	switch e {
	case ResumeSuccess:
		return "ResumeSuccess"
	case ResumeFail:
		return "ResumeFail"
	default:
		return fmt.Sprintf("UnknownResumeEvent(%d)", e)
	}
}

// convert the ResumeEvent to a Event
func (e ResumeEvent) ToEvent() Event {
	switch e {
	case ResumeSuccess:
		return OnSuccess
	case ResumeFail:
		return OnFailed
	default:
		return OnUnknownSituation
	}
}

// Event represents an event that triggers state transitions.
type Event int

const (
	OnSuccess Event = iota
	OnFailed
	OnAlreadyCompleted
	OnPause
	OnResume
	OnRollback
	OnRetry
	OnResetTimeout
	OnUnknownSituation
	OnManualOverride
	OnError
	OnBeforeEvent
	OnAfterEvent
	OnCompleted
	OnRollbackCompleted
	OnAlreadyRollbackCompleted
	OnRollbackFailed
	OnCancelled
	OnParked
	OnLock
	OnWakeUp
)

func (e Event) String() string {
	switch e {
	case OnSuccess:
		return "OnSuccess"
	case OnFailed:
		return "OnFailed"
	case OnAlreadyCompleted:
		return "OnAlreadyCompleted"
	case OnPause:
		return "OnPause"
	case OnResume:
		return "OnResume"
	case OnRollback:
		return "OnRollback"
	case OnRetry:
		return "OnRetry"
	case OnResetTimeout:
		return "OnResetTimeout"
	case OnUnknownSituation:
		return "OnUnknownSituation"
	case OnManualOverride:
		return "OnManualOverride"
	case OnError:
		return "OnError"
	case OnBeforeEvent:
		return "OnBeforeEvent"
	case OnAfterEvent:
		return "OnAfterEvent"
	case OnCompleted:
		return "OnCompleted"
	case OnRollbackCompleted:
		return "OnRollbackCompleted"
	case OnAlreadyRollbackCompleted:
		return "OnAlreadyRollbackCompleted"
	case OnRollbackFailed:
		return "OnRollbackFailed"
	case OnCancelled:
		return "OnCancelled"
	case OnParked:
		return "OnParked"
	case OnLock:
		return "OnLock"
	default:
		return fmt.Sprintf("UnknownEvent(%d)", e)
	}
}
