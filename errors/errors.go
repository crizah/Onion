package errors

import "errors"

var (
	ErrTaskNotRegistered = errors.New("task not registered")
	ErrBrokerUnavailable = errors.New("broker unavailable")
	ErrInvalidSchedule   = errors.New("invalid schedule expression")
	ErrAppRunning        = errors.New("cant update app while its running")
	ErrBrokerRequired    = errors.New("broker address required")
)
