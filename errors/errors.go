package errors

import "errors"

var (
	ErrTaskNotRegistered = errors.New("task not registered")
	ErrBrokerUnavailable = errors.New("broker unavailable")
	ErrInvalidSchedule   = errors.New("invalid schedule expression")
	ErrAppRunning        = errors.New("cant update app while its running")
	ErrBrokerRequired    = errors.New("broker address required")
	ErrQueueNotFound     = errors.New("no queue matches the given task name, nor is a default queue set")
	ErrTaskNotFound      = errors.New("task not found")
)
