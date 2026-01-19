package core

import "errors"

var (
	ErrBackend5xx         = errors.New("backend service error")
	ErrBackend4xx         = errors.New("backend request rejected")
	ErrUnsupportProtocol  = errors.New("unsupported protocol")
	ErrNotMatchingBackend = errors.New("not matching backend Pod")
	ErrClientCancel       = errors.New("Clietn connection interrupted")
	ErrInvalidRequest     = errors.New("invalid request")
	ErrStreamUnsupport    = errors.New("streaming unsupport. can't using http flusher!")
)
