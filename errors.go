package ladybug

import "errors"

var (
	// ErrClosed is returned when an operation is performed on a closed Database, Connection, Result, or PreparedStatement.
	ErrClosed = errors.New("ladybug: closed")
	// ErrInvalidConn is returned when the connection is invalid or closed.
	ErrInvalidConn = errors.New("ladybug: invalid connection")
)
