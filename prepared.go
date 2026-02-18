package ladybug

import (
	"context"
	"fmt"
	"time"

	"github.com/vkozio/ladybug-go-zero/internal/lbugc"
)

// PreparedStatement is a prepared Cypher statement. Call Close when done.
type PreparedStatement struct {
	c     *lbugc.PreparedStatement
	conn  *Connection
	query string
}

// Close destroys the prepared statement.
func (ps *PreparedStatement) Close() error {
	if ps == nil || ps.c == nil {
		return nil
	}
	ps.c.Close()
	ps.c = nil
	ps.conn = nil
	return nil
}

// BindBool binds a bool parameter.
func (ps *PreparedStatement) BindBool(name string, v bool) error {
	if ps == nil || ps.c == nil {
		return ErrClosed
	}
	if err := ps.c.BindBool(name, v); err != nil {
		return fmt.Errorf("ladybug: %w", err)
	}
	return nil
}

// BindInt64 binds an int64 parameter.
func (ps *PreparedStatement) BindInt64(name string, v int64) error {
	if ps == nil || ps.c == nil {
		return ErrClosed
	}
	if err := ps.c.BindInt64(name, v); err != nil {
		return fmt.Errorf("ladybug: %w", err)
	}
	return nil
}

// BindDouble binds a float64 parameter.
func (ps *PreparedStatement) BindDouble(name string, v float64) error {
	if ps == nil || ps.c == nil {
		return ErrClosed
	}
	if err := ps.c.BindDouble(name, v); err != nil {
		return fmt.Errorf("ladybug: %w", err)
	}
	return nil
}

// BindString binds a string parameter.
func (ps *PreparedStatement) BindString(name string, v string) error {
	if ps == nil || ps.c == nil {
		return ErrClosed
	}
	if err := ps.c.BindString(name, v); err != nil {
		return fmt.Errorf("ladybug: %w", err)
	}
	return nil
}

// BindDate binds a date parameter (midnight UTC) from time.Time.
func (ps *PreparedStatement) BindDate(name string, v time.Time) error {
	if ps == nil || ps.c == nil {
		return ErrClosed
	}
	if err := ps.c.BindDate(name, v); err != nil {
		return fmt.Errorf("ladybug: %w", err)
	}
	return nil
}

// BindTime binds a timestamp parameter with nanosecond precision.
func (ps *PreparedStatement) BindTime(name string, v time.Time) error {
	if ps == nil || ps.c == nil {
		return ErrClosed
	}
	if err := ps.c.BindTime(name, v); err != nil {
		return fmt.Errorf("ladybug: %w", err)
	}
	return nil
}

// BindInterval binds an interval parameter from time.Duration.
func (ps *PreparedStatement) BindInterval(name string, v time.Duration) error {
	if ps == nil || ps.c == nil {
		return ErrClosed
	}
	if err := ps.c.BindInterval(name, v); err != nil {
		return fmt.Errorf("ladybug: %w", err)
	}
	return nil
}

// BindUUID binds a UUID parameter as string.
func (ps *PreparedStatement) BindUUID(name string, v string) error {
	if ps == nil || ps.c == nil {
		return ErrClosed
	}
	if err := ps.c.BindUUID(name, v); err != nil {
		return fmt.Errorf("ladybug: %w", err)
	}
	return nil
}

// Execute runs the prepared statement and returns a Result. Caller must call Result.Close.
func (ps *PreparedStatement) Execute(ctx context.Context) (*Result, error) {
	if ps == nil || ps.c == nil {
		return nil, ErrClosed
	}
	if ps.conn == nil || ps.conn.c == nil {
		return nil, ErrInvalidConn
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err := ps.conn.setTimeoutFromContext(ctx); err != nil {
		return nil, err
	}

	done := make(chan struct{})
	if ctx != nil {
		go func() {
			select {
			case <-ctx.Done():
				ps.conn.Interrupt()
			case <-done:
			}
		}()
	}
	defer close(done)

	res, err := ps.c.Execute(ps.conn.c)
	if err != nil {
		wrapped := fmt.Errorf("ladybug: %w", err)
		invokeQueryHook(ps.conn.cfg, ctx, ps.query, QuerySummary{}, wrapped)
		return nil, wrapped
	}
	r := &Result{c: res}
	if ctx != nil && ctx.Err() != nil {
		r.Close()
		errCtx := ctx.Err()
		invokeQueryHook(ps.conn.cfg, ctx, ps.query, QuerySummary{}, errCtx)
		return nil, errCtx
	}

	var summary QuerySummary
	if s, err := r.Summary(); err == nil && s != nil {
		summary = *s
	}
	invokeQueryHook(ps.conn.cfg, ctx, ps.query, summary, nil)
	return r, nil
}
