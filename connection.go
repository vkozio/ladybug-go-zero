package ladybug

import (
	"context"
	"fmt"
	"time"

	"github.com/vkozio/ladybug-go-zero/internal/lbugc"
)

// Connection is a connection to a Ladybug database. Call Close when done.
// Per lbug.h, the underlying C connection is thread-safe. However, Result iteration is not
// safe for concurrent use (consume a Result from one goroutine at a time).
type Connection struct {
	c   *lbugc.Connection
	cfg *Config
}

// Close closes the connection.
func (c *Connection) Close() error {
	if c == nil || c.c == nil {
		return nil
	}
	c.c.Close()
	c.c = nil
	return nil
}

// Query runs a Cypher query and returns a Result. Caller must call Result.Close.
func (c *Connection) Query(ctx context.Context, cypher string) (*Result, error) {
	if c == nil || c.c == nil {
		return nil, ErrInvalidConn
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err := c.setTimeoutFromContext(ctx); err != nil {
		return nil, err
	}

	done := make(chan struct{})
	if ctx != nil {
		go func() {
			select {
			case <-ctx.Done():
				c.Interrupt()
			case <-done:
			}
		}()
	}
	defer close(done)

	res, err := c.c.Query(cypher)
	if err != nil {
		wrapped := fmt.Errorf("ladybug: %w", err)
		invokeQueryHook(c.cfg, ctx, cypher, QuerySummary{}, wrapped)
		return nil, wrapped
	}
	r := &Result{c: res}
	if ctx != nil && ctx.Err() != nil {
		r.Close()
		errCtx := ctx.Err()
		invokeQueryHook(c.cfg, ctx, cypher, QuerySummary{}, errCtx)
		return nil, errCtx
	}

	var summary QuerySummary
	if s, err := r.Summary(); err == nil && s != nil {
		summary = *s
	}
	invokeQueryHook(c.cfg, ctx, cypher, summary, nil)
	return r, nil
}

// Prepare prepares a Cypher statement. Caller must call PreparedStatement.Close.
func (c *Connection) Prepare(ctx context.Context, cypher string) (*PreparedStatement, error) {
	if c == nil || c.c == nil {
		return nil, ErrInvalidConn
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err := c.setTimeoutFromContext(ctx); err != nil {
		return nil, err
	}

	done := make(chan struct{})
	if ctx != nil {
		go func() {
			select {
			case <-ctx.Done():
				c.Interrupt()
			case <-done:
			}
		}()
	}
	defer close(done)

	ps, err := c.c.Prepare(cypher)
	if err != nil {
		return nil, fmt.Errorf("ladybug: %w", err)
	}
	if ctx != nil && ctx.Err() != nil {
		ps.Close()
		return nil, ctx.Err()
	}
	return &PreparedStatement{c: ps, conn: c, query: cypher}, nil
}

// SetQueryTimeout sets the query timeout (0 = no timeout).
func (c *Connection) SetQueryTimeout(d time.Duration) {
	if c == nil || c.c == nil {
		return
	}
	_ = c.c.SetQueryTimeout(uint64(d.Milliseconds()))
}

// Interrupt interrupts the current query on this connection.
func (c *Connection) Interrupt() {
	if c == nil || c.c == nil {
		return
	}
	c.c.Interrupt()
}

func (c *Connection) setTimeoutFromContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil
	}
	ms := time.Until(deadline).Milliseconds()
	if ms < 0 {
		ms = 0
	}
	return c.c.SetQueryTimeout(uint64(ms))
}

func invokeQueryHook(cfg *Config, ctx context.Context, cypher string, summary QuerySummary, err error) {
	if cfg == nil || cfg.OnQueryFinished == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	cfg.OnQueryFinished(ctx, cypher, summary, err)
}
