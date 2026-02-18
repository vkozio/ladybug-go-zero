package ladybug

import (
	"context"
	"fmt"

	"github.com/vkozio/ladybug-go-zero/internal/lbugc"
)

// Database represents an open Ladybug database. Call Close when done.
type Database struct {
	c   *lbugc.Database
	cfg Config
}

// Open opens or creates a database at path. If opts is nil, path is used and other options are default.
// Compatible Ladybug version: see README (e.g. v0.14.2-bindings.0).
func Open(ctx context.Context, path string, opts *Config) (*Database, error) {
	if path == "" && (opts == nil || opts.Path == "") {
		return nil, fmt.Errorf("ladybug: path required")
	}
	var cfg Config
	if opts != nil {
		cfg = *opts
		if cfg.Path != "" {
			path = cfg.Path
		}
	}
	cfg.Path = path

	cDB, err := lbugc.Open(path)
	if err != nil {
		return nil, fmt.Errorf("ladybug: %w", err)
	}
	return &Database{c: cDB, cfg: cfg}, nil
}

// Close closes the database and releases resources.
func (db *Database) Close() error {
	if db == nil || db.c == nil {
		return nil
	}
	db.c.Close()
	db.c = nil
	return nil
}

// Conn returns a new connection. Caller must call Connection.Close.
// A Connection is not safe for concurrent use by multiple goroutines.
func (db *Database) Conn(ctx context.Context) (*Connection, error) {
	if db == nil || db.c == nil {
		return nil, ErrClosed
	}
	cConn, err := db.c.Conn()
	if err != nil {
		return nil, fmt.Errorf("ladybug: %w", err)
	}
	return &Connection{c: cConn, cfg: &db.cfg}, nil
}
