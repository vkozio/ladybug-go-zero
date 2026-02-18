package ladybug

import "context"

// Config holds options for opening a database.
// Zero value uses Ladybug defaults (see lbug_default_system_config).
type Config struct {
	// Path is the database path (directory). Required.
	Path string
	// ReadOnly opens the database in read-only mode.
	ReadOnly bool
	// BufferPoolSize is the buffer pool size in bytes (0 = default).
	BufferPoolSize uint64
	// MaxNumThreads is the max threads for query execution (0 = default).
	MaxNumThreads uint64
	// OnQueryFinished, if non-nil, is called after each Query or Execute.
	// Summary may be zero-valued if underlying support is unavailable.
	OnQueryFinished func(ctx context.Context, cypher string, summary QuerySummary, err error)
}
