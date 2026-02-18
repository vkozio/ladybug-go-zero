# Test Contract Documentation

This document describes what the E2E tests (`ladybug_e2e_test.go`) verify and what guarantees they enforce.

## Purpose

These tests serve two purposes:
1. **Regression prevention**: Catch bugs we've fixed (dangling pointers, memory leaks, incorrect ownership).
2. **Contract documentation**: Explicitly state what the API guarantees and what it does not.

## Test Coverage

### Memory and Lifetime

- **TestHandleLifetime**: Verifies all C handles (Database, Connection, Result) are allocated on C heap, not stack. Prevents dangling pointer bugs where handles become invalid after function return.

- **TestDoubleClose**: Ensures all `Close()` methods are idempotent (safe to call multiple times). Prevents crashes in cleanup code that may call Close() multiple times.

- **TestClosedHandleErrors**: Verifies operations on closed handles return errors (`ErrClosed`, `ErrInvalidConn`) rather than crashing or returning invalid data.

### Arrow Ownership

- **TestArrowSchemaLifetime**: Documents that `ImportCArrowSchema` takes ownership of the schema's release callback. We only free the ArrowSchema struct allocation; the schema's Release() is handled by arrow-go.

- **TestArrowRecordRelease**: Documents that `ImportCRecordBatchWithSchema` takes ownership of array buffers via ArrowArrayMove. We only free the ArrowArray struct; the Record's Release() handles buffer cleanup. Caller must call `Record.Release()`.

### Concurrency

- **TestConnectionThreadSafety**: Verifies that Connection is thread-safe per `lbug.h` ("each connection is thread-safe"). Multiple goroutines can safely call Query/Execute on the same Connection concurrently.

- **TestResultNotConcurrent**: Documents that a single Result must NOT be consumed concurrently. While Connection is thread-safe, Result iteration is not. The test verifies sequential consumption works; concurrent Next() calls would cause crashes. Consume each Result from one goroutine at a time.

### Context and Cancellation

- **TestContextCancellation**: Verifies that `ctx.Done()` triggers `Interrupt()` on the connection, allowing queries to be cancelled mid-execution.

- **TestContextTimeout**: Verifies that context deadline is converted to query timeout via `lbug_connection_set_query_timeout`.

- **TestContextInterruptDuringQuery**: Documents that Interrupt() is called when ctx.Done() fires during an active query.

### Edge Cases

- **TestEmptyResult**: Verifies empty results (no rows) are handled correctly. Schema should be available even with no rows. Next() and NextRecord() should return false/nil without errors.

- **TestPreparedStatementLifetime**: Verifies PreparedStatement handles are properly managed and Execute correctly uses the prepared statement with a connection.

## Running Tests

```bash
go test ./... -v
```

To run with race detector (recommended for concurrency tests):
```bash
go test ./... -race -v
```

## Notes

- Tests skip if Ladybug libs are not available (no `include/lbug.h` or `lib/dynamic/<platform>`).
- Some tests document behavior that may not be directly observable (e.g., interrupt during very fast queries).
- These tests complement unit tests in `ladybug_test.go` which test basic functionality.
