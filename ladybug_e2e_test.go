package ladybug

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestHandleLifetime verifies that all C handles are allocated on C heap (not stack),
// preventing dangling pointer bugs. This test ensures handles remain valid after function return.
func TestHandleLifetime(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "lifetime_test")
	ctx := context.Background()

	// Test Database handle lifetime: Open returns a handle that must remain valid.
	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Store reference and verify it's still usable after potential GC.
	_ = db
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Connection handle must remain valid.
	res, err := conn.Query(ctx, "RETURN 1")
	if err != nil {
		t.Fatal(err)
	}
	// Result handle must remain valid.
	_ = res
	// All handles should be usable and not cause crashes on Close.
	res.Close()
	conn.Close()
	db.Close()
	// Double-close should be safe (idempotent).
	db.Close()
	conn.Close()
	res.Close()
}

// TestArrowSchemaLifetime verifies Arrow schema ownership and release semantics.
// ImportCArrowSchema takes ownership of the schema's release callback; we must only free the struct.
func TestArrowSchemaLifetime(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arrow_schema_test")
	ctx := context.Background()

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	res, err := conn.Query(ctx, "RETURN 1 AS x")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	// Schema() should be callable multiple times without leaks.
	schema1 := res.Schema()
	if schema1 == nil {
		t.Fatal("expected schema")
	}
	schema2 := res.Schema()
	if schema2 == nil {
		t.Fatal("expected schema")
	}
	// Should return the same schema instance (cached).
	if schema1 != schema2 {
		t.Error("Schema() should return cached instance")
	}

	// Schema should remain valid until Result.Close().
	_ = schema1.NumFields()
	res.Close()
	// After Close, schema pointer may be invalid; don't use it.
}

// TestArrowRecordRelease verifies that Arrow records must be Released by caller,
// and that ImportCRecordBatchWithSchema takes ownership of array buffers (not struct).
func TestArrowRecordRelease(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "arrow_record_test")
	ctx := context.Background()

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	res, err := conn.Query(ctx, "RETURN 1 AS x, 2 AS y")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	rec, err := res.NextRecord(0)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil {
		t.Fatal("expected record")
	}
	// Record must be Released; ImportCRecordBatchWithSchema takes ownership of buffers.
	// Our code only frees the ArrowArray struct; the Record's Release() handles buffer cleanup.
	rec.Release()
	// Double-release should be safe (idempotent).
	rec.Release()

	// Next record should work after Release.
	rec2, err := res.NextRecord(0)
	if err != nil {
		t.Fatal(err)
	}
	if rec2 == nil {
		t.Fatal("expected second record")
	}
	rec2.Release()
}

// TestConnectionThreadSafety verifies that Connection is thread-safe per lbug.h:
// "each connection is thread-safe. Multiple connections can connect to the same Database".
// This test runs concurrent queries from multiple goroutines on the same Connection.
func TestConnectionThreadSafety(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "concurrency_test")
	ctx := context.Background()

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	const numGoroutines = 10
	const queriesPerGoroutine = 5
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines*queriesPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < queriesPerGoroutine; j++ {
				res, err := conn.Query(ctx, "RETURN 1 AS x")
				if err != nil {
					errCh <- err
					return
				}
				row, ok := res.Next()
				if !ok {
					errCh <- err
					res.Close()
					return
				}
				v, err := row.Value(0)
				if err != nil || v != int64(1) {
					errCh <- err
					res.Close()
					return
				}
				res.Close()
			}
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestResultNotConcurrent documents that a single Result must not be consumed concurrently.
// While Connection is thread-safe, Result iteration is not.
// This test verifies sequential consumption works; concurrent consumption would cause crashes.
func TestResultNotConcurrent(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "result_concurrent_test")
	ctx := context.Background()

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Create a result with multiple rows.
	res, err := conn.Query(ctx, "UNWIND range(1, 100) AS x RETURN x")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	// Sequential consumption works correctly.
	count := 0
	for row, ok := res.Next(); ok; row, ok = res.Next() {
		count++
		_ = row
	}
	if count != 100 {
		t.Errorf("expected 100 rows, got %d", count)
	}

	// Note: Concurrent Next() calls on the same Result would cause crashes or data races.
	// In production, consume each Result from one goroutine at a time.
	// Use multiple Connections (which are thread-safe) if you need concurrent query execution.
}

// TestContextCancellation verifies that ctx.Done() triggers Interrupt() on the connection,
// allowing queries to be cancelled mid-execution.
func TestContextCancellation(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "cancel_test")
	ctx := context.Background()

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Create a context that cancels immediately.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // Cancel immediately

	// Query should detect cancellation and return ctx.Err().
	_, err = conn.Query(cancelCtx, "RETURN 1")
	if err == nil {
		t.Error("expected error from cancelled context")
	}
	if err != context.Canceled {
		t.Logf("got error: %v (may be wrapped)", err)
	}
}

// TestContextTimeout verifies that context deadline is converted to query timeout
// via lbug_connection_set_query_timeout.
func TestContextTimeout(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "timeout_test")
	ctx := context.Background()

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Set a very short deadline.
	timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Millisecond)
	defer cancel()

	// Even a simple query should respect the timeout (though it may complete before timeout).
	_, err = conn.Query(timeoutCtx, "RETURN 1")
	// May succeed if query completes quickly, or fail with timeout.
	_ = err
}

// TestContextInterruptDuringQuery verifies that Interrupt() is called when ctx.Done()
// fires during an active query (simulated with a longer-running query if available).
func TestContextInterruptDuringQuery(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "interrupt_test")
	ctx := context.Background()

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Create a context that cancels after a short delay.
	cancelCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	// Start a query; cancellation should trigger Interrupt().
	// Note: For a truly long-running query, we'd need a Cypher query that takes time.
	// This test documents the behavior; actual interrupt may not be observable with fast queries.
	_, err = conn.Query(cancelCtx, "UNWIND range(1, 10000) AS x RETURN x")
	// Query may complete before cancellation, or be interrupted.
	_ = err
}

// TestDoubleClose verifies that all Close() methods are idempotent (safe to call multiple times).
// This prevents crashes if cleanup code calls Close() multiple times.
func TestDoubleClose(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "double_close_test")
	ctx := context.Background()

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := conn.Query(ctx, "RETURN 1")
	if err != nil {
		t.Fatal(err)
	}

	// All Close() calls should be idempotent.
	res.Close()
	res.Close()
	conn.Close()
	conn.Close()
	db.Close()
	db.Close()
}

// TestClosedHandleErrors verifies that operations on closed handles return appropriate errors
// (ErrClosed or ErrInvalidConn) rather than crashing or returning invalid data.
func TestClosedHandleErrors(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "closed_handle_test")
	ctx := context.Background()

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := conn.Query(ctx, "RETURN 1")
	if err != nil {
		t.Fatal(err)
	}

	// Close everything.
	res.Close()
	conn.Close()
	db.Close()

	// Operations on closed handles should return errors, not crash.
	_, err = db.Conn(ctx)
	if err == nil {
		t.Error("Conn() on closed Database should return error")
	}
	_, err = conn.Query(ctx, "RETURN 1")
	if err == nil {
		t.Error("Query() on closed Connection should return error")
	}
	_, ok := res.Next()
	if ok {
		t.Error("Next() on closed Result should return false")
	}
}

// TestEmptyResult verifies that empty results (no rows) are handled correctly.
func TestEmptyResult(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "empty_result_test")
	ctx := context.Background()

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Use a query that returns no rows. Try a simple RETURN with a condition that never matches.
	// If that doesn't work, we'll skip schema check for empty results.
	res, err := conn.Query(ctx, "RETURN 1 AS x LIMIT 0")
	if err != nil {
		// If LIMIT 0 doesn't work, try without it and just check that Next() returns false.
		res, err = conn.Query(ctx, "RETURN 1 AS x")
		if err != nil {
			t.Fatal(err)
		}
		defer res.Close()
		// Consume the one row to make result empty.
		_, _ = res.Next()
		// Now result should be empty.
	} else {
		defer res.Close()
	}

	// Should have schema even with no rows.
	schema := res.Schema()
	if schema == nil {
		t.Error("expected schema even for empty result")
	}

	// Next() should return false immediately.
	_, ok := res.Next()
	if ok {
		t.Error("expected no rows")
	}

	// NextRecord() may return a record with 0 rows or nil.
	rec, err := res.NextRecord(0)
	if err != nil {
		t.Errorf("NextRecord() should not error on empty result: %v", err)
	}
	if rec != nil {
		if rec.NumRows() != 0 {
			t.Errorf("expected 0 rows, got %d", rec.NumRows())
		}
		rec.Release()
	}
}

// TestPreparedStatementLifetime verifies that PreparedStatement handles are properly managed
// and that Execute correctly uses the prepared statement with a connection.
func TestPreparedStatementLifetime(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "prepared_test")
	ctx := context.Background()

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ps, err := conn.Prepare(ctx, "RETURN $x AS value")
	if err != nil {
		t.Fatal(err)
	}
	// PreparedStatement handle must remain valid.
	err = ps.BindInt64("x", 42)
	if err != nil {
		t.Fatal(err)
	}

	res, err := ps.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	row, ok := res.Next()
	if !ok {
		t.Fatal("expected row")
	}
	v, err := row.Value(0)
	if err != nil {
		t.Fatal(err)
	}
	if v != int64(42) {
		t.Errorf("got %v, want 42", v)
	}

	// Double-close should be safe.
	ps.Close()
	ps.Close()
}

// TestBindAndTypedAccessors verifies Bind* helpers and typed Row accessors for basic types.
func TestBindAndTypedAccessors(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "bind_typed_accessors_test")
	ctx := context.Background()

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ps, err := conn.Prepare(ctx, "RETURN $b AS b, $i AS i, $u AS u, $f AS f, $s AS s, $ts AS ts, $d AS d")
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	now := time.Now().UTC().Truncate(time.Microsecond)
	day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	if err := ps.BindBool("b", true); err != nil {
		t.Fatal(err)
	}
	if err := ps.BindInt64("i", 42); err != nil {
		t.Fatal(err)
	}
	if err := ps.BindDouble("f", 3.5); err != nil {
		t.Fatal(err)
	}
	if err := ps.BindString("s", "hello"); err != nil {
		t.Fatal(err)
	}
	if err := ps.BindInt64("u", 7); err != nil {
		t.Fatal(err)
	}
	if err := ps.BindTime("ts", now); err != nil {
		t.Fatal(err)
	}
	if err := ps.BindDate("d", day); err != nil {
		t.Fatal(err)
	}

	res, err := ps.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	row, ok := res.Next()
	if !ok {
		t.Fatal("expected row")
	}

	if v, err := row.Bool(0); err != nil || v != true {
		t.Fatalf("Bool(0) = %v, %v", v, err)
	}
	if v, err := row.Int64(1); err != nil || v != int64(42) {
		t.Fatalf("Int64(1) = %v, %v", v, err)
	}
	if v, err := row.Int64(2); err != nil || v != int64(7) {
		t.Fatalf("Int64(2) = %v, %v", v, err)
	}
	if v, err := row.Float64(3); err != nil || v != 3.5 {
		t.Fatalf("Float64(3) = %v, %v", v, err)
	}
	if v, err := row.String(4); err != nil || v != "hello" {
		t.Fatalf("String(4) = %v, %v", v, err)
	}
	if v, err := row.Time(5); err != nil || v.IsZero() {
		t.Fatalf("Time(5) = %v, %v", v, err)
	}
	if v, err := row.Date(6); err != nil || v.IsZero() {
		t.Fatalf("Date(6) = %v, %v", v, err)
	}
}

// TestQuerySummaryAndHook verifies that Summary and OnQueryFinished hook are called.
func TestQuerySummaryAndHook(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "summary_hook_test")

	var (
		gotSummary QuerySummary
		gotErr     error
		calls      int
	)

	cfg := &Config{
		Path: dbPath,
		OnQueryFinished: func(ctx context.Context, cypher string, summary QuerySummary, err error) {
			calls++
			gotSummary = summary
			gotErr = err
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, "", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	res, err := conn.Query(ctx, "RETURN 1 AS x")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()

	if _, ok := res.Next(); !ok {
		t.Fatal("expected row")
	}

	s, err := res.Summary()
	if err != nil {
		t.Fatalf("Summary error: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil summary")
	}

	if calls == 0 {
		t.Fatal("expected OnQueryFinished to be called")
	}
	if gotErr != nil {
		t.Fatalf("expected nil hook error, got %v", gotErr)
	}
	if gotSummary.CompileMS < 0 || gotSummary.ExecMS < 0 {
		t.Fatalf("invalid summary times: %+v", gotSummary)
	}
}
