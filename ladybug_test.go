package ladybug

import (
	"context"
	"path/filepath"
	"testing"
)

func TestVersion(t *testing.T) {
	ver, storageVer := Version()
	if ver == "" && storageVer == 0 {
		t.Skip("Ladybug libs not available (run scripts/download_liblbug.sh); skipping")
	}
	if ver != "" {
		t.Logf("version=%s storage_version=%d", ver, storageVer)
	}
}

func TestOpenQueryRow(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb")
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
	res, err := conn.Query(ctx, "RETURN 1 AS x, 'hello' AS y")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	row, ok := res.Next()
	if !ok {
		t.Fatal("expected one row")
	}
	vx, err := row.Value(0)
	if err != nil {
		t.Fatal(err)
	}
	if vx != int64(1) {
		t.Errorf("x = %v, want 1", vx)
	}
	vy, err := row.Value(1)
	if err != nil {
		t.Fatal(err)
	}
	if vy != "hello" {
		t.Errorf("y = %v, want hello", vy)
	}
	if _, ok := res.Next(); ok {
		t.Error("expected no second row")
	}
}

func TestOpenQueryArrow(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "testdb2")
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
	res, err := conn.Query(ctx, "RETURN 42 AS n")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	rec, err := res.NextRecord(0)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil {
		t.Fatal("expected one record")
	}
	defer rec.Release()
	if rec.NumRows() != 1 {
		t.Errorf("NumRows() = %d, want 1", rec.NumRows())
	}
}

func TestOpenInvalidPath(t *testing.T) {
	ver, _ := Version()
	if ver == "" {
		t.Skip("Ladybug libs not available; skipping")
	}
	ctx := context.Background()
	_, err := Open(ctx, "", nil)
	if err == nil {
		t.Error("Open with empty path should fail")
	}
}
