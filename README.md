# ladybug-go-zero

Unofficial Go driver for [Ladybug](https://github.com/LadybugDB/ladybug) graph database. **Cypher** query language, zero-copy Arrow, C API only at the boundary.

## Compatible Ladybug version

Prebuilt C libraries and header for this driver are taken from the fork [vkozio/ladybug](https://github.com/vkozio/ladybug) (releases built from master). Document the exact release tag you use (e.g. v0.14.2-bindings.0).

## Obtaining the library and header

From the repo root:

```bash
./scripts/download_liblbug.sh latest darwin .
```

- **version:** Release tag (e.g. v0.14.2-bindings.0) or `latest`
- **platform:** linux-amd64 | linux-arm64 | darwin | windows-amd64
- **output_dir:** Default `.`; script creates `lib/dynamic/<platform>/` and `include/`

The script uses https://github.com/vkozio/ladybug/releases by default. To use upstream: `LADYBUG_REPO=LadybugDB/ladybug ./scripts/download_liblbug.sh ...`

Requires: curl, tar (for .tar.gz), unzip (for Windows).

## Build

After libs and header are in place:

```bash
go build ./...
```

Module: `github.com/vkozio/ladybug-go-zero`. Public API: package `ladybug`.

### Version

```go
import "github.com/vkozio/ladybug-go-zero"

ver, storageVer := ladybug.Version()
```

### Open, Cypher Query, Row iteration

```go
ctx := context.Background()
db, err := ladybug.Open(ctx, "/path/to/db", nil)
if err != nil { ... }
defer db.Close()

conn, err := db.Conn(ctx)
if err != nil { ... }
defer conn.Close()

res, err := conn.Query(ctx, "RETURN 1 AS x, 'hello' AS y")
if err != nil { ... }
defer res.Close()

for row, ok := res.Next(); ok; row, ok = res.Next() {
    x, _ := row.Int64(0)
    y, _ := row.String(1)
    // x == 1, y == "hello"
}
```

### Arrow (zero-copy) iteration

```go
res, err := conn.Query(ctx, "MATCH (n) RETURN n LIMIT 10000")
defer res.Close()
for rec, err := res.NextRecord(64*1024); err == nil && rec != nil; rec, err = res.NextRecord(64*1024) {
    defer rec.Release()  // caller must call Release() when done with the record
    // use rec.Schema(), rec.Column(i), rec.NumRows()
}
```

Per lbug.h, the underlying C connection is thread-safe. Consume a single Result from one goroutine at a time.

### Prepared statements, temporal types, and summaries

```go
ps, err := conn.Prepare(ctx, "RETURN $ts AS ts, $d AS d")
if err != nil { ... }
defer ps.Close()

now := time.Now().UTC()
day := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

_ = ps.BindTime("ts", now)
_ = ps.BindDate("d", day)

res, err := ps.Execute(ctx)
if err != nil { ... }
defer res.Close()

row, ok := res.Next()
if !ok { ... }

ts, _ := row.Time(0)
d, _ := row.Date(1)

summary, _ := res.Summary()
_ = summary // contains compile and execution time in milliseconds
```

You can attach a lightweight metrics/tracing hook via Config:

```go
cfg := &ladybug.Config{
    Path: "/path/to/db",
    OnQueryFinished: func(ctx context.Context, cypher string, s ladybug.QuerySummary, err error) {
        // record metrics; must be lightweight and non-panicking
    },
}
db, err := ladybug.Open(ctx, "", cfg)
```

## Layout

- `internal/lbugc` — CGO layer (only package with import "C"); thin wrappers over lbug.h.
- Root package `ladybug` — public API (Open, Database, Conn, Query, Result with Arrow/row, Prepare, Version).

## Examples

- `examples/basic` — minimal program showing Open, Query, typed Row accessors, Scan, and OnQueryFinished hook.

## Links

- Ladybug upstream: https://github.com/LadybugDB/ladybug  
- Prebuilt bindings (this driver): https://github.com/vkozio/ladybug/releases
