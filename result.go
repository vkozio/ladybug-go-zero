package ladybug

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/vkozio/ladybug-go-zero/internal/lbugc"
)

// DefaultArrowChunkSize is the default number of rows per Arrow record (64k).
const DefaultArrowChunkSize = 64 * 1024

// Result holds the result of a Cypher query. Call Close when done.
type Result struct {
	c       *lbugc.Result
	schema  *arrow.Schema
	lastRow *lbugc.Row
}

// Close releases the result and any Arrow schema. Call after consuming rows/records.
func (r *Result) Close() error {
	if r == nil || r.c == nil {
		return nil
	}
	if r.lastRow != nil {
		r.lastRow.Release()
		r.lastRow = nil
	}
	r.c.Close()
	r.c = nil
	r.schema = nil
	return nil
}

// Err returns the query error if the result indicates failure (e.g. after Query).
func (r *Result) Err() error {
	if r == nil || r.c == nil {
		return nil
	}
	// Result was already checked for success in Query; no per-result error message exposed here.
	return nil
}

// QuerySummary contains basic timing information for a query.
type QuerySummary struct {
	CompileMS float64
	ExecMS    float64
}

// Summary returns the query summary (compile and execution time in milliseconds), if available.
func (r *Result) Summary() (*QuerySummary, error) {
	if r == nil || r.c == nil {
		return nil, nil
	}
	compile, exec, err := r.c.Summary()
	if err != nil {
		return nil, fmt.Errorf("ladybug: %w", err)
	}
	return &QuerySummary{
		CompileMS: compile,
		ExecMS:    exec,
	}, nil
}

// Schema returns the Arrow schema. Valid until Result.Close(); do not retain after Close.
// Returns nil if the schema could not be obtained.
func (r *Result) Schema() *arrow.Schema {
	if r == nil || r.c == nil {
		return nil
	}
	if r.schema != nil {
		return r.schema
	}
	schemaPtr, cleanup, releaseAndFree := r.c.SchemaRaw()
	if schemaPtr == nil {
		return nil
	}
	// ImportCArrowSchema calls ArrowSchemaRelease on the schema; do not call release() on success.
	sc, err := cdata.ImportCArrowSchema((*cdata.CArrowSchema)(schemaPtr))
	if err != nil {
		releaseAndFree()
		return nil
	}
	cleanup()
	r.schema = sc
	return r.schema
}

// NextRecord returns the next Arrow record batch (chunkSize rows; 0 = DefaultArrowChunkSize).
// Caller must call record.Release() when done.
// Returns (nil, nil) when there are no more records.
func (r *Result) NextRecord(chunkSize int64) (arrow.Record, error) {
	if r == nil || r.c == nil {
		return nil, nil
	}
	if chunkSize <= 0 {
		chunkSize = DefaultArrowChunkSize
	}
	sc := r.Schema()
	if sc == nil {
		return nil, fmt.Errorf("ladybug: no schema")
	}
	arrayPtr, cleanup, releaseAndFree, err := r.c.NextChunkRaw(chunkSize)
	if err != nil {
		return nil, fmt.Errorf("ladybug: %w", err)
	}
	if arrayPtr == nil {
		return nil, nil
	}
	// ImportCRecordBatchWithSchema takes ownership of the array buffers via ArrowArrayMove.
	// We still must free the ArrowArray struct allocation (cleanup). On import error, call releaseAndFree.
	rec, err := cdata.ImportCRecordBatchWithSchema((*cdata.CArrowArray)(arrayPtr), sc)
	if err != nil {
		releaseAndFree()
		return nil, fmt.Errorf("ladybug: %w", err)
	}
	cleanup()
	return rec, nil
}

// Next returns the next row. The returned Row is valid until the next call to Next or Close.
// Arrow iteration is preferred for bulk; use NextRecord for better performance.
func (r *Result) Next() (Row, bool) {
	if r == nil || r.c == nil {
		return Row{}, false
	}
	if r.lastRow != nil {
		r.lastRow.Release()
		r.lastRow = nil
	}
	row, ok, _ := r.c.GetNext()
	if !ok || row == nil {
		return Row{}, false
	}
	r.lastRow = row
	return Row{c: row, numCols: r.c.NumColumns()}, true
}

// Row represents one result row. Do not retain; only use until next Next() or Result.Close().
type Row struct {
	c       *lbugc.Row
	numCols uint64
}

// Value returns the value at column index (0-based). Returns nil for NULL.
func (row Row) Value(index uint64) (interface{}, error) {
	if row.c == nil {
		return nil, ErrClosed
	}
	return row.c.Value(index)
}

// NumColumns returns the number of columns in this row.
func (row Row) NumColumns() uint64 {
	return row.numCols
}

// Bool returns the bool value at column index.
func (row Row) Bool(index int) (bool, error) {
	v, err := row.Value(uint64(index))
	if err != nil {
		return false, err
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("ladybug: column %d is not bool (got %T)", index, v)
	}
	return b, nil
}

// Int64 returns the int64 value at column index.
func (row Row) Int64(index int) (int64, error) {
	v, err := row.Value(uint64(index))
	if err != nil {
		return 0, err
	}
	i, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("ladybug: column %d is not int64 (got %T)", index, v)
	}
	return i, nil
}

// UInt64 returns the uint64 value at column index.
func (row Row) UInt64(index int) (uint64, error) {
	v, err := row.Value(uint64(index))
	if err != nil {
		return 0, err
	}
	u, ok := v.(uint64)
	if !ok {
		return 0, fmt.Errorf("ladybug: column %d is not uint64 (got %T)", index, v)
	}
	return u, nil
}

// Float64 returns the float64 value at column index.
func (row Row) Float64(index int) (float64, error) {
	v, err := row.Value(uint64(index))
	if err != nil {
		return 0, err
	}
	f, ok := v.(float64)
	if !ok {
		return 0, fmt.Errorf("ladybug: column %d is not float64 (got %T)", index, v)
	}
	return f, nil
}

// String returns the string value at column index.
func (row Row) String(index int) (string, error) {
	v, err := row.Value(uint64(index))
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("ladybug: column %d is not string (got %T)", index, v)
	}
	return s, nil
}

// Bytes returns the []byte value at column index.
func (row Row) Bytes(index int) ([]byte, error) {
	v, err := row.Value(uint64(index))
	if err != nil {
		return nil, err
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("ladybug: column %d is not []byte (got %T)", index, v)
	}
	return b, nil
}

// Time returns the time.Time value at column index (for timestamps and dates).
func (row Row) Time(index int) (time.Time, error) {
	v, err := row.Value(uint64(index))
	if err != nil {
		return time.Time{}, err
	}
	t, ok := v.(time.Time)
	if !ok {
		return time.Time{}, fmt.Errorf("ladybug: column %d is not time.Time (got %T)", index, v)
	}
	return t, nil
}

// Date is an alias for Time, provided for clarity when working with DATE columns.
func (row Row) Date(index int) (time.Time, error) {
	return row.Time(index)
}

// UUID returns the UUID value at column index as string.
func (row Row) UUID(index int) (string, error) {
	return row.String(index)
}

// Node represents a graph node value.
type Node struct {
	ID         any
	Labels     []string
	Properties map[string]any
}

// Rel represents a graph relationship value.
type Rel struct {
	ID         any
	SrcID      any
	DstID      any
	Label      string
	Properties map[string]any
}

// Property returns the property value by name and whether it was present.
func (n Node) Property(name string) (any, bool) {
	if n.Properties == nil {
		return nil, false
	}
	v, ok := n.Properties[name]
	return v, ok
}

// Property returns the relationship property value by name and whether it was present.
func (r Rel) Property(name string) (any, bool) {
	if r.Properties == nil {
		return nil, false
	}
	v, ok := r.Properties[name]
	return v, ok
}

// AsNode attempts to interpret v as a Node returned by the driver.
func AsNode(v any) (Node, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return Node{}, false
	}
	id, _ := m["id"]
	rawLabels, _ := m["labels"]
	propsAny, _ := m["properties"]

	var labels []string
	switch l := rawLabels.(type) {
	case []string:
		labels = l
	case []any:
		for _, el := range l {
			if s, ok := el.(string); ok {
				labels = append(labels, s)
			}
		}
	}

	props, _ := propsAny.(map[string]any)
	if props == nil {
		props = map[string]any{}
	}

	return Node{
		ID:         id,
		Labels:     labels,
		Properties: props,
	}, true
}

// AsRel attempts to interpret v as a relationship value.
func AsRel(v any) (Rel, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return Rel{}, false
	}
	id, _ := m["id"]
	src, _ := m["src_id"]
	dst, _ := m["dst_id"]
	label, _ := m["label"].(string)
	propsAny, _ := m["properties"]
	props, _ := propsAny.(map[string]any)
	if props == nil {
		props = map[string]any{}
	}

	return Rel{
		ID:         id,
		SrcID:      src,
		DstID:      dst,
		Label:      label,
		Properties: props,
	}, true
}

// Node returns the Node at the given column index.
func (row Row) Node(index int) (Node, error) {
	v, err := row.Value(uint64(index))
	if err != nil {
		return Node{}, err
	}
	n, ok := AsNode(v)
	if !ok {
		return Node{}, fmt.Errorf("ladybug: column %d is not Node (got %T)", index, v)
	}
	return n, nil
}

// Rel returns the relationship at the given column index.
func (row Row) Rel(index int) (Rel, error) {
	v, err := row.Value(uint64(index))
	if err != nil {
		return Rel{}, err
	}
	r, ok := AsRel(v)
	if !ok {
		return Rel{}, fmt.Errorf("ladybug: column %d is not Rel (got %T)", index, v)
	}
	return r, nil
}

// Scan assigns the columns in the row to the destinations in dest.
// len(dest) must be <= NumColumns(); extra columns are ignored.
// Each dest must be a non-nil pointer to a supported type.
func (row Row) Scan(dest ...any) error {
	if row.c == nil {
		return ErrClosed
	}
	if len(dest) == 0 {
		return nil
	}
	if uint64(len(dest)) > row.numCols {
		return fmt.Errorf("ladybug: Scan has %d destinations, but row has %d columns", len(dest), row.numCols)
	}
	for i, d := range dest {
		if d == nil {
			return fmt.Errorf("ladybug: Scan dest[%d] is nil", i)
		}
		v, err := row.Value(uint64(i))
		if err != nil {
			return err
		}
		if v == nil {
			switch ptr := d.(type) {
			case *any:
				*ptr = nil
			default:
				// leave zero value for other pointer types
			}
			continue
		}

		switch ptr := d.(type) {
		case *bool:
			val, ok := v.(bool)
			if !ok {
				return fmt.Errorf("ladybug: column %d is not assignable to *bool (got %T)", i, v)
			}
			*ptr = val
		case *int64:
			val, ok := v.(int64)
			if !ok {
				return fmt.Errorf("ladybug: column %d is not assignable to *int64 (got %T)", i, v)
			}
			*ptr = val
		case *uint64:
			val, ok := v.(uint64)
			if !ok {
				return fmt.Errorf("ladybug: column %d is not assignable to *uint64 (got %T)", i, v)
			}
			*ptr = val
		case *float64:
			val, ok := v.(float64)
			if !ok {
				return fmt.Errorf("ladybug: column %d is not assignable to *float64 (got %T)", i, v)
			}
			*ptr = val
		case *string:
			val, ok := v.(string)
			if !ok {
				return fmt.Errorf("ladybug: column %d is not assignable to *string (got %T)", i, v)
			}
			*ptr = val
		case *[]byte:
			val, ok := v.([]byte)
			if !ok {
				return fmt.Errorf("ladybug: column %d is not assignable to *[]byte (got %T)", i, v)
			}
			*ptr = val
		case *time.Time:
			val, ok := v.(time.Time)
			if !ok {
				return fmt.Errorf("ladybug: column %d is not assignable to *time.Time (got %T)", i, v)
			}
			*ptr = val
		case *time.Duration:
			val, ok := v.(time.Duration)
			if !ok {
				return fmt.Errorf("ladybug: column %d is not assignable to *time.Duration (got %T)", i, v)
			}
			*ptr = val
		case *Node:
			n, ok := AsNode(v)
			if !ok {
				return fmt.Errorf("ladybug: column %d is not assignable to *Node (got %T)", i, v)
			}
			*ptr = n
		case *Rel:
			r, ok := AsRel(v)
			if !ok {
				return fmt.Errorf("ladybug: column %d is not assignable to *Rel (got %T)", i, v)
			}
			*ptr = r
		case *[]any:
			val, ok := v.([]any)
			if !ok {
				return fmt.Errorf("ladybug: column %d is not assignable to *[]any (got %T)", i, v)
			}
			*ptr = val
		case *map[string]any:
			val, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("ladybug: column %d is not assignable to *map[string]any (got %T)", i, v)
			}
			*ptr = val
		case *any:
			*ptr = v
		default:
			return fmt.Errorf("ladybug: unsupported Scan dest type %T for column %d", d, i)
		}
	}
	return nil
}

// arrow.Record returned by NextRecord must be Released by caller.
