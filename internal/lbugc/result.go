package lbugc

/*
#include "lbug.h"
#include <stdlib.h>
static void call_arrow_schema_release(struct ArrowSchema* s) {
	if (s && s->release) s->release(s);
}
static void call_arrow_array_release(struct ArrowArray* a) {
	if (a && a->release) a->release(a);
}
*/
import "C"
import "unsafe"

// Result wraps the C lbug_query_result. Call Close when done.
type Result struct {
	c *C.lbug_query_result
}

// Close destroys the result and releases resources (including Arrow schema if obtained).
func (r *Result) Close() {
	if r == nil {
		return
	}
	if r.c != nil {
		C.lbug_query_result_destroy(r.c)
		C.free(unsafe.Pointer(r.c))
		r.c = nil
	}
}

// NumColumns returns the number of columns.
func (r *Result) NumColumns() uint64 {
	if r == nil || r.c == nil {
		return 0
	}
	return uint64(C.lbug_query_result_get_num_columns(r.c))
}

// NumTuples returns the number of rows (may be 0 if unknown).
func (r *Result) NumTuples() uint64 {
	if r == nil || r.c == nil {
		return 0
	}
	return uint64(C.lbug_query_result_get_num_tuples(r.c))
}

// ColumnName returns the column name at index. Empty string on error.
func (r *Result) ColumnName(index uint64) string {
	if r == nil || r.c == nil {
		return ""
	}
	var cName *C.char
	st := C.lbug_query_result_get_column_name(r.c, C.uint64_t(index), &cName)
	if st != C.LbugSuccess {
		return ""
	}
	return copyCString(cName)
}

// HasNext returns true if there is another row.
func (r *Result) HasNext() bool {
	if r == nil || r.c == nil {
		return false
	}
	return bool(C.lbug_query_result_has_next(r.c))
}

// GetNext returns the next row. The returned Row must be released by calling Row.Release.
// ok is false when there are no more rows.
func (r *Result) GetNext() (row *Row, ok bool, err error) {
	if r == nil || r.c == nil {
		return nil, false, nil
	}
	if !bool(C.lbug_query_result_has_next(r.c)) {
		return nil, false, nil
	}
	ft := (*C.lbug_flat_tuple)(C.calloc(1, C.size_t(unsafe.Sizeof(C.lbug_flat_tuple{}))))
	if ft == nil {
		return nil, false, errFromState("get_next", C.LbugError, "alloc failed")
	}

	st := C.lbug_query_result_get_next(r.c, ft)
	if st != C.LbugSuccess {
		C.free(unsafe.Pointer(ft))
		return nil, false, errFromState("get_next", st, "")
	}
	return &Row{c: ft, numCols: r.NumColumns()}, true, nil
}

// SchemaRaw returns the Arrow schema as an unsafe.Pointer to C ArrowSchema (Arrow C Data Interface).
// The caller must call the returned cleanup callback to free the ArrowSchema struct.
// If the caller does not pass the schema into arrow/cdata (which calls ArrowSchemaRelease),
// it must call releaseAndFree to avoid leaking the underlying schema resources.
func (r *Result) SchemaRaw() (schemaPtr unsafe.Pointer, cleanup func(), releaseAndFree func()) {
	if r == nil || r.c == nil {
		return nil, func() {}, func() {}
	}
	s := (*C.struct_ArrowSchema)(C.calloc(1, C.size_t(unsafe.Sizeof(C.struct_ArrowSchema{}))))
	if s == nil {
		return nil, func() {}, func() {}
	}
	if C.lbug_query_result_get_arrow_schema(r.c, s) != C.LbugSuccess {
		C.free(unsafe.Pointer(s))
		return nil, func() {}, func() {}
	}

	cleanup = func() {
		C.free(unsafe.Pointer(s))
	}
	releaseAndFree = func() {
		C.call_arrow_schema_release(s)
		C.free(unsafe.Pointer(s))
	}
	return unsafe.Pointer(s), cleanup, releaseAndFree
}

// NextChunkRaw returns the next Arrow array chunk as an unsafe.Pointer to C ArrowArray.
// cleanup frees the ArrowArray struct allocation.
// releaseAndFree calls ArrowArrayRelease and frees the ArrowArray struct. Use on import error.
func (r *Result) NextChunkRaw(chunkSize int64) (arrayPtr unsafe.Pointer, cleanup func(), releaseAndFree func(), err error) {
	if r == nil || r.c == nil {
		return nil, func() {}, func() {}, errFromState("get_next_arrow_chunk", C.LbugError, "result closed")
	}
	cArray := (*C.struct_ArrowArray)(C.calloc(1, C.size_t(unsafe.Sizeof(C.struct_ArrowArray{}))))
	if cArray == nil {
		return nil, func() {}, func() {}, errFromState("get_next_arrow_chunk", C.LbugError, "alloc failed")
	}
	st := C.lbug_query_result_get_next_arrow_chunk(r.c, C.int64_t(chunkSize), cArray)
	if st != C.LbugSuccess {
		C.free(unsafe.Pointer(cArray))
		return nil, func() {}, func() {}, errFromState("get_next_arrow_chunk", st, "")
	}
	cleanup = func() {
		C.free(unsafe.Pointer(cArray))
	}
	releaseAndFree = func() {
		C.call_arrow_array_release(cArray)
		C.free(unsafe.Pointer(cArray))
	}
	return unsafe.Pointer(cArray), cleanup, releaseAndFree, nil
}

// Summary returns compile and execution time in milliseconds if available.
func (r *Result) Summary() (compileMS, execMS float64, err error) {
	if r == nil || r.c == nil {
		return 0, 0, nil
	}
	qs := (*C.lbug_query_summary)(C.calloc(1, C.size_t(unsafe.Sizeof(C.lbug_query_summary{}))))
	if qs == nil {
		return 0, 0, errFromState("query_summary", C.LbugError, "alloc failed")
	}
	st := C.lbug_query_result_get_query_summary(r.c, qs)
	if st != C.LbugSuccess {
		C.free(unsafe.Pointer(qs))
		return 0, 0, errFromState("query_summary", st, "")
	}
	compile := float64(C.lbug_query_summary_get_compiling_time(qs))
	exec := float64(C.lbug_query_summary_get_execution_time(qs))
	C.lbug_query_summary_destroy(qs)
	C.free(unsafe.Pointer(qs))
	return compile, exec, nil
}
