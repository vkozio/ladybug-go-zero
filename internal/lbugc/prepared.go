package lbugc

/*
#include "lbug.h"
#include <stdlib.h>
*/
import "C"
import (
	"time"
	"unsafe"
)

// PreparedStatement wraps the C lbug_prepared_statement. Call Close when done.
type PreparedStatement struct {
	c *C.lbug_prepared_statement
}

// Prepare prepares a Cypher statement. Caller must call Close on the returned PreparedStatement.
func (c *Connection) Prepare(cypher string) (*PreparedStatement, error) {
	if c == nil || c.c == nil {
		return nil, errFromState("prepare", C.LbugError, "connection closed")
	}
	cQuery := C.CString(cypher)
	defer C.free(unsafe.Pointer(cQuery))

	out := (*C.lbug_prepared_statement)(C.calloc(1, C.size_t(unsafe.Sizeof(C.lbug_prepared_statement{}))))
	if out == nil {
		return nil, errFromState("prepare", C.LbugError, "alloc failed")
	}

	st := C.lbug_connection_prepare(c.c, cQuery, out)
	if st != C.LbugSuccess {
		C.free(unsafe.Pointer(out))
		return nil, errFromState("prepare", st, "")
	}
	ps := &PreparedStatement{c: out}
	if !bool(C.lbug_prepared_statement_is_success(ps.c)) {
		msg := copyCString(C.lbug_prepared_statement_get_error_message(ps.c))
		ps.Close()
		return nil, errFromState("prepare", C.LbugError, msg)
	}
	return ps, nil
}

// Close destroys the prepared statement.
func (ps *PreparedStatement) Close() {
	if ps == nil || ps.c == nil {
		return
	}
	C.lbug_prepared_statement_destroy(ps.c)
	C.free(unsafe.Pointer(ps.c))
	ps.c = nil
}

// BindBool binds a bool parameter.
func (ps *PreparedStatement) BindBool(name string, v bool) error {
	if ps == nil || ps.c == nil {
		return errFromState("bind_bool", C.LbugError, "prepared statement closed")
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	if C.lbug_prepared_statement_bind_bool(ps.c, cName, C.bool(v)) != C.LbugSuccess {
		return errFromState("bind_bool", C.LbugError, "")
	}
	return nil
}

// BindInt64 binds an int64 parameter.
func (ps *PreparedStatement) BindInt64(name string, v int64) error {
	if ps == nil || ps.c == nil {
		return errFromState("bind_int64", C.LbugError, "prepared statement closed")
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	if C.lbug_prepared_statement_bind_int64(ps.c, cName, C.int64_t(v)) != C.LbugSuccess {
		return errFromState("bind_int64", C.LbugError, "")
	}
	return nil
}

// BindDouble binds a double parameter.
func (ps *PreparedStatement) BindDouble(name string, v float64) error {
	if ps == nil || ps.c == nil {
		return errFromState("bind_double", C.LbugError, "prepared statement closed")
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	if C.lbug_prepared_statement_bind_double(ps.c, cName, C.double(v)) != C.LbugSuccess {
		return errFromState("bind_double", C.LbugError, "")
	}
	return nil
}

// BindString binds a string parameter.
func (ps *PreparedStatement) BindString(name string, v string) error {
	if ps == nil || ps.c == nil {
		return errFromState("bind_string", C.LbugError, "prepared statement closed")
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	cVal := C.CString(v)
	defer C.free(unsafe.Pointer(cVal))
	if C.lbug_prepared_statement_bind_string(ps.c, cName, cVal) != C.LbugSuccess {
		return errFromState("bind_string", C.LbugError, "")
	}
	return nil
}

// BindDate binds a date parameter using days since Unix epoch at midnight UTC.
func (ps *PreparedStatement) BindDate(name string, v time.Time) error {
	if ps == nil || ps.c == nil {
		return errFromState("bind_date", C.LbugError, "prepared statement closed")
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	t := v.UTC()
	days := t.Unix() / 86400
	var date C.lbug_date_t
	date.days = C.int32_t(days)
	if C.lbug_prepared_statement_bind_date(ps.c, cName, date) != C.LbugSuccess {
		return errFromState("bind_date", C.LbugError, "")
	}
	return nil
}

// BindTime binds a timestamp parameter with nanosecond precision.
func (ps *PreparedStatement) BindTime(name string, v time.Time) error {
	if ps == nil || ps.c == nil {
		return errFromState("bind_timestamp", C.LbugError, "prepared statement closed")
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	t := v.UTC()
	ns := t.UnixNano()
	var ts C.lbug_timestamp_ns_t
	ts.value = C.int64_t(ns)
	if C.lbug_prepared_statement_bind_timestamp_ns(ps.c, cName, ts) != C.LbugSuccess {
		return errFromState("bind_timestamp", C.LbugError, "")
	}
	return nil
}

// BindInterval binds an interval parameter using the total duration.
func (ps *PreparedStatement) BindInterval(name string, v time.Duration) error {
	if ps == nil || ps.c == nil {
		return errFromState("bind_interval", C.LbugError, "prepared statement closed")
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	seconds := float64(v) / float64(time.Second)
	var interval C.lbug_interval_t
	C.lbug_interval_from_difftime(C.double(seconds), &interval)
	if C.lbug_prepared_statement_bind_interval(ps.c, cName, interval) != C.LbugSuccess {
		return errFromState("bind_interval", C.LbugError, "")
	}
	return nil
}

// BindUUID binds a UUID parameter as string.
func (ps *PreparedStatement) BindUUID(name string, v string) error {
	if ps == nil || ps.c == nil {
		return errFromState("bind_uuid", C.LbugError, "prepared statement closed")
	}
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	cVal := C.CString(v)
	defer C.free(unsafe.Pointer(cVal))
	if C.lbug_prepared_statement_bind_string(ps.c, cName, cVal) != C.LbugSuccess {
		return errFromState("bind_uuid", C.LbugError, "")
	}
	return nil
}

// Execute runs the prepared statement and returns a Result. Caller must call Result.Close.
func (ps *PreparedStatement) Execute(conn *Connection) (*Result, error) {
	if ps == nil || ps.c == nil {
		return nil, errFromState("execute", C.LbugError, "prepared statement closed")
	}
	if conn == nil || conn.c == nil {
		return nil, errFromState("execute", C.LbugError, "connection closed")
	}

	out := (*C.lbug_query_result)(C.calloc(1, C.size_t(unsafe.Sizeof(C.lbug_query_result{}))))
	if out == nil {
		return nil, errFromState("execute", C.LbugError, "alloc failed")
	}

	st := C.lbug_connection_execute(conn.c, ps.c, out)
	if st != C.LbugSuccess {
		C.free(unsafe.Pointer(out))
		return nil, errFromState("execute", st, "")
	}
	res := &Result{c: out}
	if err := resultErr(res.c); err != nil {
		res.Close()
		return nil, err
	}
	return res, nil
}
