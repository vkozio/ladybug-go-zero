package lbugc

/*
#include "lbug.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// Connection wraps the C lbug_connection. Call Close when done.
type Connection struct {
	c *C.lbug_connection
}

// Conn creates a new connection from the database. Caller must call Close.
func (d *Database) Conn() (*Connection, error) {
	if d == nil || d.c == nil {
		return nil, errFromState("connection_init", C.LbugError, "database closed")
	}

	out := (*C.lbug_connection)(C.calloc(1, C.size_t(unsafe.Sizeof(C.lbug_connection{}))))
	if out == nil {
		return nil, errFromState("connection_init", C.LbugError, "alloc failed")
	}

	st := C.lbug_connection_init(d.c, out)
	if st != C.LbugSuccess {
		C.free(unsafe.Pointer(out))
		return nil, errFromState("connection_init", st, "")
	}
	return &Connection{c: out}, nil
}

// Close destroys the connection.
func (c *Connection) Close() {
	if c == nil || c.c == nil {
		return
	}
	C.lbug_connection_destroy(c.c)
	C.free(unsafe.Pointer(c.c))
	c.c = nil
}

// Query runs a Cypher query and returns a Result. Caller must call Result.Close.
func (c *Connection) Query(cypher string) (*Result, error) {
	if c == nil || c.c == nil {
		return nil, errFromState("query", C.LbugError, "connection closed")
	}
	cQuery := C.CString(cypher)
	defer C.free(unsafe.Pointer(cQuery))

	out := (*C.lbug_query_result)(C.calloc(1, C.size_t(unsafe.Sizeof(C.lbug_query_result{}))))
	if out == nil {
		return nil, errFromState("query", C.LbugError, "alloc failed")
	}

	st := C.lbug_connection_query(c.c, cQuery, out)
	if st != C.LbugSuccess {
		C.free(unsafe.Pointer(out))
		return nil, errFromState("query", st, "")
	}
	res := &Result{c: out}
	if err := resultErr(res.c); err != nil {
		res.Close()
		return nil, err
	}
	return res, nil
}

// SetQueryTimeout sets the query timeout in milliseconds (0 = no timeout).
func (c *Connection) SetQueryTimeout(timeoutMs uint64) error {
	if c == nil || c.c == nil {
		return errFromState("set_query_timeout", C.LbugError, "connection closed")
	}
	st := C.lbug_connection_set_query_timeout(c.c, C.uint64_t(timeoutMs))
	if st != C.LbugSuccess {
		return errFromState("set_query_timeout", st, "")
	}
	return nil
}

// Interrupt interrupts the current query on this connection.
func (c *Connection) Interrupt() {
	if c == nil || c.c == nil {
		return
	}
	C.lbug_connection_interrupt(c.c)
}
