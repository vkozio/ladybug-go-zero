package lbugc

/*
#include "lbug.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// Database wraps the C lbug_database. Call Close when done.
type Database struct {
	c *C.lbug_database
}

// Open opens or creates a database at path using default system config.
// Caller must call Close on the returned Database.
func Open(path string) (*Database, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	config := C.lbug_default_system_config()

	out := (*C.lbug_database)(C.calloc(1, C.size_t(unsafe.Sizeof(C.lbug_database{}))))
	if out == nil {
		return nil, errFromState("database_init", C.LbugError, "alloc failed")
	}

	st := C.lbug_database_init(cPath, config, out)
	if st != C.LbugSuccess {
		C.free(unsafe.Pointer(out))
		return nil, errFromState("database_init", st, "")
	}
	return &Database{c: out}, nil
}

// Close destroys the database and frees resources.
func (d *Database) Close() {
	if d == nil || d.c == nil {
		return
	}
	C.lbug_database_destroy(d.c)
	C.free(unsafe.Pointer(d.c))
	d.c = nil
}
