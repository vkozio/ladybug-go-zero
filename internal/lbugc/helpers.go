package lbugc

/*
#include "lbug.h"
*/
import "C"
import "unsafe"

// copyCString copies a C string to Go and frees the C string with lbug_destroy_string.
// Returns "" if cstr is nil.
func copyCString(cstr *C.char) string {
	if cstr == nil {
		return ""
	}
	s := C.GoString(cstr)
	C.lbug_destroy_string(cstr)
	return s
}

// copyBlob copies a C blob to a Go slice and frees the C blob with lbug_destroy_blob.
// Returns nil if ptr is nil or length is 0.
func copyBlob(ptr *C.uint8_t, length C.uint64_t) []byte {
	if ptr == nil || length == 0 {
		return nil
	}
	n := int(length)
	if n <= 0 || n > 0x7fffffff {
		return nil
	}
	b := C.GoBytes(unsafe.Pointer(ptr), C.int(n))
	C.lbug_destroy_blob(ptr)
	return b
}
