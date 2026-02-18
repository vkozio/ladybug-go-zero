package lbugc

/*
#include "lbug.h"
*/
import "C"

// Version returns the Ladybug library version string and storage version.
// The library and header (lbug.h) must be present (run scripts/download_liblbug.sh first).
func Version() (version string, storageVersion uint64) {
	cstr := C.lbug_get_version()
	if cstr == nil {
		return "", 0
	}
	defer C.lbug_destroy_string(cstr)
	version = C.GoString(cstr)
	storageVersion = uint64(C.lbug_get_storage_version())
	return version, storageVersion
}
