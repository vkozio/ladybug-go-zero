// Package lbugc provides the CGO layer to the Ladybug C API (lbug.h).
// All import "C" and #cgo directives live in this package.
// No C types are exposed in exported APIs.
package lbugc

/*
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/../../lib/dynamic/linux-amd64 -llbug -Wl,-rpath,${SRCDIR}/../../lib/dynamic/linux-amd64
#cgo linux,amd64 CFLAGS: -I${SRCDIR}/../../include
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/../../lib/dynamic/linux-arm64 -llbug -Wl,-rpath,${SRCDIR}/../../lib/dynamic/linux-arm64
#cgo linux,arm64 CFLAGS: -I${SRCDIR}/../../include
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/../../lib/dynamic/darwin -llbug -Wl,-rpath,${SRCDIR}/../../lib/dynamic/darwin
#cgo darwin,amd64 CFLAGS: -I${SRCDIR}/../../include
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/../../lib/dynamic/darwin -llbug -Wl,-rpath,${SRCDIR}/../../lib/dynamic/darwin
#cgo darwin,arm64 CFLAGS: -I${SRCDIR}/../../include
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/../../lib/dynamic/windows-amd64 -llbug_shared
#cgo windows,amd64 CFLAGS: -I${SRCDIR}/../../include
#cgo CFLAGS: -I${SRCDIR}/../../include

#include "lbug.h"
#include <stdlib.h>
*/
import "C"
