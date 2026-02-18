// Package ladybug provides a native Go driver for Ladybug graph database.
// Queries use the Cypher language. Result iteration supports Arrow (zero-copy) and row-by-row.
// Compatible Ladybug version: see README (e.g. v0.14.2-bindings.0).
// Obtain libs and header via scripts/download_liblbug.sh before building.
package ladybug

import "github.com/vkozio/ladybug-go-zero/internal/lbugc"

// Version returns the loaded Ladybug library version string and storage version.
// Requires include/lbug.h and lib/dynamic/<platform> to be present (see README).
func Version() (version string, storageVersion uint64) {
	return lbugc.Version()
}
