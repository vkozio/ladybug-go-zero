package lbugc

/*
#include "lbug.h"
*/
import "C"
import "fmt"

func errFromState(op string, st C.lbug_state, msg string) error {
	if st == C.LbugSuccess {
		return nil
	}
	if msg != "" {
		return fmt.Errorf("ladybug: %s: %s", op, msg)
	}
	return fmt.Errorf("ladybug: %s failed", op)
}

// resultErr returns an error if the query result indicates failure.
// Caller must not use res after this if error is non-nil for message extraction.
func resultErr(res *C.lbug_query_result) error {
	if res == nil || C.lbug_query_result_is_success(res) {
		return nil
	}
	msg := copyCString(C.lbug_query_result_get_error_message(res))
	return errFromState("query", C.LbugError, msg)
}
