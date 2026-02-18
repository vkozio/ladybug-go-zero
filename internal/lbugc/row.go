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

// Row wraps a single row (flat tuple). Call Release when done.
type Row struct {
	c       *C.lbug_flat_tuple
	numCols uint64
}

// Release destroys the row and frees the underlying flat tuple.
func (row *Row) Release() {
	if row == nil || row.c == nil {
		return
	}
	C.lbug_flat_tuple_destroy(row.c)
	C.free(unsafe.Pointer(row.c))
	row.c = nil
}

// Value returns the value at column index as a Go value (bool, int64, float64, string, []byte, or nil for null).
// Release must be called on the Row when done; Value does not retain the Row.
func (row *Row) Value(index uint64) (interface{}, error) {
	if row == nil || row.c == nil || index >= row.numCols {
		return nil, errFromState("value", C.LbugError, "invalid index")
	}
	var v C.lbug_value
	st := C.lbug_flat_tuple_get_value(row.c, C.uint64_t(index), &v)
	if st != C.LbugSuccess {
		return nil, errFromState("flat_tuple_get_value", st, "")
	}
	defer C.lbug_value_destroy(&v)
	return valueToGo(&v)
}

func valueToGo(v *C.lbug_value) (interface{}, error) {
	if C.lbug_value_is_null(v) {
		return nil, nil
	}
	var dt C.lbug_logical_type
	C.lbug_value_get_data_type(v, &dt)
	// Do not destroy dt: get_data_type returns internal reference.
	id := C.lbug_data_type_get_id(&dt)
	switch id {
	case C.LBUG_BOOL:
		var out C.bool
		if C.lbug_value_get_bool(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		return bool(out), nil
	case C.LBUG_INT8, C.LBUG_INT16, C.LBUG_INT32, C.LBUG_INT64, C.LBUG_SERIAL:
		var out C.int64_t
		if C.lbug_value_get_int64(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		return int64(out), nil
	case C.LBUG_UINT8, C.LBUG_UINT16, C.LBUG_UINT32, C.LBUG_UINT64:
		var out C.uint64_t
		if C.lbug_value_get_uint64(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		return uint64(out), nil
	case C.LBUG_FLOAT:
		var out C.float
		if C.lbug_value_get_float(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		return float64(out), nil
	case C.LBUG_DOUBLE:
		var out C.double
		if C.lbug_value_get_double(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		return float64(out), nil
	case C.LBUG_DATE:
		var out C.lbug_date_t
		if C.lbug_value_get_date(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		days := int64(out.days)
		// Days since Unix epoch, map to midnight UTC.
		return time.Unix(days*86400, 0).UTC(), nil
	case C.LBUG_TIMESTAMP:
		var out C.lbug_timestamp_t
		if C.lbug_value_get_timestamp(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		micros := int64(out.value)
		return time.Unix(0, micros*int64(time.Microsecond)).UTC(), nil
	case C.LBUG_TIMESTAMP_NS:
		var out C.lbug_timestamp_ns_t
		if C.lbug_value_get_timestamp_ns(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		return time.Unix(0, int64(out.value)).UTC(), nil
	case C.LBUG_TIMESTAMP_MS:
		var out C.lbug_timestamp_ms_t
		if C.lbug_value_get_timestamp_ms(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		ms := int64(out.value)
		return time.Unix(0, ms*int64(time.Millisecond)).UTC(), nil
	case C.LBUG_TIMESTAMP_SEC:
		var out C.lbug_timestamp_sec_t
		if C.lbug_value_get_timestamp_sec(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		sec := int64(out.value)
		return time.Unix(sec, 0).UTC(), nil
	case C.LBUG_TIMESTAMP_TZ:
		var out C.lbug_timestamp_tz_t
		if C.lbug_value_get_timestamp_tz(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		micros := int64(out.value)
		// Treat stored microseconds as UTC.
		return time.Unix(0, micros*int64(time.Microsecond)).UTC(), nil
	case C.LBUG_INTERVAL:
		var out C.lbug_interval_t
		if C.lbug_value_get_interval(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		var seconds C.double
		C.lbug_interval_to_difftime(out, &seconds)
		d := time.Duration(float64(seconds) * float64(time.Second))
		return d, nil
	case C.LBUG_STRING:
		var out *C.char
		if C.lbug_value_get_string(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		return copyCString(out), nil
	case C.LBUG_BLOB:
		var out *C.uint8_t
		var length C.uint64_t
		if C.lbug_value_get_blob(v, &out, &length) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		return copyBlob(out, length), nil
	case C.LBUG_UUID:
		var out *C.char
		if C.lbug_value_get_uuid(v, &out) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		return copyCString(out), nil
	case C.LBUG_LIST, C.LBUG_ARRAY:
		return listToSlice(v)
	case C.LBUG_STRUCT, C.LBUG_RECURSIVE_REL, C.LBUG_MAP, C.LBUG_UNION:
		return structOrMapToGo(v)
	case C.LBUG_NODE:
		return nodeToMap(v)
	case C.LBUG_REL:
		return relToMap(v)
	default:
		// Fallback: string representation
		s := C.lbug_value_to_string(v)
		return copyCString(s), nil
	}
}

// listToSlice returns []interface{} for LIST/ARRAY values, or a string fallback on error.
func listToSlice(v *C.lbug_value) (interface{}, error) {
	var size C.uint64_t
	if C.lbug_value_get_list_size(v, &size) != C.LbugSuccess {
		return copyCString(C.lbug_value_to_string(v)), nil
	}
	if size == 0 {
		return []interface{}{}, nil
	}
	n := int(size)
	if n < 0 {
		return copyCString(C.lbug_value_to_string(v)), nil
	}
	out := make([]interface{}, n)
	for i := C.uint64_t(0); i < size; i++ {
		var elem C.lbug_value
		if C.lbug_value_get_list_element(v, i, &elem) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		goVal, err := valueToGo(&elem)
		C.lbug_value_destroy(&elem)
		if err != nil {
			return nil, err
		}
		out[int(i)] = goVal
	}
	return out, nil
}

// structOrMapToGo returns map[string]interface{} for STRUCT/NODE/REL/RECURSIVE_REL/MAP/UNION values,
// or a string fallback on error.
func structOrMapToGo(v *C.lbug_value) (interface{}, error) {
	// MAP has dedicated accessors; handle it first.
	var dt C.lbug_logical_type
	C.lbug_value_get_data_type(v, &dt)
	id := C.lbug_data_type_get_id(&dt)
	if id == C.LBUG_MAP {
		return mapValueToMap(v)
	}
	return structValueToMap(v)
}

func structValueToMap(v *C.lbug_value) (interface{}, error) {
	var fieldCount C.uint64_t
	if C.lbug_value_get_struct_num_fields(v, &fieldCount) != C.LbugSuccess {
		return copyCString(C.lbug_value_to_string(v)), nil
	}
	m := make(map[string]interface{}, int(fieldCount))
	for i := C.uint64_t(0); i < fieldCount; i++ {
		var nameC *C.char
		if C.lbug_value_get_struct_field_name(v, i, &nameC) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		name := copyCString(nameC)

		var field C.lbug_value
		if C.lbug_value_get_struct_field_value(v, i, &field) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		goVal, err := valueToGo(&field)
		C.lbug_value_destroy(&field)
		if err != nil {
			return nil, err
		}
		m[name] = goVal
	}
	return m, nil
}

func mapValueToMap(v *C.lbug_value) (interface{}, error) {
	var size C.uint64_t
	if C.lbug_value_get_map_size(v, &size) != C.LbugSuccess {
		return copyCString(C.lbug_value_to_string(v)), nil
	}
	m := make(map[string]interface{}, int(size))
	for i := C.uint64_t(0); i < size; i++ {
		var keyVal C.lbug_value
		if C.lbug_value_get_map_key(v, i, &keyVal) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		keyStr := copyCString(C.lbug_value_to_string(&keyVal))
		C.lbug_value_destroy(&keyVal)

		var val C.lbug_value
		if C.lbug_value_get_map_value(v, i, &val) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		goVal, err := valueToGo(&val)
		C.lbug_value_destroy(&val)
		if err != nil {
			return nil, err
		}
		m[keyStr] = goVal
	}
	return m, nil
}

// nodeToMap returns a generic map representation of a NODE value.
// Keys: "id" (internal id), "labels" ([]string), "properties" (map[string]interface{}).
func nodeToMap(v *C.lbug_value) (interface{}, error) {
	var idVal C.lbug_value
	if C.lbug_node_val_get_id_val(v, &idVal) != C.LbugSuccess {
		return copyCString(C.lbug_value_to_string(v)), nil
	}
	id, err := valueToGo(&idVal)
	C.lbug_value_destroy(&idVal)
	if err != nil {
		return nil, err
	}

	var labelVal C.lbug_value
	if C.lbug_node_val_get_label_val(v, &labelVal) != C.LbugSuccess {
		return copyCString(C.lbug_value_to_string(v)), nil
	}
	labelsAny, err := valueToGo(&labelVal)
	C.lbug_value_destroy(&labelVal)
	if err != nil {
		return nil, err
	}
	var labels []string
	switch l := labelsAny.(type) {
	case string:
		if l != "" {
			labels = []string{l}
		}
	case []interface{}:
		for _, el := range l {
			if s, ok := el.(string); ok {
				labels = append(labels, s)
			}
		}
	}

	var propSize C.uint64_t
	if C.lbug_node_val_get_property_size(v, &propSize) != C.LbugSuccess {
		return copyCString(C.lbug_value_to_string(v)), nil
	}
	props := make(map[string]interface{}, int(propSize))
	for i := C.uint64_t(0); i < propSize; i++ {
		var nameC *C.char
		if C.lbug_node_val_get_property_name_at(v, i, &nameC) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		name := copyCString(nameC)

		var pv C.lbug_value
		if C.lbug_node_val_get_property_value_at(v, i, &pv) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		goVal, err := valueToGo(&pv)
		C.lbug_value_destroy(&pv)
		if err != nil {
			return nil, err
		}
		props[name] = goVal
	}

	m := make(map[string]interface{}, 3)
	m["id"] = id
	m["labels"] = labels
	m["properties"] = props
	return m, nil
}

// relToMap returns a generic map representation of a REL value.
// Keys: "id", "src_id", "dst_id", "label", "properties".
func relToMap(v *C.lbug_value) (interface{}, error) {
	var idVal C.lbug_value
	if C.lbug_rel_val_get_id_val(v, &idVal) != C.LbugSuccess {
		return copyCString(C.lbug_value_to_string(v)), nil
	}
	id, err := valueToGo(&idVal)
	C.lbug_value_destroy(&idVal)
	if err != nil {
		return nil, err
	}

	var srcVal C.lbug_value
	if C.lbug_rel_val_get_src_id_val(v, &srcVal) != C.LbugSuccess {
		return copyCString(C.lbug_value_to_string(v)), nil
	}
	srcID, err := valueToGo(&srcVal)
	C.lbug_value_destroy(&srcVal)
	if err != nil {
		return nil, err
	}

	var dstVal C.lbug_value
	if C.lbug_rel_val_get_dst_id_val(v, &dstVal) != C.LbugSuccess {
		return copyCString(C.lbug_value_to_string(v)), nil
	}
	dstID, err := valueToGo(&dstVal)
	C.lbug_value_destroy(&dstVal)
	if err != nil {
		return nil, err
	}

	var labelVal C.lbug_value
	if C.lbug_rel_val_get_label_val(v, &labelVal) != C.LbugSuccess {
		return copyCString(C.lbug_value_to_string(v)), nil
	}
	labelAny, err := valueToGo(&labelVal)
	C.lbug_value_destroy(&labelVal)
	if err != nil {
		return nil, err
	}
	labelStr, _ := labelAny.(string)

	var propSize C.uint64_t
	if C.lbug_rel_val_get_property_size(v, &propSize) != C.LbugSuccess {
		return copyCString(C.lbug_value_to_string(v)), nil
	}
	props := make(map[string]interface{}, int(propSize))
	for i := C.uint64_t(0); i < propSize; i++ {
		var nameC *C.char
		if C.lbug_rel_val_get_property_name_at(v, i, &nameC) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		name := copyCString(nameC)

		var pv C.lbug_value
		if C.lbug_rel_val_get_property_value_at(v, i, &pv) != C.LbugSuccess {
			return copyCString(C.lbug_value_to_string(v)), nil
		}
		goVal, err := valueToGo(&pv)
		C.lbug_value_destroy(&pv)
		if err != nil {
			return nil, err
		}
		props[name] = goVal
	}

	m := make(map[string]interface{}, 5)
	m["id"] = id
	m["src_id"] = srcID
	m["dst_id"] = dstID
	m["label"] = labelStr
	m["properties"] = props
	return m, nil
}
