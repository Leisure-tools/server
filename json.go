package server

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

///
/// json
///

type stringMap = map[string]string

type fieldMap struct {
	fields     []string
	properties []string
}

var fieldMaps map[string]*fieldMap = map[string]*fieldMap{}

const JSON_TAG = `(^|,)json:"([^"]+)"`

func (m *fieldMap) propertyFor(field string) string {
	for i, fld := range m.fields {
		if fld == field {
			return m.properties[i]
		}
	}
	return ""
}

func (m *fieldMap) fieldFor(property string) string {
	for i, prop := range m.properties {
		if prop == property {
			return m.fields[i]
		}
	}
	return ""
}

func getFieldMap(t reflect.Type) *fieldMap {
	m := fieldMaps[t.Name()]
	if m == nil {
		fields := make([]string, 0, t.NumField())
		properties := make([]string, 0, t.NumField())
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			re, _ := regexp.Compile(JSON_TAG)
			if match := re.FindSubmatch([]byte(field.Tag)); match != nil {
				prop := string(match[2])
				fields = append(fields, field.Name)
				properties = append(properties, prop)
			} else {
			}
		}
		m = &fieldMap{fields, properties}
		fieldMaps[t.Name()] = m
	}
	return m
}

func jmap(items ...any) map[string]any {
	result := map[string]any{}
	for i := 0; i < len(items); i += 2 {
		switch s := items[i].(type) {
		case string:
			result[string(s)] = items[i+1]
		default:
			panic(fmt.Sprintf("Expected a string for map key but got %v", items[i]))
		}
	}
	return result
}

type jsonObj struct {
	v any
}

func jsonV(value any) jsonObj {
	if j, ok := value.(jsonObj); ok {
		return j
	}
	return jsonObj{value}
}

func (j jsonObj) value() reflect.Value {
	return reflect.ValueOf(j.v)
}

func (j jsonObj) baseType() reflect.Type {
	return baseType(j.value().Type())
}

func baseType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

func (j jsonObj) baseValue() reflect.Value {
	return baseValue(j.value())
}

func baseValue(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	return v
}

func (j jsonObj) len() int {
	if j.v == nil {
		return 0
	}
	t := j.baseType()
	switch t.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		return j.baseValue().Len()
	case reflect.Struct:
		return len(getFieldMap(t).fields)
	}
	return 0
}

func (j jsonObj) typeof() string {
	if j.v == nil {
		return "object"
	}
	switch j.baseType().Kind() {
	case reflect.Map, reflect.Struct:
		return "object"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64:
		return "number"
	default:
		return "string"
	}
}

func (j jsonObj) getJson(key any) jsonObj {
	return jsonV(j.get(key))
}

func (j jsonObj) keys() []string {
	keys := make([]string, 0, 4)
	v := j.baseValue()
	switch v.Kind() {
	case reflect.Struct:
		return getFieldMap(v.Type()).properties
	case reflect.Map:
		r := v.MapRange()
		for r.Next() {
			k := r.Key()
			switch k.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				keys = append(keys, fmt.Sprint(k.Int()))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				keys = append(keys, fmt.Sprint(k.Uint()))
			case reflect.Float32, reflect.Float64:
				keys = append(keys, fmt.Sprint(k.Float()))
			case reflect.String:
				keys = append(keys, k.String())
			case reflect.Bool:
				keys = append(keys, fmt.Sprint(k.Bool()))
			default:
				keys = append(keys, "[object]")
			}
		}
	}
	return keys
}

func (j jsonObj) get(key any) any {
	if j.v == nil {
		if _, ok := key.(string); ok {
			return nil
		}
		panic("Attempt to index nil")
	}
	v := j.baseValue()
	switch v.Kind() {
	case reflect.Struct:
		fieldMap := getFieldMap(v.Type())
		if str, ok := key.(string); ok {
			if field := fieldMap.fieldFor(str); field != "" {
				result := v.FieldByName(field)
				if !result.IsValid() {
					return nil
				}
				return result.Interface()
			}
		}
	case reflect.Map:
		if k, ok := key.(string); ok {
			return value(v.MapIndex(reflect.ValueOf(k)))
		}
	case reflect.Slice, reflect.Array:
		var ind int
		switch i := key.(type) {
		case int:
			ind = i
		case int8:
			ind = int(i)
		case int16:
			ind = int(i)
		case int32:
			ind = int(i)
		case int64:
			ind = int(i)
		}
		return value(v.Index(ind))
	}
	return nil
}

func value(v reflect.Value) any {
	if !v.IsValid() {
		return nil
	}
	switch baseType(v.Type()).Kind() {
	case reflect.Map, reflect.Slice, reflect.Array, reflect.Struct, reflect.Interface:
		return v.Interface()
	case reflect.Bool:
		return v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Float32, reflect.Float64:
		return v.Float()
	case reflect.String:
		return v.String()
	default:
		return nil
	}
}

func (j jsonObj) String() string {
	switch j.typeof() {
	case "string":
		return "\"" + strings.ReplaceAll(j.asString(), "\"", "\\\"") + "\""
	case "array":
		sb := &strings.Builder{}
		sb.WriteString("[")
		first := true
		for i := 0; i < j.len(); i++ {
			if first {
				first = false
			} else {
				sb.WriteString(",")
			}
			sb.WriteString(j.getJson(i).String())
		}
		sb.WriteString("]")
		return sb.String()
	case "object":
		sb := &strings.Builder{}
		sb.WriteString("{")
		first := true
		for _, key := range j.keys() {
			if first {
				first = false
			} else {
				sb.WriteString(",")
			}
			sb.WriteString(jsonV(key).asString())
			sb.WriteString(":")
			sb.WriteString(j.getJson(key).String())
		}
		sb.WriteString("}")
		return sb.String()
	default:
		return fmt.Sprint(j.v)
	}
}

func (j jsonObj) isNil() bool {
	return j.v == nil
}

func (j jsonObj) isString() bool {
	switch j.v.(type) {
	case string:
		return true
	}
	return false
}

func (j jsonObj) asString() string {
	switch s := j.v.(type) {
	case string:
		return s
	}
	panic(fmt.Sprintf("%v is not a string", j.v))
}

func (j jsonObj) isNumber() bool {
	switch j.v.(type) {
	case int, int8, int16, int32, int64, float32, float64:
		return true
	}
	return false
}

func (j jsonObj) isInt() bool {
	switch j.v.(type) {
	case int, int8, int16, int32, int64:
		return true
	}
	return false
}

func (j jsonObj) isFloat() bool {
	switch j.v.(type) {
	case float32, float64:
		return true
	}
	return false
}

func (j jsonObj) asInt() int {
	switch i := j.v.(type) {
	case int:
		return i
	case int8:
		return int(i)
	case int16:
		return int(i)
	case int32:
		return int(i)
	case int64:
		return int(i)
	case float32:
		return int(i)
	case float64:
		return int(i)
	}
	return 0
}

func (j jsonObj) asInt64() int64 {
	switch i := j.v.(type) {
	case int:
		return int64(i)
	case int8:
		return int64(i)
	case int16:
		return int64(i)
	case int32:
		return int64(i)
	case int64:
		return i
	case float32:
		return int64(i)
	case float64:
		return int64(i)
	}
	return 0
}

func (j jsonObj) isBoolean() bool {
	switch j.v.(type) {
	case bool:
		return true
	}
	return false
}

func (j jsonObj) asFloat64() float64 {
	switch i := j.v.(type) {
	case int:
		return float64(i)
	case int8:
		return float64(i)
	case int16:
		return float64(i)
	case int32:
		return float64(i)
	case int64:
		return float64(i)
	case float32:
		return float64(i)
	case float64:
		return float64(i)
	}
	return 0
}

func (j jsonObj) isArray() bool {
	return j.value().Kind() == reflect.Slice
}

func (j jsonObj) isMap() bool {
	return j.value().Kind() == reflect.Map
}
