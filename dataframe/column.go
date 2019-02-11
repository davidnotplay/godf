package dataframe

import (
	"fmt"
	"reflect"
)

// Type used for the dataframe columns.
type columnType string

const (
	INT     columnType = "int"
	UINT    columnType = "uint"
	FLOAT   columnType = "float"
	COMPLEX columnType = "complex"
	STRING  columnType = "string"
)

// getColumnTypeFromString returns one of the columnType constant.
// Whether `str` param isn't match with any columnType returns an error.
func getColumnTypeFromString(str string) (columnType, error) {
	coltype := columnType(str)

	switch coltype {
	case INT, UINT, FLOAT, COMPLEX, STRING:
		return coltype, nil
	default:
		return columnType(""), fmt.Errorf("%s is an invalid type", str)
	}
}

// getColumnTypeFromType get a columType const, depending of the t param.
// t params must contains one of the next types:
// 	- basic type: (int, uint, float, complex, string)
//	- struct or interface that implements a ValueType (IntType, FloatType...)
//	- Ptr to basic type: (int, uint, float, complex, string)
//	- Ptr to struct or interface that implements a ValueType (IntType, FloatType...)
//
// The function returns the columnType. One bool value, indicating if the the type of t param
// is a basic type. And an error if t contains and invalid type.
func getColumnTypeFromType(t reflect.Type) (columnType, bool, error) {
	k := t.Kind()

	// The type is a ptr. Check then the ptr element.
	if k == reflect.Ptr {
		return getColumnTypeFromType(t.Elem())
	}

	if k == reflect.Struct || k == reflect.Interface {
		// check if t implements some of the ValuesType
		if t.Implements(reflect.TypeOf((*IntType)(nil)).Elem()) {
			return INT, false, nil
		}
		if t.Implements(reflect.TypeOf((*UintType)(nil)).Elem()) {
			return UINT, false, nil
		}
		if t.Implements(reflect.TypeOf((*FloatType)(nil)).Elem()) {
			return FLOAT, false, nil
		}
		if t.Implements(reflect.TypeOf((*ComplexType)(nil)).Elem()) {
			return COMPLEX, false, nil
		}
		if t.Implements(reflect.TypeOf((*StringType)(nil)).Elem()) {
			return STRING, false, nil
		}

		return columnType(""), false, fmt.Errorf("type doesn't implements a ValueType")
	}

	switch k {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return INT, true, nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return UINT, true, nil
	case reflect.Float64, reflect.Float32:
		return FLOAT, true, nil
	case reflect.Complex128, reflect.Complex64:
		return COMPLEX, true, nil
	case reflect.String:
		return STRING, true, nil

	default:
		return columnType(""), false, fmt.Errorf("%s type is invalid", k.String())
	}
}

// Kind returns the Kind type associate to the columnType constants.
// If columnType isn't one of the constants then the function throw a panic message.
func (c columnType) Kind() reflect.Kind {
	switch c {
	case INT:
		return reflect.Int
	case UINT:
		return reflect.Uint
	case FLOAT:
		return reflect.Float64
	case COMPLEX:
		return reflect.Complex128
	case STRING:
		return reflect.String
	default:
		panic("invalid column type")
	}
}

type column struct {
	// column name
	name string
	// Column type
	ctype columnType
	// column position in dataframe
	index int
	// flag indicating if is a basic type.
	basicType bool
}

type orderType int8

const (
	ASC  orderType = 1
	DESC orderType = 2
)

type internalOrderColumn struct {
	column *column
	order  orderType
}

type OrderColumn struct {
	Name  string
	Order orderType
}
