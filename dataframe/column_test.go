package dataframe

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func Test_getColumnTypeFromString_func(t *testing.T) {
	as := assert.New(t)
	types := [...]string{
		"int",
		"uint",
		"float",
		"complex",
		"string"}

	for _, strType := range types {
		ty, err := getColumnTypeFromString(strType)
		as.Equal(strType, string(ty), "the string type is invalid")
		as.Nil(err, "the error isn't nil")
	}

	// invalid types
	ty, err := getColumnTypeFromString("test")
	as.Equal("", string(ty), "the string must be empty")
	as.Equal("test is an invalid type", err.Error(), "the string must be empty")
}

func Test_getColumnTypeFromKind_func(t *testing.T) {
	as := assert.New(t)
	types := map[columnType][]interface{}{
		INT: {
			100, int64(100), int32(100), int16(100), int8(100),
			new(int), new(int64), new(int32), new(int16), new(int8),
		},
		UINT: {
			uint(100), uint64(100), uint32(100), uint16(100), uint8(100),
			new(uint), new(uint64), new(uint32), new(uint16), new(uint8),
		},
		FLOAT:   {float64(100), float32(100), new(float64), new(float32)},
		COMPLEX: {complex128(100), complex64(100), new(complex128), new(complex64)},
		STRING:  {"test", new(string)}}

	for c, arr := range types {
		for _, t := range arr {
			colt, btype, err := getColumnTypeFromType(reflect.TypeOf(t))
			if err != nil {
				as.FailNow("err isn't nil", "error: %s ", err.Error())
			}
			as.True(btype, "is not a basetype")
			as.Equal(c, colt, "the colt returned isn't match with the column type")
		}
	}

	// errors
	c, _, err := getColumnTypeFromType(reflect.TypeOf(true))
	as.Equal("", string(c), "the column isn't empty")
	as.Equal("bool type is invalid", err.Error(), "The error messages don't match")

	// Custom values.
	ctypes := map[columnType]interface{}{
		INT:     simpleIntType{3},
		UINT:    simpleUintType{3},
		FLOAT:   simpleFloatType{3},
		COMPLEX: simpleComplexType{3},
		STRING:  simpleStringType{"test"}}

	for c, elem := range ctypes {
		ct, baseType, err := getColumnTypeFromType(reflect.TypeOf(elem))
		as.Equalf(c, ct, "columType doesn't match. %s != %s", string(c), string(ct))
		as.Falsef(baseType, "%s is defined as base type", string(ct))

		if err != nil {
			as.FailNow("err isn't nil", "error: %s ", err.Error())
		}
	}

	// error in custom Value
	ct, baseType, err := getColumnTypeFromType(reflect.TypeOf(struct{}{}))
	as.Equal("", string(ct), "Column type isn't empty")
	as.False(baseType, "there an error, so basetype must be false")
	as.Equal("type doesn't implements a ValueType", err.Error(),
		"the error message doesn't match")
}

func Test_Kind_func(t *testing.T) {
	as := assert.New(t)
	types := map[string]reflect.Kind{
		"int":     reflect.Int,
		"uint":    reflect.Uint,
		"float":   reflect.Float64,
		"complex": reflect.Complex128,
		"string":  reflect.String}

	for strType, ktype := range types {
		ty, _ := getColumnTypeFromString(strType)
		as.Equal(ktype, ty.Kind(), "string type %s. kind type %s", strType, ktype.String())
	}

	// panic message
	ty := columnType("test")
	panicf := func() { ty.Kind() }
	as.PanicsWithValue("invalid column type", panicf, "panic message isn't  match")
}
