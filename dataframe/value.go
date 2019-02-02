package dataframe

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type Comparers int8

// const
const LESS Comparers = -1
const EQUAL Comparers = 0
const GREAT Comparers = 1

/**
Type struct here
*/

// simpleIntType is a struct will use in the dataframe values when
// the value type is int (int, int64, int32, int16, int8).
// This struct implements the IntType interface.
type simpleIntType struct {
	v int64
}

// Value returns the value stored in the struct.
func (i simpleIntType) Value() int64 {
	return i.v
}

// Compare func compare the value of the struct with the `v` param.
// Returns -1 whether struct value is less than `v`.
// Returns 0 whether struct value is equal than `v`.
// Returns 1 whether struct value is equal than `v`.
func (i simpleIntType) Compare(v int64) Comparers {
	if i.v == v {
		return EQUAL
	} else if i.v < v {
		return LESS
	}

	return GREAT
}

// String transforms the value of struct in a string and it returns it.
func (i simpleIntType) String() string {
	return fmt.Sprintf("%d", i.v)
}

// simpleUintType is a struct will use in the dataframe values when
// the value type is uint (uint, uint64, uint32, uint16, uint8).
// This struct implements the UintType interface.
type simpleUintType struct {
	v uint64
}

// Value returns the value stored in the struct.
func (u simpleUintType) Value() uint64 {
	return u.v
}

// Compare func compare the value of the struct with the `v` param.
// Returns -1 whether struct value is less than `v`.
// Returns 0 whether struct value is equal than `v`.
// Returns 1 whether struct value is equal than `v`.
func (u simpleUintType) Compare(v uint64) Comparers {
	if u.v == v {
		return EQUAL
	} else if u.v < v {
		return LESS
	}

	return GREAT
}

// String transforms the value of struct in a string and it returns it.
func (u simpleUintType) String() string {
	return fmt.Sprintf("%d", u.v)
}

// simpleFloatType is a struct will use in the dataframe values when
// the value type is float (float64, float32).
// This struct implements the FloatType interface.
type simpleFloatType struct {
	v float64
}

// Value returns the value stored in the struct.
func (f simpleFloatType) Value() float64 {
	return f.v
}

// Compare func compare the value of the struct with the `v` param.
// Returns -1 whether struct value is less than `v`.
// Returns 0 whether struct value is equal than `v`.
// Returns 1 whether struct value is equal than `v`.
func (f simpleFloatType) Compare(v float64) Comparers {
	if f.v == v {
		return EQUAL
	} else if f.v < v {
		return LESS
	}

	return GREAT
}

// String transforms the value of struct in a string and it returns it.
func (f simpleFloatType) String() string {
	return fmt.Sprintf("%g", f.v)
}

// simpleComplexType is a struct will use in the dataframe values when
// the value type is complex (complex64, complex128).
// This struct implements the ComplexType interface.
type simpleComplexType struct {
	v complex128
}

// Value returns the value stored in the struct.
func (c simpleComplexType) Value() complex128 {
	return c.v
}

// Compare func compare the value of the struct with the `v` param.
// Returns -1 whether struct value is less than `v`.
// Returns 0 whether struct value is equal than `v`.
// Returns 1 whether struct value is equal than `v`.
func (c simpleComplexType) Compare(v complex128) Comparers {
	if c.v == v {
		return EQUAL
	}

	// Check the real part.
	ireal := real(c.v)
	oreal := real(v)

	if ireal < oreal {
		return LESS
	} else if ireal > oreal {
		return GREAT
	}

	// the real part are equal. Check the imaginary part.
	if imag(c.v) < imag(v) {
		return LESS
	}

	return GREAT
}

// String transforms the value of struct in a string and it returns it.
func (c simpleComplexType) String() string {
	str := fmt.Sprintf("%g", c.v)
	return str[1 : len(str)-1] // remove the parentesis.
}

// simpleStringType is a struct will use in the dataframe values when
// the value type is string.
// This struct implements the StringType interface.
type simpleStringType struct {
	v string
}

// Value returns the value stored in the struct.
func (s simpleStringType) Value() string {
	return s.v
}

// Compare func compare the value of the struct with the `v` param.
// Returns -1 whether struct value is less than `v`.
// Returns 0 whether struct value is equal than `v`.
// Returns 1 whether struct value is equal than `v`.
func (s simpleStringType) Compare(v string) Comparers {
	switch strings.Compare(s.v, v) {
	case 0:
		return EQUAL
	case -1:
		return LESS
	default:
		return GREAT
	}
}

// String transforms the value of struct in a string and it returns it.
func (s simpleStringType) String() string {
	return s.v
}

// Type interfaces.
type BaseType interface {
	String() string
}

type IntType interface {
	BaseType
	Value() int64
	Compare(v int64) Comparers
}

type UintType interface {
	BaseType
	Value() uint64
	Compare(v uint64) Comparers
}

type FloatType interface {
	BaseType
	Value() float64
	Compare(v float64) Comparers
}

type ComplexType interface {
	BaseType
	Value() complex128
	Compare(v complex128) Comparers
}

type StringType interface {
	BaseType
	Value() string
	Compare(v string) Comparers
}

/*
	Value struct
*/

type Value struct {
	value interface{}
}

// newValue func create a new Value struct, using as value the param `v`.
// Whether `v` isn't a *Value type* then it returns an error.
func newValue(v interface{}) (*Value, error) {
	switch v.(type) {
	case IntType,
		UintType,
		FloatType,
		ComplexType,
		StringType:

		return &Value{v}, nil
	default:
		return nil, errors.New("the value isn't a value type")

	}
}

// checkType Check if the value stored in `v` object is an interface to handle
// the type stored in `t` param. For example: if v.value is type IntType and t
// is reflect.Int returns true. Any else returns false. If v.value is an invalid type
// then the function execute a panic func.
func (v *Value) checkType(t reflect.Kind) bool {
	switch v.value.(type) {
	case IntType:
		return t == reflect.Int
	case UintType:
		return t == reflect.Uint
	case FloatType:
		return t == reflect.Float64
	case ComplexType:
		return t == reflect.Complex128
	case StringType:
		return t == reflect.String
	default:
		panic("invalid value type")
	}
}

// toInt function transforms v.value in a int.
// v.value only will transforms in integer if is type `IntType`.
// Any else type returns an error.
func (v *Value) toInt() (int64, error) {
	ok := v.checkType(reflect.Int)

	if !ok {
		return 0, errors.New("value isn't type int")
	}

	r, _ := v.value.(IntType)
	return r.Value(), nil
}

// Int64 returns the value as int64. `v.value` must has the `IntType` type.
// If not returns an error.
func (v *Value) Int64() (int64, error) {
	return v.toInt()
}

// Int returns the value as int. `v.value` must has the `IntType` type.
// If not returns an error.
func (v *Value) Int() (int, error) {
	r, err := v.toInt()
	return int(r), err
}

// Int32 returns the value as int32. `v.value` must has the `IntType` type.
// If not returns an error.
func (v *Value) Int32() (int32, error) {
	r, err := v.toInt()
	return int32(r), err
}

// Int16 returns the value as int16. `v.value` must has the `IntType` type.
// If not returns an error.
func (v *Value) Int16() (int16, error) {
	r, err := v.toInt()
	return int16(r), err
}

// Int8 returns the value as int8. `v.value` must has the `IntType` type.
// If not returns an error.
func (v *Value) Int8() (int8, error) {
	r, err := v.toInt()
	return int8(r), err
}

// toUint function transforms v.value in a uint.
// v.value only will transforms in integer if is type `UintType`.
// Any else type returns an error.
func (v *Value) toUint() (uint64, error) {
	ok := v.checkType(reflect.Uint)

	if !ok {
		return 0, errors.New("value isn't type uint")
	}

	r, _ := v.value.(UintType)
	return r.Value(), nil
}

// Uint64 returns the value as uint64. `v.value` must has the `UintType` type.
// If not returns an error.
func (v *Value) Uint64() (uint64, error) {
	return v.toUint()
}

// Uint returns the value as uint. `v.value` must has the `UintType` type.
// If not returns an error.
func (v *Value) Uint() (uint, error) {
	r, err := v.toUint()
	return uint(r), err
}

// Uint32 returns the value as uint32. `v.value` must has the `UintType` type.
// If not returns an error.
func (v *Value) Uint32() (uint32, error) {
	r, err := v.toUint()
	return uint32(r), err
}

// Uint16 returns the value as uint16. `v.value` must has the `UintType` type.
// If not returns an error.
func (v *Value) Uint16() (uint16, error) {
	r, err := v.toUint()
	return uint16(r), err
}

// Uint8 returns the value as uint8. `v.value` must has the `UintType` type.
// If not returns an error.
func (v *Value) Uint8() (uint8, error) {
	r, err := v.toUint()
	return uint8(r), err
}

// toFloat function transforms v.value in a float64.
// v.value only will transforms, if is type `FloatType`.
// Any else type returns an error.
func (v *Value) toFloat() (float64, error) {
	ok := v.checkType(reflect.Float64)

	if !ok {
		return 0, errors.New("value isn't type float")
	}

	r, _ := v.value.(FloatType)
	return r.Value(), nil
}

// Float64 returns the value as float64. `v.value` must has the `FloaType` type.
// If not returns an error.
func (v *Value) Float64() (float64, error) {
	return v.toFloat()
}

// Float32 returns the value as float32. `v.value` must has the `FloaType` type.
// If not returns an error.
func (v *Value) Float32() (float32, error) {
	r, err := v.toFloat()
	return float32(r), err
}

// toComplex function transforms v.value in a complex128.
// v.value only will transforms, if is type `ComplexType`.
// Any else type returns an error.
func (v *Value) toComplex() (complex128, error) {
	ok := v.checkType(reflect.Complex128)

	if !ok {
		return 0, errors.New("value isn't type complex")
	}

	r, _ := v.value.(ComplexType)
	return r.Value(), nil
}

// Complex128 returns the value as complex128. `v.value` must has the `ComplexType` type.
// If not returns an error.
func (v *Value) Complex128() (complex128, error) {
	return v.toComplex()
}

// Complex64 returns the value as complex64. `v.value` must has the `ComplexType` type.
// If not returns an error.
func (v *Value) Complex64() (complex64, error) {
	r, err := v.toComplex()
	return complex64(r), err
}

// String function transforms v.value in a string.
// v.value only will transforms, if is type `StringType`.
// Any else type returns an error.
func (v *Value) String() (string, error) {
	ok := v.checkType(reflect.String)

	if !ok {
		return "", errors.New("value isn't type string")
	}

	r, _ := v.value.(StringType)
	return r.Value(), nil
}
