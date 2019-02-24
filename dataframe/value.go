package dataframe

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// BaseType interface is the base interface of the *ValueTypes* interfaces.
// These interfaces are used for implements custom types for the DataFrame columns.
// There are one *ValueTypes* interface for each valid basic type:
//	- IntType
//	- UintType
//	- FloatType
//	- ComplexType
//	- StringType
//
// Example:
//	// Float custom type.
//	type Kelvin struct {
//		value float64
//	}
//
//	// implements the interface IntType
//
//	func (k *Kelvin) String() string {
//		return fmt.Sprintf("%g", k.value + 273)
//	}
//
//	func (c *Kelvin) Value() float64 {
//		return c.value + 273
//	}
//
//	func (c *Kelvin) Compare(v float64) Comparers {
//		if c.value == v {
//			return EQUAL
//		}
//
//		if c.value < v {
//			return LESS
//		}
//
//		return GREAT
//	}
type BaseType interface {
	// String returns the DataFrame value stored in the struct as string.
	String() string
}

// IntType interface is used to create custom int types for the DataFrame columns.
type IntType interface {
	BaseType
	// Value returns the DataFrame value stored in the struct as int.
	Value() int64
	// Compare compare the DataFrame value stored in the struct with the param.
	Compare(v int64) Comparers
}

// UintType interface is used to create custom uint types for the DataFrame columns.
type UintType interface {
	BaseType
	// Value returns the DataFrame value stored in the struct as uint.
	Value() uint64
	// Compare compare the DataFrame value stored in the struct with the param.
	Compare(v uint64) Comparers
}

// FloatType interface is used to create custom float types for the DataFrame columns.
type FloatType interface {
	BaseType
	// Value returns the DataFrame value stored in the struct as float.
	Value() float64
	// Compare compare the DataFrame value stored in the struct with the param.
	Compare(v float64) Comparers
}

// ComplexType interface is used to create custom complex types for the DataFrame columns.
type ComplexType interface {
	BaseType
	// Value returns the DataFrame value stored in the struct as complex.
	Value() complex128
	// Compare compare the DataFrame value stored in the struct with the param.
	Compare(v complex128) Comparers
}

// StringType interface is used to create custom string types for the DataFrame columns.
type StringType interface {
	BaseType
	// Value returns the DataFrame value stored in the struct as string.
	Value() string
	// Compare compare the DataFrame value stored in the struct with the param.
	Compare(v string) Comparers
}

// Comparers is the variable type that returns the Compare functions in The *ValueTypes*
type Comparers int8

// Values returned by the compare functions.
const (
	LESS  Comparers = -1
	EQUAL Comparers = 0
	GREAT Comparers = 1
)

// simpleIntType struct is used for the DataFrame when the column is type int
// (int, int64, int32, int16, int8). This struct implements the IntType interface.
type simpleIntType struct {
	v int64
}

// Value returns the value stored in the struct.
func (i simpleIntType) Value() int64 {
	return i.v
}

// Compare compare the value of the struct with the v param.
// Returns -1 whether struct value is less than v.
// Returns 0 whether struct value is equal than v.
// Returns 1 whether struct value is equal than v.
func (i simpleIntType) Compare(v int64) Comparers {
	if i.v == v {
		return EQUAL
	} else if i.v < v {
		return LESS
	}

	return GREAT
}

// String returns the value of the struct as string.
func (i simpleIntType) String() string {
	return fmt.Sprintf("%d", i.v)
}

// simpleUintType struct is used for the DataFrame when the column is type uint
// (uint, uint64, uint32, uint16, uint8). This struct implements the UintType interface.
type simpleUintType struct {
	v uint64
}

// Value returns the value stored in the struct.
func (u simpleUintType) Value() uint64 {
	return u.v
}

// Compare compare the value of the struct with the v param.
// Returns -1 whether struct value is less than v.
// Returns 0 whether struct value is equal than v.
// Returns 1 whether struct value is equal than v.
func (u simpleUintType) Compare(v uint64) Comparers {
	if u.v == v {
		return EQUAL
	} else if u.v < v {
		return LESS
	}

	return GREAT
}

// String returns the value of the struct as string.
func (u simpleUintType) String() string {
	return fmt.Sprintf("%d", u.v)
}

// simpleFloatType struct is used for the DataFrame when the column is type float
// (float32, float64). This struct implements the FloatType interface.
type simpleFloatType struct {
	v float64
}

// Value returns the value stored in the struct.
func (f simpleFloatType) Value() float64 {
	return f.v
}

// Compare compare the value of the struct with the v param.
// Returns -1 whether struct value is less than v.
// Returns 0 whether struct value is equal than v.
// Returns 1 whether struct value is equal than v.
func (f simpleFloatType) Compare(v float64) Comparers {
	if f.v == v {
		return EQUAL
	} else if f.v < v {
		return LESS
	}

	return GREAT
}

// String returns the value of the struct as string.
func (f simpleFloatType) String() string {
	return fmt.Sprintf("%g", f.v)
}

// simpleComplexType struct is used for the DataFrame when the column is type complex
// (complex64, complex128). This struct implements the ComplexType interface.
type simpleComplexType struct {
	v complex128
}

// Value returns the value stored in the struct.
func (c simpleComplexType) Value() complex128 {
	return c.v
}

// Compare compare the value of the struct with the v param.
// Returns -1 whether struct value is less than v.
// Returns 0 whether struct value is equal than v.
// Returns 1 whether struct value is equal than v.
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

// String returns the value of the struct as string.
func (c simpleComplexType) String() string {
	str := fmt.Sprintf("%g", c.v)
	return str[1 : len(str)-1] // remove the parentesis.
}

// simpleStringType struct is used for the DataFrame when the column is type string
// This struct implements the IntType interface.
type simpleStringType struct {
	v string
}

// Value returns the value stored in the struct.
func (s simpleStringType) Value() string {
	return s.v
}

// Compare compare the value of the struct with the v param.
// Returns -1 whether struct value is less than v.
// Returns 0 whether struct value is equal than v.
// Returns 1 whether struct value is equal than v.
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

// String returns the value of the struct as string.
func (s simpleStringType) String() string {
	return s.v
}

// Value is the struct where save a DataFrame cell value.
type Value struct {
	// DataFrame value. The type only must be one of the "ValueTypes"
	value interface{}
}

// newValue creates a new Value using as value the v param.
// Whether v is not a *ValueTypes* then returns an errors.
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

// checkType checks if the value stored in v object implements a valid interface for handle
// the type gived by the t param.
//
// For example: if v.value is type IntType and t is reflect.Int returns true.
// Any else returns false. If v.value is an invalid type then the function execute a panic func.
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

// IntType casts the v.value variable in IntType.
// It generates an error if the casting is impossible.
func (v *Value) IntType() (IntType, error) {
	ok := v.checkType(reflect.Int)

	if !ok {
		return simpleIntType{0}, errors.New("value type is not int")
	}

	r, _ := v.value.(IntType)
	return r, nil
}

// toInt casts the v.value variable in a int. v.value variable only will cast in integer
// if is type IntType. Any else type returns an error.
func (v *Value) toInt() (int64, error) {
	t, err := v.IntType()
	return t.Value(), err
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

// UintType casts the v.value variable in UintType.
// It generates an error if the casting is impossible.
func (v *Value) UintType() (UintType, error) {
	ok := v.checkType(reflect.Uint)

	if !ok {
		return simpleUintType{0}, errors.New("value type is not uint")
	}

	r, _ := v.value.(UintType)
	return r, nil
}

// toUint casts the v.value variable in a int. v.value variable only will cast in uint
// if is type UintType. Any else type returns an error.
func (v *Value) toUint() (uint64, error) {
	i, err := v.UintType()
	return i.Value(), err
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

// FloatType casts the v.value variable in FloatType.
// It generates an error if the casting is impossible.
func (v *Value) FloatType() (FloatType, error) {
	ok := v.checkType(reflect.Float64)

	if !ok {
		return simpleFloatType{0}, errors.New("value type is not float")
	}

	r, _ := v.value.(FloatType)
	return r, nil
}

// toFloat casts the v.value variable in a float. v.value variable only will cast in float
// if is type IntType. Any else type returns an error.
func (v *Value) toFloat() (float64, error) {
	r, err := v.FloatType()
	return r.Value(), err
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

// ComplexType casts the v.value variable in ComplexType.
// It generates an error if the casting is impossible.
func (v *Value) ComplexType() (ComplexType, error) {
	ok := v.checkType(reflect.Complex128)

	if !ok {
		return simpleComplexType{0}, errors.New("value type is not complex")
	}

	r, _ := v.value.(ComplexType)
	return r, nil
}

// toComplex casts the v.value variable in a int. v.value variable only will cast in integer
// if is type ComplexType. Any else type returns an error.
func (v *Value) toComplex() (complex128, error) {
	i, err := v.ComplexType()
	return i.Value(), err
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

// StringType casts the v.value variable in StringType.
// It generates an error if the casting is impossible.
func (v *Value) StringType() (StringType, error) {
	ok := v.checkType(reflect.String)

	if !ok {
		return simpleStringType{""}, errors.New("value type is not string")
	}

	r, _ := v.value.(StringType)
	return r, nil
}

// Str casts the v.value variable in a string. v.value variable only will cast in string
// if is type StringType. Any else type returns an error.
func (v *Value) Str() (string, error) {
	i, err := v.StringType()
	return i.Value(), err
}

// String casts all valid values to string and return they.
// If the value is not valid then throw and panic error.
func (v *Value) String() string {
	val, ok := v.value.(BaseType)

	if !ok {
		panic("value is not a ValueTypes")
	}

	return val.String()
}
