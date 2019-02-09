package dataframe

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func Test_simpleIntType_struct(t *testing.T) {
	as := assert.New(t)
	var i IntType = simpleIntType{v: 1000}

	// Value function
	as.Equal(int64(1000), i.Value(), "in Value func, the value returned isn't match")

	// Compare function
	as.Equal(LESS, i.Compare(1001), "in Compare func, value is less than the param")
	as.Equal(EQUAL, i.Compare(1000), "in Compare func, value is equal than the param")
	as.Equal(GREAT, i.Compare(999), "in Compare func, value is great than the param")

	// String function
	as.Equal("1000", i.String(), "in String func, the str returned isn't match to the value")
}

func Test_simpleUintType_struct(t *testing.T) {
	as := assert.New(t)
	var i UintType = simpleUintType{v: 1000}

	// Value function
	as.Equal(uint64(1000), i.Value(), "in Value func, the value returned isn't match")

	// Compare function
	as.Equal(LESS, i.Compare(1001), "in Compare func, value is less than the param")
	as.Equal(EQUAL, i.Compare(1000), "in Compare func, value is equal than the param")
	as.Equal(GREAT, i.Compare(999), "in Compare func, value is great than the param")

	// String function
	as.Equal("1000", i.String(), "in String func, the str returned isn't match to the value")
}

func Test_simpleFloatType_struct(t *testing.T) {
	as := assert.New(t)
	var i FloatType = simpleFloatType{v: 100.10}

	// Value function
	as.Equal(float64(100.10), i.Value(), "in Value func, the value returned isn't match")

	// Compare function
	as.Equal(LESS, i.Compare(100.11), "in Compare func, value is less than the param")
	as.Equal(EQUAL, i.Compare(100.10), "in Compare func, value is equal than the param")
	as.Equal(GREAT, i.Compare(100.09), "in Compare func, value is great than the param")

	// String function
	as.Equal("100.1",
		i.String(),
		"in String func, the str returned isn't match to the value")
}

func Test_simpleComplexType_struct(t *testing.T) {
	as := assert.New(t)
	var c ComplexType = simpleComplexType{v: 100 + 10i}

	// Value function
	as.Equal(complex(100, 10), c.Value(), "in Value func, the value returned isn't match")

	// String function
	as.Equal("100+10i",
		c.String(),
		"in String func, the str returned isn't match to the value")

	// Compare function
	as.Equal(EQUAL, c.Compare(100+10i), "in Compare func, value is equal than the param")

	// real part
	as.Equal(LESS,
		c.Compare(101+10i),
		"in Compare func, the real part of value is less than the real part of param")
	as.Equal(GREAT,
		c.Compare(99+10i),
		"in Compare func, the real part of value is great than the real part of param")

	// Imaginary part.
	as.Equal(LESS,
		c.Compare(100+11i),
		"in Compare func, the imag. part of value is less than the imag. part of param")
	as.Equal(GREAT,
		c.Compare(100+9i),
		"in Compare func, the imag. part of value is great than the imag. part of param")

}

func Test_simpleStringType_struct(t *testing.T) {
	as := assert.New(t)
	var s StringType = simpleStringType{v: "test"}

	// Value function
	as.Equal("test", s.Value(), "in Value func, the value returned isn't match")

	// String function
	as.Equal("test", s.String(), "in String func, the str returned isn't match to the value")

	// Compare function
	as.Equal(EQUAL, s.Compare("test"), "in Compare func, value is equal than the param")
	as.Equal(LESS, s.Compare("tfst"), "in Compare func, value is less than the param")
	as.Equal(GREAT, s.Compare("tdst"), "in Compare func, value is great than the param")
}

func Test_newValue_func(t *testing.T) {
	var v *Value
	var err error
	as := assert.New(t)

	// int
	i := simpleIntType{3}
	v, err = newValue(i)
	as.Nil(err, "the error isn't nil")
	as.Equal(i, v.value, "value stored in Value struct isn't match")

	// uint
	u := simpleUintType{3}
	v, err = newValue(u)
	as.Nil(err, "the error isn't nil")
	as.Equal(u, v.value, "value stored in Value struct isn't match")

	// float
	f := simpleFloatType{3}
	v, err = newValue(f)
	as.Nil(err, "the error isn't nil")
	as.Equal(f, v.value, "value stored in Value struct isn't match")

	// complex
	c := simpleComplexType{3 - 2i}
	v, err = newValue(c)
	as.Nil(err, "the error isn't nil")
	as.Equal(c, v.value, "value stored in Value struct isn't match")

	// string
	s := simpleStringType{"test"}
	v, err = newValue(s)
	as.Nil(err, "the error isn't nil")
	as.Equal(s, v.value, "value stored in Value struct isn't match")
}

func Test_newValue_func_errors(t *testing.T) {
	as := assert.New(t)
	v, err := newValue(3)

	as.Nil(v, "value must be nil when there is an error")
	as.Equal("the value isn't a value type", err.Error(), "the error message doesn't match")

}

func Test_checkType_func(t *testing.T) {
	as := assert.New(t)

	// valid values.
	values := map[reflect.Kind]interface{}{
		reflect.Int:        simpleIntType{1},
		reflect.Uint:       simpleUintType{1},
		reflect.Float64:    simpleFloatType{1},
		reflect.Complex128: simpleComplexType{1 + 0i},
		reflect.String:     simpleStringType{"test"}}

	for t, v := range values {
		value, _ := newValue(v)
		as.Truef(value.checkType(t), "the type %s isn't match with the value", t.String())
	}

	// invalid values
	values = map[reflect.Kind]interface{}{
		reflect.Int:        simpleUintType{1},
		reflect.Uint:       simpleFloatType{1},
		reflect.Float64:    simpleComplexType{1},
		reflect.Complex128: simpleStringType{"test"},
		reflect.String:     simpleIntType{3}}

	for t, v := range values {
		value, _ := newValue(v)
		as.Falsef(value.checkType(t), "the type %s is match with the value", t.String())
	}

	// panic function
	value := Value{3}
	panicf := func() {
		value.checkType(reflect.Int)
	}

	as.PanicsWithValue(
		"invalid value type",
		panicf,
		"the function isn't in panic or the panic message is invalid")
}

func Test_IntType_func(t *testing.T) {
	as := assert.New(t)
	v, _ := newValue(simpleIntType{3})
	i, err := v.IntType()

	if err != nil {
		as.FailNowf(
			"error generated when it created a new value",
			"error: %s", err.Error())
		return
	}

	as.Equal(int64(3), i.Value(), "The value isn't match.")

	// Error invalid type
	v, _ = newValue(simpleUintType{3})
	u, err := v.IntType()

	as.Equal(int64(0), u.Value(), "There is an error, the value must be 0")
	as.Equal("value type is not int", err.Error(), "There is an error, the value must be 0")
}

func Test_toInt_func(t *testing.T) {
	as := assert.New(t)

	// Getting the int value.
	v, _ := newValue(simpleIntType{3})
	i, err := v.toInt()
	as.Nil(err, "there is an error fetching the integer data")
	as.Equal(int64(3), i, "the number returned isn't match")

	// Error transforming the value in int.
	v, _ = newValue(simpleFloatType{3})
	i, err = v.toInt()
	as.Equal(int64(0), i, "the value must be 0 when there is an error")
	as.Equal("value type is not int", err.Error(), "the error message isn't match")
}

func Test_IntX_func(t *testing.T) {
	as := assert.New(t)
	v, _ := newValue(simpleIntType{100})

	r1, _ := v.Int64()
	as.Equal(int64(100), r1, "the value returned isn't match")

	r2, _ := v.Int()
	as.Equal(int(100), r2, "the value returned isn't match")

	r3, _ := v.Int32()
	as.Equal(int32(100), r3, "the value returned isn't match")

	r4, _ := v.Int16()
	as.Equal(int16(100), r4, "the value returned isn't match")

	r5, _ := v.Int8()
	as.Equal(int8(100), r5, "the value returned isn't match")
}

func Test_UintType_func(t *testing.T) {
	as := assert.New(t)
	v, _ := newValue(simpleUintType{3})
	i, err := v.UintType()

	if err != nil {
		as.FailNowf(
			"error generated when it created a new value",
			"error: %s", err.Error())
		return
	}

	as.Equal(uint64(3), i.Value(), "The value isn't match.")

	// Error
	v, _ = newValue(simpleFloatType{3})
	u, err := v.UintType()

	as.Equal(uint64(0), u.Value(), "There is an error, the value must be 0")
	as.Equal("value type is not uint", err.Error(), "There is an error, the value must be 0")
}

func Test_toUint_func(t *testing.T) {
	as := assert.New(t)

	// Getting the int value.
	v, _ := newValue(simpleUintType{3})
	u, err := v.toUint()
	as.Nil(err, "there is an error fetching the integer data")
	as.Equal(uint64(3), u, "the number returned isn't match")

	// Error transforming the value in int.
	v, _ = newValue(simpleFloatType{3})
	u, err = v.toUint()
	as.Equal(uint64(0), u, "the value must be 0 when there is an error")
	as.Equal("value type is not uint", err.Error(), "the error message isn't match")
}

func Test_UintX_func(t *testing.T) {
	as := assert.New(t)
	v, _ := newValue(simpleUintType{100})

	r1, _ := v.Uint64()
	as.Equal(uint64(100), r1, "the value returned isn't match")

	r2, _ := v.Uint()
	as.Equal(uint(100), r2, "the value returned isn't match")

	r3, _ := v.Uint32()
	as.Equal(uint32(100), r3, "the value returned isn't match")

	r4, _ := v.Uint16()
	as.Equal(uint16(100), r4, "the value returned isn't match")

	r5, _ := v.Uint8()
	as.Equal(uint8(100), r5, "the value returned isn't match")
}

func Test_FloatType_func(t *testing.T) {
	as := assert.New(t)
	v, _ := newValue(simpleFloatType{3})
	i, err := v.FloatType()

	if err != nil {
		as.FailNowf(
			"error generated when it created a new value",
			"error: %s", err.Error())
		return
	}

	as.Equal(float64(3), i.Value(), "The value isn't match.")

	// Error
	v, _ = newValue(simpleUintType{3})
	u, err := v.FloatType()

	as.Equal(float64(0), u.Value(), "There is an error, the value must be 0")
	as.Equal("value type is not float", err.Error(), "There is an error, the value must be 0")
}

func Test_toFloat_func(t *testing.T) {
	as := assert.New(t)

	// Getting the int value.
	v, _ := newValue(simpleFloatType{float64(3)})
	f, err := v.toFloat()
	as.Nil(err, "there is an error fetching the integer data")
	as.Equal(float64(3), f, "the number returned isn't match")

	// Error transforming the value in int.
	v, _ = newValue(simpleUintType{3})
	f, err = v.toFloat()
	as.Equal(float64(0), f, "the value must be 0 when there is an error")
	as.Equal("value type is not float", err.Error(), "the error message isn't match")
}

func Test_FloatX_func(t *testing.T) {
	as := assert.New(t)
	v, _ := newValue(simpleFloatType{100})

	r1, _ := v.Float64()
	as.Equal(float64(100), r1, "the value returned isn't match")

	r2, _ := v.Float32()
	as.Equal(float32(100), r2, "the value returned isn't match")
}

func Test_ComplexType_func(t *testing.T) {
	as := assert.New(t)
	v, _ := newValue(simpleComplexType{3})
	i, err := v.ComplexType()

	if err != nil {
		as.FailNowf(
			"error generated when it created a new value",
			"error: %s", err.Error())
		return
	}

	as.Equal(complex128(3), i.Value(), "The value isn't match.")

	// Error
	v, _ = newValue(simpleUintType{3})
	u, err := v.ComplexType()

	as.Equal(complex128(0), u.Value(), "There is an error, the value must be 0")
	as.Equal("value type is not complex", err.Error(), "There is an error, the value must be 0")
}

func Test_toComplex_func(t *testing.T) {
	as := assert.New(t)

	// Getting the int value.
	v, _ := newValue(simpleComplexType{3 + 3i})
	c, err := v.toComplex()
	as.Nil(err, "there is an error fetching the integer data")
	as.Equal(3+3i, c, "the number returned isn't match")

	// Error transforming the value in int.
	v, _ = newValue(simpleUintType{3})
	c, err = v.toComplex()
	as.Equal(complex128(0), c, "the value must be 0 when there is an error")
	as.Equal("value type is not complex", err.Error(), "the error message isn't match")
}

func Test_ComplexX_func(t *testing.T) {
	as := assert.New(t)
	v, _ := newValue(simpleComplexType{3 - 10i})

	r1, _ := v.Complex128()
	as.Equal(complex128(3-10i), r1, "the value returned isn't match")

	r2, _ := v.Complex64()
	as.Equal(complex64(3-10i), r2, "the value returned isn't match")
}

func Test_StringType_func(t *testing.T) {
	as := assert.New(t)
	v, _ := newValue(simpleStringType{"test"})
	i, err := v.StringType()

	if err != nil {
		as.FailNowf(
			"error generated when it created a new value",
			"error: %s", err.Error())
		return
	}

	as.Equal("test", i.Value(), "The value isn't match.")

	// Error
	v, _ = newValue(simpleUintType{3})
	u, err := v.StringType()

	as.Equal("", u.Value(), "There is an error, the value must be an empty string")
	as.Equal("value type is not string",
		err.Error(),
		"There is an error, the value must be an empty string")
}

func Test_String_func(t *testing.T) {
	as := assert.New(t)

	// Getting the int value.
	v, _ := newValue(simpleStringType{"test"})
	s, err := v.String()
	as.Nil(err, "there is an error fetching the integer data")
	as.Equal("test", s, "the number returned isn't match")

	// Error transforming the value in int.
	v, _ = newValue(simpleUintType{3})
	s, err = v.String()
	as.Equal("", s, "the value must be 0 when there is an error")
	as.Equal("value type is not string", err.Error(), "the error message isn't match")
}
