package dataframe

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func Test_dataHasValid_func(t *testing.T) {
	as := assert.New(t)

	a := [2]int{}
	sl := []int{}

	as.True(dataHasValidType(a))
	as.True(dataHasValidType(&a))
	as.True(dataHasValidType(sl))
	as.True(dataHasValidType(&sl))

	// invalid types.
	i := 3
	ss := "string"
	b := true
	st := struct{}{}
	as.False(dataHasValidType(i))
	as.False(dataHasValidType(&i))
	as.False(dataHasValidType(ss))
	as.False(dataHasValidType(&ss))
	as.False(dataHasValidType(b))
	as.False(dataHasValidType(&b))
	as.False(dataHasValidType(st))
	as.False(dataHasValidType(&st))
}

func Test_getStructOfData_func(t *testing.T) {
	as := assert.New(t)
	a := [2]struct{ a int }{}
	s := []struct{ a int }{}

	// array
	st, err := getStructOfData(a)
	as.Nil(err)
	as.Equal(st.NumField(), 1)

	// array ptr
	st, err = getStructOfData(&a)
	as.Nil(err)
	as.Equal(st.NumField(), 1)

	// slice
	st, err = getStructOfData(s)
	as.Nil(err, "There an error when is data an array")
	as.Equal(st.NumField(), 1, "st isn't struct.")

	// slice ptr
	st, err = getStructOfData(&s)
	as.Nil(err)
	as.Equal(st.NumField(), 1, "st isn't struct.")
}

func Test_getStructOfData_func_Error(t *testing.T) {
	as := assert.New(t)

	// the param isn't an *array*.
	i := 3
	st, err := getStructOfData(i)
	as.Nil(st)
	as.Equal(err.Error(), "invalid data type. Valid type: array, array ptr, slice, slice ptr")

	// The array datatype isn't a struct.
	st, err = getStructOfData([]int{})
	as.Nil(st)
	as.Equal(err.Error(), "the data type is not a struct")
}

func Test_isExportable_func(t *testing.T) {
	as := assert.New(t)
	s := struct {
		a int
		B int
	}{}

	st := reflect.TypeOf(s)
	as.False(isExportableField(st.Field(0)), "the 'a' field isn't exportable")
	as.True(isExportableField(st.Field(1)), "the 'B' field is exportable")
}

func Test_NewDataFrameFromStruct_func_Struct(t *testing.T) {
	as := assert.New(t)
	type s struct {
		a int
		B int
		C int16   `colName:"c"`
		D float32 `colName:"d"`
		i int
		T string `colName:"ct"`
	}
	data := []s{}

	df, err := NewDataFrameFromStruct(data)

	// Check error
	if err != nil {
		as.FailNowf("dataframe error", "function generate the error %s", err.Error())
		return
	}
	as.Nil(err)

	// Check df.columns
	as.Equal(len(df.columns), 3)

	// Check column 0
	as.False(df.columns[0].hidden, "the column c is hidden")
	as.Equal(df.columns[0].name, "c", "the name of the column is c")
	as.Equal(df.columns[0].ctype, columnType("int"), "c column has an invalid type")
	as.Equal(df.columns[0].index, 2, "the field position in struct is 2")

	// Check column 1
	as.False(df.columns[1].hidden, "the column d is hidden")
	as.Equal(df.columns[1].name, "d", "the name of the column is d")
	as.Equal(df.columns[1].ctype, columnType("float"), "d column has an invalid type")
	as.Equal(df.columns[1].index, 3, "the field position in struct is 2")

	// Check column 2
	as.False(df.columns[2].hidden, "the column ct is hidden")
	as.Equal(df.columns[2].name, "ct", "the name of the column is ct")
	as.Equal(df.columns[2].ctype, columnType("string"), "ct column has an invalid type")
	as.Equal(df.columns[2].index, 5, "the field position in struct is 5")

	// Check indexByNamed
	as.Equal(len(df.cIndexByName), 3, "there are 3 columns, so it must be in map")
	as.Equal(df.cIndexByName["c"], 0, "the 'c' key is in the 0 position of columns array")
	as.Equal(df.cIndexByName["d"], 1, "the 'd' key is in the 1 position of columns array")
	as.Equal(df.cIndexByName["ct"], 2, "the 'ct' key is in the 2 position of columns array")
}

func Test_NewDataFrameFromStruct_func_ValidParam(t *testing.T) {
	as := assert.New(t)

	// check slice
	df, err := NewDataFrameFromStruct([]struct{}{})
	as.NotNil(df, "Only is Nil when there is an error")
	as.Nil(err, "The error must be Nil")

	// check *slice
	df, err = NewDataFrameFromStruct(new([]struct{}))
	as.NotNil(df, "Only is Nil when there is an error")
	as.Nil(err, "The error must be Nil")

	// check array
	df, err = NewDataFrameFromStruct([1]struct{}{})
	as.NotNil(df, "Only is Nil when there is an error")
	as.Nil(err, "The error must be Nil")

	// check *array
	df, err = NewDataFrameFromStruct(new([1]struct{}))
	as.NotNil(df, "Only is Nil when there is an error")
	as.Nil(err, "The error must be Nil")
}

func Test_NewDataFrameFromStruct_func_error(t *testing.T) {
	as := assert.New(t)

	// Invalid param
	df, err := NewDataFrameFromStruct(3)
	as.Nil(df, "there an error, the dataframe must be nil")
	as.Equal("invalid data type. Valid type: array, array ptr, slice, slice ptr",
		err.Error(), "the error message doesn't match")

	df, err = NewDataFrameFromStruct([]int{3})
	as.Nil(df, "there an error, the dataframe must be nil")
	as.Equal("the data type is not a struct", err.Error(), "the error message doesn't match")

	// error in the structs.
	df, err = NewDataFrameFromStruct([]struct {
		a int `colName:"a"`
	}{})
	as.Nil(df, "there an error, the dataframe must be nil")
	as.Equal("the column a is unexportable", err.Error(), "the error message doesn't match")

	df, err = NewDataFrameFromStruct([]struct {
		A int `colName:"a"`
		B int `colName:"a"`
	}{})
	as.Nil(df, "there an error, the dataframe must be nil")
	as.Equal("the column a is duplicated", err.Error(), "the error message doesn't match")

	// error in the type property of the struct.
	df, err = NewDataFrameFromStruct([]struct {
		A bool `colName:"a"`
	}{})
	as.Nil(df, "there an error, the dataframe must be nil")
	as.Equal("in column a: bool type is invalid",
		err.Error(), "the error message doesn't match")

	// error in the type property of the struct.
	df, err = NewDataFrameFromStruct([]struct {
		A struct{} `colName:"a"`
	}{})
	as.Nil(df, "there an error, the dataframe must be nil")
	as.Equal("in column a: type doesn't implements a ValueType",
		err.Error(), "the error message doesn't match")
}

func Test_parseValue_func(t *testing.T) {
	as := assert.New(t)

	// base values
	data := map[columnType]interface{}{
		INT:     3,
		UINT:    uint(3),
		FLOAT:   3.2,
		COMPLEX: 3 + 3i,
		STRING:  "3"}
	for ct, value := range data {
		col := column{false, "test", ct, 0, true}
		valueObj, err := parseValue(reflect.ValueOf(value), col)

		if err != nil {
			as.FailNow("there an error in parse value", err.Error())
			return
		}

		switch ct {
		case INT:
			i, _ := valueObj.Int()
			as.Equal(value, i, "the value isn't match")
		case UINT:
			u, _ := valueObj.Uint()
			as.Equal(value, u, "the value isn't match")
		case FLOAT:
			f, _ := valueObj.Float64()
			as.Equal(value, f, "the value isn't match")
		case COMPLEX:
			c, _ := valueObj.Complex128()
			as.Equal(value, c, "the value isn't match")
		case STRING:
			s, _ := valueObj.String()
			as.Equal(value, s, "the value isn't match")
		}
	}

	// struct values
	data = map[columnType]interface{}{
		INT:     simpleIntType{3},
		UINT:    simpleUintType{3},
		FLOAT:   simpleFloatType{3.2},
		COMPLEX: simpleComplexType{3 + 3i},
		STRING:  simpleStringType{"3"}}
	for ct, value := range data {
		col := column{false, "test", ct, 0, false}
		valueObj, err := parseValue(reflect.ValueOf(value), col)

		if err != nil {
			as.FailNow("there an error in parse value", err.Error())
			return
		}

		switch ct {
		case INT:
			i, _ := valueObj.Int64()
			value := value.(IntType)
			as.Equal(value.Value(), i, "the value isn't match")
		case UINT:
			u, _ := valueObj.Uint64()
			value := value.(UintType)
			as.Equal(value.Value(), u, "the value isn't match")
		case FLOAT:
			f, _ := valueObj.Float64()
			value := value.(FloatType)
			as.Equal(value.Value(), f, "the value isn't match")
		case COMPLEX:
			c, _ := valueObj.Complex128()
			value := value.(ComplexType)
			as.Equal(value.Value(), c, "the value isn't match")
		case STRING:
			s, _ := valueObj.String()
			value := value.(StringType)
			as.Equal(value.Value(), s, "the value isn't match")
		}
	}
	/** @TODO testear errores y panic */
}

func Test_makeRange(t *testing.T) {
	as := assert.New(t)

	a := makeRange(2, 5)
	r := []int{2, 3, 4, 5}
	as.Equal(len(r), len(a), "the number of elements in slice is different")
	for _, e := range r {
		as.Containsf(a, e, "the slice doesn't contains the number %d", e)
	}

	a = makeRange(121, 129)
	r = []int{121, 122, 123, 124, 125, 126, 127, 128, 129}
	as.Equal(len(r), len(a), "the number of elements in slice is different")
	for _, e := range r {
		as.Containsf(a, e, "the slice doesn't contains the number %d", e)
	}
}

// Test_NewDataFrameFromStruct_func_dataHandler checks the dataHandlerStruct struct stored in
// the dataframe struct
func Test_NewDataFrameFromStruct_func_dataHandler(t *testing.T) {
	as := assert.New(t)
	data := []struct {
		A int              `colName:"a"`
		B float32          `colName:"b"`
		C simpleStringType `colName:"s"`
	}{
		{3, 3.2, simpleStringType{"test1"}},
		{4, 4.2, simpleStringType{"test2"}}}

	df, err := NewDataFrameFromStruct(data)

	if err != nil {
		as.FailNow("there an error", err.Error())
	}

	// Check row 1, column a
	v, ok := df.handler.data[0]["a"]
	if !ok {
		as.FailNow("error in column a", "column a not found")
	}
	i, err := v.Int()
	if err != nil {
		as.FailNow("error fetching the value", err.Error())
	}
	as.Equal(3, i, "the values doesn't match")

	// Check row 1, column b
	v, ok = df.handler.data[0]["b"]
	if !ok {
		as.FailNow("error in column b", "column b not found")
	}
	f, err := v.Float32()
	if err != nil {
		as.FailNow("error fetching the value", err.Error())
	}
	as.Equal(float32(3.2), f, "the values doesn't match")

	// Check row 1, column c
	v, ok = df.handler.data[0]["s"]
	if !ok {
		as.FailNow("error in column s", "column s not found")
	}
	s, err := v.String()
	if err != nil {
		as.FailNow("error fetching the value", err.Error())
	}
	as.Equal("test1", s, "the values doesn't match")

	// Check row 2, column a
	v, ok = df.handler.data[1]["a"]
	if !ok {
		as.FailNow("error in column a", "column a not found")
	}
	i, err = v.Int()
	if err != nil {
		as.FailNow("error fetching the value", err.Error())
	}
	as.Equal(4, i, "the values doesn't match")

	// Check row 2, column b
	v, ok = df.handler.data[1]["b"]
	if !ok {
		as.FailNow("error in column b", "column b not found")
	}
	f, err = v.Float32()
	if err != nil {
		as.FailNow("error fetching the value", err.Error())
	}
	as.Equal(float32(4.2), f, "the values doesn't match")

	// Check row 2, column c
	v, ok = df.handler.data[1]["s"]
	if !ok {
		as.FailNow("error in column s", "column s not found")
	}
	s, err = v.String()
	if err != nil {
		as.FailNow("error fetching the value", err.Error())
	}
	as.Equal("test2", s, "the values doesn't match")

	// Check the order field of the data handler.
	as.Equal(2, len(df.handler.order), "the order length is invalid")
	as.Equal(0, df.handler.order[0], "the order is incorrect")
	as.Equal(1, df.handler.order[1], "the order is incorrect")

	// check if dataframehandler has the dataframe as field. // check memory address.
	as.Equal(df, df.handler.dataframe, "the memory address is different")
}
