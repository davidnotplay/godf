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
	as.Equal(df.columns[0].name, "c", "the name of the column is c")
	as.Equal(df.columns[0].ctype, columnType("int"), "c column has an invalid type")
	as.Equal(df.columns[0].index, 2, "the field position in struct is 2")

	// Check column 1
	as.Equal(df.columns[1].name, "d", "the name of the column is d")
	as.Equal(df.columns[1].ctype, columnType("float"), "d column has an invalid type")
	as.Equal(df.columns[1].index, 3, "the field position in struct is 2")

	// Check column 2
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
		col := column{"test", ct, 0, true}
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
		col := column{"test", ct, 0, false}
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

	dhs, _ := df.handler.(*dataHandlerStruct)

	// Check row 1, column a
	v, ok := dhs.data[0]["a"]
	if !ok {
		as.FailNow("error in column a", "column a not found")
	}
	i, err := v.Int()
	if err != nil {
		as.FailNow("error fetching the value", err.Error())
	}
	as.Equal(3, i, "the values doesn't match")

	// Check row 1, column b
	v, ok = dhs.data[0]["b"]
	if !ok {
		as.FailNow("error in column b", "column b not found")
	}
	f, err := v.Float32()
	if err != nil {
		as.FailNow("error fetching the value", err.Error())
	}
	as.Equal(float32(3.2), f, "the values doesn't match")

	// Check row 1, column c
	v, ok = dhs.data[0]["s"]
	if !ok {
		as.FailNow("error in column s", "column s not found")
	}
	s, err := v.String()
	if err != nil {
		as.FailNow("error fetching the value", err.Error())
	}
	as.Equal("test1", s, "the values doesn't match")

	// Check row 2, column a
	v, ok = dhs.data[1]["a"]
	if !ok {
		as.FailNow("error in column a", "column a not found")
	}
	i, err = v.Int()
	if err != nil {
		as.FailNow("error fetching the value", err.Error())
	}
	as.Equal(4, i, "the values doesn't match")

	// Check row 2, column b
	v, ok = dhs.data[1]["b"]
	if !ok {
		as.FailNow("error in column b", "column b not found")
	}
	f, err = v.Float32()
	if err != nil {
		as.FailNow("error fetching the value", err.Error())
	}
	as.Equal(float32(4.2), f, "the values doesn't match")

	// Check row 2, column c
	v, ok = dhs.data[1]["s"]
	if !ok {
		as.FailNow("error in column s", "column s not found")
	}
	s, err = v.String()
	if err != nil {
		as.FailNow("error fetching the value", err.Error())
	}
	as.Equal("test2", s, "the values doesn't match")

	// check if dataframehandler has the dataframe as field. // check memory address.
	as.Equal(df, dhs.dataframe, "the memory address is different")

	/** @TODO check the DataFrame order field. */
}

func Test_dataHandlerStruct_Get_func(t *testing.T) {
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
		as.FailNowf("error creating the DataFrame from struct", err.Error())
		return
	}

	value, err := df.handler.Get(0, "a")
	if err != nil {
		as.FailNowf("error fecthing a value", err.Error())
		return
	}

	i, _ := value.Int()
	as.Equal(3, i, "the value fecthed is wrong")

	// test again.
	value, err = df.handler.Get(1, "s")
	if err != nil {
		as.FailNowf("error fecthing a value", err.Error())
		return
	}

	s, _ := value.String()
	as.Equal("test2", s, "the value fecthed is wrong")

	// invalid row
	_, err = df.handler.Get(2, "a")
	as.Equal("row 2 out of range", err.Error(), "error message returned is wrong")

	// invalid column
	_, err = df.handler.Get(0, "c1")
	as.Equal("column c1 not found", err.Error(), "error message returned is wrong")
}

func Test_prepareOrderFuncs_func(t *testing.T) {
	as := assert.New(t)
	data := []struct {
		I int       `colName:"i"`
		U uint      `colName:"u"`
		F float32   `colName:"f"`
		C complex64 `colName:"c"`
		S string    `colName:"s"`
	}{}
	df, _ := NewDataFrameFromStruct(data)

	df.order = []internalOrderColumn{
		{&df.columns[0], ASC},
		{&df.columns[1], ASC},
		{&df.columns[2], ASC},
		{&df.columns[3], ASC},
		{&df.columns[4], ASC},
	}

	// make the order function
	dhs, _ := df.handler.(*dataHandlerStruct)

	// previously the func should be null
	as.Nil(dhs.orderFuncs, "the orderFuncs slice is not empty")

	dhs.prepareOrderFuncs()

	as.Equal(5, len(dhs.orderFuncs), "the length of the array is not match")

	a, _ := newValue(simpleIntType{3})
	b, _ := newValue(simpleIntType{3})
	r, _ := dhs.orderFuncs[0](*a, *b)
	as.Equal(EQUAL, r, "the func returns a different value")

	a, _ = newValue(simpleUintType{3})
	b, _ = newValue(simpleUintType{3})
	r, _ = dhs.orderFuncs[1](*a, *b)
	as.Equal(EQUAL, r, "the func returns a different value")

	a, _ = newValue(simpleFloatType{3})
	b, _ = newValue(simpleFloatType{3})
	r, _ = dhs.orderFuncs[2](*a, *b)
	as.Equal(EQUAL, r, "the func returns a different value")

	a, _ = newValue(simpleComplexType{3})
	b, _ = newValue(simpleComplexType{3})
	r, _ = dhs.orderFuncs[2](*a, *b)
	as.Equal(EQUAL, r, "the func returns a different value")

	a, _ = newValue(simpleStringType{"3"})
	b, _ = newValue(simpleStringType{"3"})
	r, _ = dhs.orderFuncs[2](*a, *b)
	as.Equal(EQUAL, r, "the func returns a different value")
}

func Test_Len_method(t *testing.T) {
	as := assert.New(t)
	data := []struct {
		A int `colName:"a"`
	}{
		{3}, {2}, {1},
	}

	df, err := NewDataFrameFromStruct(data[0:0])
	if err != nil {
		as.FailNowf(
			"error generated when it created a new DataFrame",
			"error: %s", err.Error())
		return
	}

	dhs := df.handler.(*dataHandlerStruct)
	as.Equal(0, dhs.Len(), "the value returned is not valid")

	df, err = NewDataFrameFromStruct(data)
	if err != nil {
		as.FailNowf(
			"error generated when it created a new DataFrame",
			"error: %s", err.Error())
		return
	}

	dhs = df.handler.(*dataHandlerStruct)
	as.Equal(3, dhs.Len(), "the value returned is not valid")
}

func Test_Swap_func(t *testing.T) {
	as := assert.New(t)
	data := []struct {
		A int `colName:"a"`
	}{
		{1}, {2}, {3}, {4}, {5}, {6},
	}
	results := [...]int{1, 2, 3, 4, 5, 6}

	df, err := NewDataFrameFromStruct(data)
	if err != nil {
		as.FailNowf(
			"error generated when it created a new DataFrame",
			"error: %s", err.Error())
		return
	}

	dhs := df.handler.(*dataHandlerStruct)

	// initial data
	for i, row := range dhs.data {
		cell, _ := row["a"]
		num, _ := cell.Int()
		as.Equalf(results[i], num, "the inital value of the %d position is invalid", i)
	}

	// swapping value
	for i := 0; i < dhs.Len(); i++ {
		for j := 0; j < dhs.Len(); j++ {
			results[i], results[j] = results[j], results[i]
			dhs.Swap(i, j)
			celli, _ := dhs.data[i]["a"]
			cellj, _ := dhs.data[j]["a"]
			iv, _ := celli.Int()
			jv, _ := cellj.Int()
			as.Equal(
				results[i], iv,
				"swapping the positions %d %d, the value is invalid", i, j)
			as.Equal(
				results[j], jv,
				"swapping the positions %d %d, the value is invalid", i, j)
		}
	}
}

func Test_Less_func(t *testing.T) {
	type resultStruct struct {
		i int
		j int
		r bool
	}

	as := assert.New(t)
	data := []struct {
		A int `colName:"a"`
		B int `colName:"b"`
	}{
		{1, 1}, {1, 2}, {2, 3}, {2, 3}, {3, 4}, {2, 1},
	}

	df, err := NewDataFrameFromStruct(data)
	if err != nil {
		as.FailNowf(
			"error generated when it created a new DataFrame",
			"error: %s", err.Error())
		return
	}

	dhs := df.handler.(*dataHandlerStruct)

	// single column ASC
	dhs.dataframe.order = []internalOrderColumn{{&dhs.dataframe.columns[0], ASC}}
	dhs.prepareOrderFuncs()
	results := []resultStruct{
		{0, 2, true},
		{0, 1, false},
		{2, 1, false},
	}

	for _, r := range results {
		as.Equal(r.r, dhs.Less(r.i, r.j),
			"the comparison between %d and %d is wrong", r.i, r.j)

	}

	// single column DESC
	dhs.dataframe.order = []internalOrderColumn{{&dhs.dataframe.columns[0], DESC}}
	dhs.prepareOrderFuncs()
	results = []resultStruct{
		{0, 2, false},
		{0, 1, false},
		{2, 1, true},
	}
	for _, r := range results {
		as.Equal(r.r, dhs.Less(r.i, r.j),
			"the comparison between %d and %d is wrong", r.i, r.j)

	}

	// multiple columns ASC
	dhs.dataframe.order = []internalOrderColumn{
		{&dhs.dataframe.columns[0], ASC},
		{&dhs.dataframe.columns[1], ASC},
	}
	dhs.prepareOrderFuncs()
	results = []resultStruct{
		{0, 2, true},
		{0, 1, true},
		{2, 3, false},
		{2, 5, false},
		{3, 4, true},
	}

	for _, r := range results {
		as.Equal(r.r, dhs.Less(r.i, r.j),
			"the comparison between %d and %d is wrong", r.i, r.j)

	}

	// multiple columns DESC
	dhs.dataframe.order = []internalOrderColumn{
		{&dhs.dataframe.columns[0], DESC},
		{&dhs.dataframe.columns[1], DESC},
	}
	dhs.prepareOrderFuncs()
	results = []resultStruct{
		{0, 2, false},
		{0, 1, false},
		{2, 3, false},
		{2, 5, true},
		{3, 4, false},
	}

	for _, r := range results {
		as.Equal(r.r, dhs.Less(r.i, r.j),
			"the comparison between %d and %d is wrong", r.i, r.j)

	}
}

func Test_dataHandlerStruct_order_func(t *testing.T) {
	type dataStruct struct {
		A int `colName:"a"`
		B int `colName:"b"`
	}

	as := assert.New(t)
	data := []dataStruct{
		{3, 5}, {4, 1}, {1, 1}, {1, 2}, {2, 3}, {2, 3}, {3, 4}, {2, 1},
	}

	df, err := NewDataFrameFromStruct(data)
	if err != nil {
		as.FailNowf(
			"error generated when it created a new DataFrame",
			"error: %s", err.Error())
		return
	}

	dhs := df.handler.(*dataHandlerStruct)
	dhs.dataframe.order = []internalOrderColumn{
		{&dhs.dataframe.columns[0], ASC},
		{&dhs.dataframe.columns[1], DESC},
	}
	dhs.order()
	dataOrdered := []dataStruct{
		{1, 2}, {1, 1}, {2, 3}, {2, 3}, {2, 1}, {3, 5}, {3, 4}, {4, 1},
	}

	for i, r := range dataOrdered {
		a, _ := dhs.Get(i, "a")
		b, _ := dhs.Get(i, "b")
		av, _ := a.Int()
		bv, _ := b.Int()
		as.Equalf(r.A, av, "the cell %d a does not match", i)
		as.Equalf(r.B, bv, "the cell %d a does not match", i)
	}
}
