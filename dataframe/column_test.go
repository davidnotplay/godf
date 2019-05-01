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

func Test_DataFrame_Column_func(t *testing.T) {
	var df *DataFrame
	var data []mockData
	as := assert.New(t)

	if df, data = makeDataFrameMockData(t); df == nil {
		return
	}

	// column a
	values, err := df.Column("a")
	as.Equal(df.NumberRows(), len(values), "different value number in the column")
	if err != nil {
		as.FailNowf("error getting the column a", "error: %s", err.Error())
		return
	}

	for i, rowData := range data {
		value := values[i]
		valueI, _ := value.Int()

		as.Equalf(rowData.A, valueI, "the value in position %d does not match", i)
	}

	// column b
	values, err = df.Column("b")
	as.Equal(df.NumberRows(), len(values), "different value number in the column")
	if err != nil {
		as.FailNowf("error getting the column b", "error: %s", err.Error())
		return
	}

	for i, rowData := range data {
		value := values[i]
		valueI, _ := value.Int()
		as.Equalf(rowData.B, valueI, "the value in position %d does not match", i)
	}

	// invalid column
	values, err = df.Column("invalid")
	as.Nil(values, "values must be null because the column invalid is not exists")
	as.Equal("column invalid not found", err.Error(), "error message does not match")
}

func Test_DataFrame_ColumnRange_func(t *testing.T) {
	var df *DataFrame
	var data []mockData
	as := assert.New(t)

	if df, data = makeDataFrameMockData(t); df == nil {
		return
	}

	// column a
	values, err := df.ColumnRange("a", 1, 3)
	if err != nil {
		as.FailNowf("error getting the column a", "error: %s", err.Error())
		return
	}

	as.Equal(2, len(values), "different value number in the column")
	for i, rowData := range data[1:3] {
		value := values[i]
		valueI, _ := value.Int()

		as.Equalf(rowData.A, valueI, "the value in position %d does not match", i)
	}

	// column b
	values, err = df.ColumnRange("b", 1, 3)
	if err != nil {
		as.FailNowf("error getting the column b", "error: %s", err.Error())
		return
	}

	as.Equal(2, len(values), "different value number in the column")
	for i, rowData := range data[1:3] {
		value := values[i]
		valueI, _ := value.Int()
		as.Equalf(rowData.B, valueI, "the value in position %d does not match", i)
	}

	// invalid column
	values, err = df.ColumnRange("invalid", 1, 2)
	as.Nil(values, "values must be null because the column invalid is not exists")
	as.Equal("column invalid not found", err.Error(), "error message does not match")
}



func getDataFrameToColumns(t *testing.T) *DataFrame {
	type ds struct {
		A int	     `colName:"col A"`
		B uint	     `colName:"col B"`
		C float32    `colName:"col C"`
		D complex128 `colName:"col D"`
		E string     `colName:"col E"`
	}

	var df *DataFrame

	data := []ds {
		{-1, 1, 1.111, 1.1 -1.1i, "test 1"},
		{-2, 2, 2.222, 2.2 -2.2i, "test 2"},
		{-3, 3, 3.333, 3.3 -3.3i, "test 3"},
		{-4, 4, 4.444, 4.4 -4.4i, "test 4"},
		{-5, 5, 5.555, 5.5 -5.5i, "test 5"},
		{-6, 6, 6.666, 6.6 -6.6i, "test 6"},
		{-7, 7, 7.777, 7.7 -7.7i, "test 7"},
		{-8, 8, 8.888, 8.8 -8.8i, "test 8"},
		{-9, 9, 9.999, 9.9 -9.9i, "test 9"},
	}

	if df = makeDataFrame(data, t); df == nil {
		return nil
	}

	return df
}


func Test_DataFrame_ColumnAsIntRange_func(t *testing.T) {
	df := getDataFrameToColumns(t)
	as := assert.New(t)

	values, err := df.ColumnAsIntRange("col A", 1, 5)
	if err != nil {
		as.FailNowf("error getting values as int", "error: %s", err.Error())
		return
	}

	as.Equalf(len(values), 4, "The number of elements is differents")
	iterator, _ := df.IteratorRange(1, 5)
	i := 0
	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell("col A")
		actual, _ := value.Int64()
		as.Equalf(values[i], actual, "in position %d the values are differents")
		i++
	}


	// fails
	// invalid column
	values, err = df.ColumnAsIntRange("invalid", 1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column invalid not found")

	// invalid column type
	values, err = df.ColumnAsIntRange("col B", 1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column col B is not type int")

	// invalid iterator number
	values, err = df.ColumnAsIntRange("col A", -1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "index must be non-negative number")

}

func Test_DataFrame_ColumnAsInt_func(t *testing.T) {
	df := getDataFrameToColumns(t)
	as := assert.New(t)

	values, err := df.ColumnAsInt("col A")
	if err != nil {
		as.FailNowf("error getting values as int", "error: %s", err.Error())
		return
	}

	as.Equalf(len(values), 9, "The number of elements is differents")
	iterator := df.Iterator()
	i := 0
	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell("col A")
		actual, _ := value.Int64()
		as.Equalf(values[i], actual, "in position %d the values are differents")
		i++
	}


	// fails
	// invalid column
	values, err = df.ColumnAsInt("invalid")
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column invalid not found")

	// invalid column type
	values, err = df.ColumnAsInt("col B")
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column col B is not type int")
}

func Test_DataFrame_ColumnAsUintRange_func(t *testing.T) {
	df := getDataFrameToColumns(t)
	as := assert.New(t)

	values, err := df.ColumnAsUintRange("col B", 1, 5)
	if err != nil {
		as.FailNowf("error getting values as int", "error: %s", err.Error())
		return
	}

	as.Equalf(len(values), 4, "The number of elements is differents")
	iterator, _ := df.IteratorRange(1, 5)
	i := 0
	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell("col B")
		actual, _ := value.Uint64()
		as.Equalf(values[i], actual, "in position %d the values are differents")
		i++
	}


	// fails
	// invalid column
	values, err = df.ColumnAsUintRange("invalid", 1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column invalid not found")

	// invalid column type
	values, err = df.ColumnAsUintRange("col A", 1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column col A is not type uint")

	// invalid iterator number
	values, err = df.ColumnAsUintRange("col B", -1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "index must be non-negative number")

}

func Test_DataFrame_ColumnAsUint_func(t *testing.T) {
	df := getDataFrameToColumns(t)
	as := assert.New(t)

	values, err := df.ColumnAsUint("col B")
	if err != nil {
		as.FailNowf("error getting values as int", "error: %s", err.Error())
		return
	}

	as.Equalf(len(values), 9, "The number of elements is differents")
	iterator := df.Iterator()
	i := 0
	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell("col B")
		actual, _ := value.Uint64()
		as.Equalf(values[i], actual, "in position %d the values are differents")
		i++
	}


	// fails
	// invalid column
	values, err = df.ColumnAsUint("invalid")
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column invalid not found")

	// invalid column type
	values, err = df.ColumnAsUint("col A")
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column col A is not type uint")
}

func Test_DataFrame_ColumnAsFloatRange_func(t *testing.T) {
	df := getDataFrameToColumns(t)
	as := assert.New(t)

	values, err := df.ColumnAsFloatRange("col C", 1, 5)
	if err != nil {
		as.FailNowf("error getting values as int", "error: %s", err.Error())
		return
	}

	as.Equalf(len(values), 4, "The number of elements is differents")
	iterator, _ := df.IteratorRange(1, 5)
	i := 0
	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell("col C")
		actual, _ := value.Float64()
		as.Equalf(values[i], actual, "in position %d the values are differents", i)
		i++
	}

	// fails
	// invalid column
	values, err = df.ColumnAsFloatRange("invalid", 1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column invalid not found")

	// invalid column type
	values, err = df.ColumnAsFloatRange("col A", 1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column col A is not type float")

	// invalid iterator number
	values, err = df.ColumnAsFloatRange("col C", -1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "index must be non-negative number")
}

func Test_DataFrame_ColumnAsFloat_func(t *testing.T) {
	df := getDataFrameToColumns(t)
	as := assert.New(t)

	values, err := df.ColumnAsFloat("col C")
	if err != nil {
		as.FailNowf("error getting values as int", "error: %s", err.Error())
		return
	}

	as.Equalf(len(values), 9, "The number of elements is differents")
	iterator := df.Iterator()
	i := 0
	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell("col C")
		actual, _ := value.Float64()
		as.Equalf(values[i], actual, "in position %d the values are differents", i)
		i++
	}

	// fails
	// invalid column
	values, err = df.ColumnAsFloat("invalid")
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column invalid not found")

	// invalid column type
	values, err = df.ColumnAsFloat("col A")
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column col A is not type float")
}

func Test_DataFrame_ColumnAsComplexRange_func(t *testing.T) {
	df := getDataFrameToColumns(t)
	as := assert.New(t)

	values, err := df.ColumnAsComplexRange("col D", 1, 5)
	if err != nil {
		as.FailNowf("error getting values as int", "error: %s", err.Error())
		return
	}

	as.Equalf(len(values), 4, "The number of elements is differents")
	iterator, _ := df.IteratorRange(1, 5)
	i := 0
	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell("col D")
		actual, _ := value.Complex128()
		as.Equalf(values[i], actual, "in position %d the values are differents", i)
		i++
	}

	// fails
	// invalid column
	values, err = df.ColumnAsComplexRange("invalid", 1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column invalid not found")

	// invalid column type
	values, err = df.ColumnAsComplexRange("col A", 1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column col A is not type complex")

	// invalid iterator number
	values, err = df.ColumnAsComplexRange("col D", -1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "index must be non-negative number")
}

func Test_DataFrame_ColumnAsComplex_func(t *testing.T) {
	df := getDataFrameToColumns(t)
	as := assert.New(t)

	values, err := df.ColumnAsComplex("col D")
	if err != nil {
		as.FailNowf("error getting values as int", "error: %s", err.Error())
		return
	}

	as.Equalf(len(values), 9, "The number of elements is differents")
	iterator := df.Iterator()
	i := 0
	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell("col D")
		actual, _ := value.Complex128()
		as.Equalf(values[i], actual, "in position %d the values are differents", i)
		i++
	}

	// fails
	// invalid column
	values, err = df.ColumnAsComplex("invalid")
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column invalid not found")

	// invalid column type
	values, err = df.ColumnAsComplex("col A")
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column col A is not type complex")
}

func Test_DataFrame_ColumnAsStringRange_func(t *testing.T) {
	df := getDataFrameToColumns(t)
	as := assert.New(t)

	values, err := df.ColumnAsStringRange("col E", 1, 5)
	if err != nil {
		as.FailNowf("error getting values as int", "error: %s", err.Error())
		return
	}

	as.Equalf(len(values), 4, "The number of elements is differents")
	iterator, _ := df.IteratorRange(1, 5)
	i := 0
	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell("col E")
		actual, _ := value.Str()
		as.Equalf(values[i], actual, "in position %d the values are differents", i)
		i++
	}

	// fails
	// invalid column
	values, err = df.ColumnAsStringRange("invalid", 1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column invalid not found")

	// invalid column type
	values, err = df.ColumnAsStringRange("col A", 1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column col A is not type string")

	// invalid iterator number
	values, err = df.ColumnAsStringRange("col E", -1, 5)
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "index must be non-negative number")
}

func Test_DataFrame_ColumnAsString_func(t *testing.T) {
	df := getDataFrameToColumns(t)
	as := assert.New(t)

	values, err := df.ColumnAsString("col E")
	if err != nil {
		as.FailNowf("error getting values as int", "error: %s", err.Error())
		return
	}

	as.Equalf(len(values), 9, "The number of elements is differents")
	iterator := df.Iterator()
	i := 0
	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell("col E")
		actual, _ := value.Str()
		as.Equalf(values[i], actual, "in position %d the values are differents", i)
		i++
	}

	// fails
	// invalid column
	values, err = df.ColumnAsString("invalid")
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column invalid not found")

	// invalid column type
	values, err = df.ColumnAsString("col A")
	as.Nil(values, "values is not nil when the column is invalid")
	as.Equal(err.Error(), "column col A is not type string")
}
