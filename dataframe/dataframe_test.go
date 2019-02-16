package dataframe

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type mockData struct {
	A int `colName:"a"`
	B int `colName:"b"`
}

func makeDataFrameMockData(t *testing.T) (df *DataFrame, md []mockData){
	var err error
	md = []mockData{
		{1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}, {1, 6},
		{2, 1}, {2, 2}, {2, 3}, {2, 4}, {2, 5}, {2, 6},
		{3, 1}, {3, 2}, {3, 3}, {3, 4}, {3, 5}, {3, 6},
		{4, 1}, {4, 2}, {4, 3}, {4, 4}, {4, 5}, {4, 6},
		{5, 1}, {5, 2}, {5, 3}, {5, 4}, {5, 5}, {5, 6},
		{6, 1}, {6, 2}, {6, 3}, {6, 4}, {6, 5}, {6, 6},
	}

	df, err = NewDataFrameFromStruct(md)

	if err != nil {
		as := assert.New(t)
		as.FailNow("error creating DataFrame", "error: %s", err.Error())
	}

	return
}

func makeDataFrame(data interface{}, t *testing.T) (df *DataFrame) {
	var err error
	df, err = NewDataFrameFromStruct(data)

	if err != nil {
		as := assert.New(t)
		as.FailNow("error creating DataFrame", "error: %s", err.Error())
	}

	return
}

func Test_getColumnByName_func(t *testing.T) {
	as := assert.New(t)
	var df *DataFrame
	data := []struct {
		A int `colName:"a"`
		a int
		B float64 `colName:"b"`
		C string  `colName:"c"`
	}{}

	if df = makeDataFrame(data, t); df == nil {
		return
	}

	col, exists := df.getColumnByName("a")
	as.True(exists, "column a not found")
	as.Equal("a", col.name, "The column fetched is invalid")

	col, exists = df.getColumnByName("b")
	as.True(exists, "column b not found")
	as.Equal("b", col.name, "The column fetched is invalid")

	col, exists = df.getColumnByName("c")
	as.True(exists, "column c not found")
	as.Equal("c", col.name, "The column fetched is invalid")

	// column not found:
	col, exists = df.getColumnByName("d")
	as.Nil(col, "column d isn't exists, but the function returned data")
	as.False(exists, "the column d isn't exists, but the function returned the column exists.")
}

func Test_Header_func(t *testing.T) {
	var df *DataFrame
	as := assert.New(t)
	data := []struct {
		A int `colName:"a"`
		a int
		B float64 `colName:"b"`
		C string  `colName:"c"`
	}{}

	if df = makeDataFrame(data, t); df == nil {
		return
	}

	headers := df.Headers()
	results := []string{"a", "b", "c"}
	for _, result := range results {
		as.Containsf(headers, result, "The %s header isn't exists", result)
	}
}

func Test_dataframe_len_func(t *testing.T) {
	var df *DataFrame
	var data []mockData
	as := assert.New(t)


	if df, data = makeDataFrameMockData(t); df == nil {
		return
	}

	as.Equal(len(data), df.NumberRows(), "the dataframe length does not match.")
}

func Test_dataframe_iterator_func(t *testing.T) {
	var df *DataFrame
	as := assert.New(t)

	if df, _ = makeDataFrameMockData(t); df == nil {
		return
	}

	iterator := df.Iterator()
	as.Equal(df, iterator.df, "the dataframe addresses are differents")
	as.Equal(0, iterator.pos, "the iterator position is not 0")
}

func Test_dataframe_order_func(t *testing.T) {
	var df *DataFrame
	as := assert.New(t)
	data := []mockData{
		{3, 5}, {4, 1}, {1, 1}, {1, 2}, {2, 3}, {2, 3}, {3, 4}, {2, 1}}

	if df = makeDataFrame(data, t); df == nil {
		return
	}

	df.Order(OrderColumn{"a", ASC}, OrderColumn{"b", DESC})
	dataOrdered := []mockData{
		{1, 2}, {1, 1}, {2, 3}, {2, 3}, {2, 1}, {3, 5}, {3, 4}, {4, 1},
	}

	for i, r := range dataOrdered {
		a, _ := df.handler.Get(i, "a")
		b, _ := df.handler.Get(i, "b")
		av, _ := a.Int()
		bv, _ := b.Int()
		as.Equalf(r.A, av, "the cell %d a does not match", i)
		as.Equalf(r.B, bv, "the cell %d a does not match", i)
	}
}
