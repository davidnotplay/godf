package dataframe

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_getColumnByName_func(t *testing.T) {
	as := assert.New(t)

	df, err := NewDataFrameFromStruct([]struct {
		A int `colName:"a"`
		a int
		B float64 `colName:"b"`
		C string  `colName:"c"`
	}{})

	if err != nil {
		as.FailNowf("there an error creating the DataFrame", err.Error())
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
	as := assert.New(t)
	df, _ := NewDataFrameFromStruct([]struct {
		A int `colName:"a"`
		a int
		B float64 `colName:"b"`
		C string  `colName:"c"`
	}{})

	headers := df.Headers()
	results := []string{"a", "b", "c"}
	for _, result := range results {
		as.Containsf(headers, result, "The %s header isn't exists", result)
	}
}

func Test_dataframe_iterator_func(t *testing.T) {
	type dataStruct struct {
		A int `colName:"a"`
		B int `colName:"b"`
	}

	as := assert.New(t)
	data := []dataStruct{
		{3, 5}, {4, 1}, {1, 1}, {1, 2},
	}

	df, err := NewDataFrameFromStruct(data)
	if err != nil {
		as.FailNowf(
			"error generated when it created a new DataFrame",
			"error: %s", err.Error())
		return
	}

	iterator := df.Iterator()
	as.Equal(df, iterator.df, "the dataframe addresses are differents")
	as.Equal(0, iterator.pos, "the iterator position is not 0")
}

func Test_dataframe_order_func(t *testing.T) {
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

	df.Order(OrderColumn{"a", ASC}, OrderColumn{"b", DESC})
	dataOrdered := []dataStruct{
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
