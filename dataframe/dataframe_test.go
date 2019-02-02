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

func Test_ShowAllColumns_func(t *testing.T) {
	as := assert.New(t)
	df, _ := NewDataFrameFromStruct([]struct {
		A int `colName:"a"`
		a int
		B float64 `colName:"b"`
		C string  `colName:"c"`
	}{})

	// First hide all columns
	for i, _ := range df.columns {
		df.columns[i].hidden = true
	}

	df.ShowAllColumns()
	for _, col := range df.columns {
		as.Falsef(col.hidden, "the column %s is hidden", col.name)
	}
}

func Test_HideAllColumns_func(t *testing.T) {
	as := assert.New(t)
	df, _ := NewDataFrameFromStruct([]struct {
		A int `colName:"a"`
		a int
		B float64 `colName:"b"`
		C string  `colName:"c"`
	}{})

	// First show all columns
	for i, _ := range df.columns {
		df.columns[i].hidden = false
	}

	df.HideAllColumns()
	for _, col := range df.columns {
		as.Truef(col.hidden, "the column %s is visible", col.name)
	}
}

func Test_ShowColumns_func(t *testing.T) {
	as := assert.New(t)
	df, _ := NewDataFrameFromStruct([]struct {
		A int `colName:"a"`
		a int
		B float64 `colName:"b"`
		C string  `colName:"c"`
	}{})
	results := map[string]bool{
		"a": false,
		"b": true,
		"c": false}

	df.HideAllColumns()
	err := df.ShowColumns("a", "c")
	if err != nil {
		as.FailNow("the function has generated an error", err.Error())
	}

	for _, col := range df.columns {
		as.Equalf(results[col.name], col.hidden,
			"the column %s is hidden: %s", col.name, results[col.name])
	}

	// show columns error
	err = df.ShowColumns("r")
	as.Equal("the column r doesn't exists", err.Error(), "the error message doesn't match")
}

func Test_HiddenColumns_func(t *testing.T) {
	as := assert.New(t)
	df, _ := NewDataFrameFromStruct([]struct {
		A int `colName:"a"`
		a int
		B float64 `colName:"b"`
		C string  `colName:"c"`
	}{})
	results := map[string]bool{
		"a": true,
		"b": true,
		"c": false}

	df.ShowAllColumns()
	err := df.HideColumns("a", "b")
	if err != nil {
		as.FailNow("the function has generated an error", err.Error())
	}

	for _, col := range df.columns {
		as.Equalf(results[col.name], col.hidden,
			"the column %s is hidden: %s", col.name, results[col.name])
	}

	// hidden columns error
	err = df.HideColumns("r")
	as.Equal("the column r doesn't exists", err.Error(), "the error message doesn't match")
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

	// Fetch the headers after hide the column b
	df.HideColumns("b")
	headers = df.Headers()
	results = []string{"a", "c"}
	for _, result := range results {
		as.Containsf(headers, result, "The %s header isn't exists", result)
	}
}
