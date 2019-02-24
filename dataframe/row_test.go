package dataframe

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_newRow_func(t *testing.T) {
	var df *DataFrame
	as := assert.New(t)
	data := []struct {
		A int `colName:"a"`
		B int `colName:"b"`
	}{
		{3, 5}, {4, 1},
	}

	if df = makeDataFrame(data, t); df == nil {
		return
	}

	// Valid index
	validIndex := []int{0, 1}
	for _, i := range validIndex {
		row, err := newRow(df, i)
		if err != nil {
			as.FailNowf(
				"error generated when it created a new row",
				"error: %s", err.Error())
			return
		}
		as.Equal(df, row.df, "the dataframe address is different")
		as.Equal(i, row.index, "the row index is different")
	}

	// Invalid index
	invalidIndex := []int{2, 3}
	for _, i := range invalidIndex {
		row, err := newRow(df, i)
		if row.df != nil {
			as.FailNow("valid row",
				"return a valid row, when it makes a invalid row.")
			return
		}

		as.Equal(fmt.Errorf("row %d out of range", i), err, "error message doesn't match")
	}
}

func Test_Cell_func(t *testing.T) {
	var df *DataFrame
	var data []mockData
	as := assert.New(t)

	if df, data = makeDataFrameMockData(t); df == nil {
		return
	}

	for i, dataRow := range data {
		row, err := newRow(df, i)
		if err != nil {
			as.FailNowf(
				"error generate when it created a new row",
				"error: %s", err.Error())
			return
		}

		c, err := row.Cell("a")
		valueInt, _ := c.Int()
		if err != nil {
			as.FailNowf(
				"error generated when it gets a cell value.",
				"error: %s", err.Error())
			return
		}
		as.Equalf(dataRow.A, valueInt, "in row %d. the a value is not match.", i)
		c, err = row.Cell("b")
		valueInt, _ = c.Int()
		if err != nil {
			as.FailNowf(
				"error generated when it gets a cell value.",
				"error: %s", err.Error())
			return
		}
		as.Equalf(dataRow.B, valueInt, "in row %d. the b value is not match.", i)

	}

	// invalid column
	row, err := newRow(df, 0)
	if err != nil {
		as.FailNowf(
			"error generate when it created a new row",
			"error: %s", err.Error())
		return
	}

	_, err = row.Cell("invalid column")
	as.Equal(
		"column invalid column not found", err.Error(),
		"the error message is wrong.")
}
