package dataframe

import (
	"github.com/stretchr/testify/assert"
	"testing"
)


func Test_newIterator_func(t *testing.T) {
	var df *DataFrame
	as := assert.New(t)
	if df, _ = makeDataFrameMockData(t); df == nil {
		return
	}
	iterator := newIterator(df)
	as.Equal(df, iterator.df, "the DataFrame addresses are differents")
	as.Equal(0, iterator.pos, "the new iterator position is not 0")
}

func Test_Next_func(t *testing.T) {
	var df *DataFrame
	var data []mockData
	as := assert.New(t)

	if df, data = makeDataFrameMockData(t); df == nil {
		return
	}

	iterator := newIterator(df)

	for i, dataRow := range data {
		row, cont := iterator.Next()
		as.True(cont, "Continue must be true")

		c, _ := row.Cell("a")
		a, _ := c.Int()
		as.Equalf(dataRow.A, a, "the position %d, column a does not match", i)
		c, _ = row.Cell("b")
		b, _ := c.Int()
		as.Equalf(dataRow.B, b, "the position %d, column b does not match", i)
	}

	// Check if the iterator has finished.
	row, cont := iterator.Next()
	as.Nil(row.df, "the iterator has finished. The row must be nil")
	as.False(cont, "the flag must be false, because the iterator has reached to the end")

	// Check again.
	row, cont = iterator.Next()
	as.Nil(row.df, "the iterator has finished. The row must be nil")
	as.False(cont, "the flag must be false, because the iterator has reached to the end")
}

func Test_Current_func(t *testing.T) {
	var df *DataFrame
	var data []mockData
	as := assert.New(t)

	if df, data = makeDataFrameMockData(t); df == nil {
		return
	}

	iterator := newIterator(df)

	for i, dataRow := range data {
		row := iterator.Current()
		c, _ := row.Cell("a")
		a, _ := c.Int()
		as.Equalf(dataRow.A, a, "the position %d, column a does not match", i)
		c, _ = row.Cell("b")
		b, _ := c.Int()
		as.Equalf(dataRow.B, b, "the position %d, column b does not match", i)
		iterator.Next()
	}

	row := iterator.Current()
	as.Nil(row.df, "the iterator has reached to the end. The row should be null")
	row = iterator.Current()
	as.Nil(row.df, "the iterator has reached to the end. The row should be null")
}

func Test_Position_func(t *testing.T) {
	var df *DataFrame
	as := assert.New(t)
	if df, _ = makeDataFrameMockData(t); df == nil {
		return
	}

	iterator := newIterator(df)
	i := 0
	for ;i < df.handler.Len(); i++ {
		as.Equalf(i, iterator.Position(),
			"the position %d is match with the iterator", i)
		iterator.Next()
	}

	as.Equalf(i, iterator.Position(), "the position %d is match with the iterator", i)
}

func Test_Reset_func(t *testing.T) {
	var df *DataFrame
	as := assert.New(t)
	if df, _ = makeDataFrameMockData(t); df == nil {
		return
	}

	iterator := newIterator(df)
	iterator.Next()
	iterator.Next()
	iterator.Reset()
	as.Equal(0, iterator.Position(), "the iterator has not been restarted")
}
