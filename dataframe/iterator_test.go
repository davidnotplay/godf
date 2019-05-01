package dataframe

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func makeIterator(df *DataFrame, min, max int, t *testing.T) *Iterator {
	iterator, err := newIterator(df, min, max)

	if err != nil {
		assert.FailNowf(t, "error creating iterator", "error: %d", err.Error())
		return nil
	}

	return iterator
}

func Test_newIterator_func(t *testing.T) {
	var df *DataFrame
	as := assert.New(t)
	if df, _ = makeDataFrameMockData(t); df == nil {
		return
	}

	iterator, err := newIterator(df, 0, df.NumberRows())
	if err != nil {
		as.FailNowf("error creating iterator", "erro: %d", err.Error())

	}
	as.Equal(iterator.df, df, "the DataFrame addresses are differents")
	as.Equal(iterator.pos, 0, "the new iterator position is not 0")
	as.Equal(iterator.min, 0, "the min value is not 0")
	as.Equal(iterator.max, df.NumberRows(), "the max value is not dataframe row numbers")

	// Check range.
	iterator, err = newIterator(df, 2, 6)
	if err != nil {
		as.FailNowf("error creating iterator", "erro: %d", err.Error())

	}
	as.Equal(iterator.df, df, "the DataFrame addresses are differents")
	as.Equal(iterator.pos, 2, "the new iterator position is not 2")
	as.Equal(iterator.min, 2, "the min value is not 2")
	as.Equal(iterator.max, 6, "the max value is not 6")

	// check errors.
	iterator, err = newIterator(df, -1, 3)
	as.Nil(iterator, "the main value returned must be nil, because there is an error")
	as.Equal(
		err.Error(),
		"index must be non-negative number",
		"the error message does not match",
	)

}

func Test_Next_func(t *testing.T) {
	var df *DataFrame
	var iterator *Iterator
	var data []mockData
	as := assert.New(t)

	if df, data = makeDataFrameMockData(t); df == nil {
		return
	}

	if iterator = makeIterator(df, 0, df.NumberRows(), t); iterator == nil {
		return
	}

	for i, dataRow := range data {
		row, cont := iterator.Next()
		as.True(cont, "Continue must be true")

		c, _ := row.Cell("a")
		a, _ := c.Int()
		as.Equalf(a, dataRow.A, "the position %d, column a does not match", i)
		c, _ = row.Cell("b")
		b, _ := c.Int()
		as.Equalf(b, dataRow.B, "the position %d, column b does not match", i)
	}

	// Check if the iterator has finished.
	row, cont := iterator.Next()
	as.Nil(row.df, "the iterator has finished. The row must be nil")
	as.False(cont, "the flag must be false, because the iterator has reached to the end")

	// Check again.
	row, cont = iterator.Next()
	as.Nil(row.df, "the iterator has finished. The row must be nil")
	as.False(cont, "the flag must be false, because the iterator has reached to the end")

	// Iterator with range.
	if iterator = makeIterator(df, 1, 4, t); iterator == nil {
		return
	}
	for i, dataRow := range data[1:4] {
		row, cont := iterator.Next()
		as.True(cont, "Continue must be true")

		c, _ := row.Cell("a")
		a, _ := c.Int()
		as.Equalf(a, dataRow.A, "the position %d, column a does not match", i)
		c, _ = row.Cell("b")
		b, _ := c.Int()
		as.Equalf(b, dataRow.B, "the position %d, column b does not match", i)
	}

	// Check if the iterator has finished.
	row, cont = iterator.Next()
	as.Nil(row.df, "the iterator has finished. The row must be nil")
	as.False(cont, "the flag must be false, because the iterator has reached to the end")

	// Check again.
	row, cont = iterator.Next()
	as.Nil(row.df, "the iterator has finished. The row must be nil")
	as.False(cont, "the flag must be false, because the iterator has reached to the end")

}

func Test_Current_func(t *testing.T) {
	var df *DataFrame
	var iterator *Iterator
	var data []mockData
	as := assert.New(t)

	if df, data = makeDataFrameMockData(t); df == nil {
		return
	}

	if iterator = makeIterator(df, 0, df.NumberRows(), t); iterator == nil {
		return
	}

	for i, dataRow := range data {
		row := iterator.Current()
		c, _ := row.Cell("a")
		a, _ := c.Int()
		as.Equalf(a, dataRow.A, "the position %d, column a does not match", i)
		c, _ = row.Cell("b")
		b, _ := c.Int()
		as.Equalf(b, dataRow.B, "the position %d, column b does not match", i)
		iterator.Next()
	}

	row := iterator.Current()
	as.Nil(row.df, "the iterator has reached to the end. The row should be null")
	row = iterator.Current()
	as.Nil(row.df, "the iterator has reached to the end. The row should be null")

	// check with range
	if iterator = makeIterator(df, 1, 4, t); iterator == nil {
		return
	}

	for i, dataRow := range data[1:4] {
		row := iterator.Current()
		c, _ := row.Cell("a")
		a, _ := c.Int()
		as.Equalf(a, dataRow.A, "the position %d, column a does not match", i)
		c, _ = row.Cell("b")
		b, _ := c.Int()
		as.Equalf(b, dataRow.B, "the position %d, column b does not match", i)
		iterator.Next()
	}

	row = iterator.Current()
	as.Nil(row.df, "the iterator has reached to the end. The row should be null")
	row = iterator.Current()
	as.Nil(row.df, "the iterator has reached to the end. The row should be null")
}

func Test_Position_func(t *testing.T) {
	var i int
	var df *DataFrame
	var iterator *Iterator
	as := assert.New(t)
	if df, _ = makeDataFrameMockData(t); df == nil {
		return
	}

	if iterator = makeIterator(df, 0, df.NumberRows(), t); iterator == nil {
		return
	}

	for i = 0; i < df.handler.Len(); i++ {
		as.Equalf(iterator.Position(), i,
			"the position %d is match with the iterator", i)
		iterator.Next()
	}
	as.Equalf(iterator.Position(), i, "the position %d is match with the iterator", i)

	// check range
	if iterator = makeIterator(df, 1, df.NumberRows()-1, t); iterator == nil {
		return
	}

	for i = 1; i < df.handler.Len()-1; i++ {
		as.Equalf(iterator.Position(), i,
			"the position %d is match with the iterator", i)
		iterator.Next()
	}
	as.Equalf(iterator.Position(), i, "the position %d is match with the iterator", i)
}

func Test_Iterator_Index_func(t *testing.T) {
	var df *DataFrame
	var iterator *Iterator
	as := assert.New(t)
	if df, _ = makeDataFrameMockData(t); df == nil {
		return
	}

	if iterator = makeIterator(df, 0, df.NumberRows(), t); iterator == nil {
		return
	}

	i := 0
	as.Equal(iterator.Index(), i, "the index must be 0")

	for _, cont := iterator.Next(); cont; _, cont = iterator.Next() {
		i++
		as.Equal(iterator.Index(), i, "the index must be %")
	}

}

func Test_Iterator_Reset_func(t *testing.T) {
	var df *DataFrame
	var iterator *Iterator
	as := assert.New(t)
	if df, _ = makeDataFrameMockData(t); df == nil {
		return
	}

	if iterator = makeIterator(df, 0, df.NumberRows(), t); iterator == nil {
		return
	}

	as.Equal(iterator.pos, 0, "the position must be 0")
	as.Equal(iterator.index, 0, "the position must be 0")
	iterator.Next()
	iterator.Next()
	as.Equal(iterator.pos, 2, "the position must be 2")
	as.Equal(iterator.index, 2, "the position must be 2")
	iterator.Reset()
	as.Equal(iterator.pos, 0, "the position must be 0")
	as.Equal(iterator.index, 0, "the position must be 0")

	// check range
	if iterator = makeIterator(df, 2, df.NumberRows(), t); iterator == nil {
		return
	}

	as.Equal(iterator.pos, 2, "the position must be 2")
	as.Equal(iterator.index, 0, "the position must be 0")
	iterator.Next()
	iterator.Next()
	as.Equal(iterator.pos, 4, "the position must be 4")
	as.Equal(iterator.index, 2, "the position must be 2")
	iterator.Reset()
	as.Equal(iterator.pos, 2, "the position must be 2")
	as.Equal(iterator.index, 0, "the position must be 0")
}

