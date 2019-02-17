package dataframe

import (
	"fmt"
)

type DataHandler interface {
	Get(row int, column string) (Value, error)
	order() error
	Len() int
}

type DataFrame struct {
	cIndexByName map[string]int
	columns      []column
	// TODO: rename handler to dhandler.
	handler DataHandler
	order   []internalOrderColumn
}

// getColumnByName returns the DataFrame column that his name
// match with the `coln` param. The second param returned is a flag
// indicating if the column `coln` exists in DataFrame.
func (df *DataFrame) getColumnByName(coln string) (*column, bool) {
	pos, exists := df.cIndexByName[coln]

	if !exists {
		return nil, false
	}

	return &df.columns[pos], true
}

// checkRange checks if the min and max range index are valid.
// The min and max values are similar to slice range [min:max].
// Whether min and max are valid returns nil.
func (df *DataFrame) checkRange(min, max int) error {
	if min < 0 || max < 0 {
		return fmt.Errorf("index must be non-negative number")
	}

	if min > max {
		return fmt.Errorf("max index < min index")
	}

	lenn := df.NumberRows()

	if min > lenn || max > lenn {
		return fmt.Errorf("index out of range")
	}

	return nil
}

// Headers returns the columns header of dataframe.
// the columns hidden are ignored.
func (df *DataFrame) Headers() []string {
	header := []string{}
	for _, col := range df.columns {
		header = append(header, col.name)
	}
	return header
}

// NumberRows returns the number of rows in DataFrame.
func (df *DataFrame) NumberRows() int {
	return df.handler.Len()
}

// Iterator creates and returns a new Iterator to DataFrame data.
func (df *DataFrame) Iterator() *Iterator {
	iterator, _ := df.IteratorRange(0, df.NumberRows())
	return iterator
}

// IteratorRange creates a new Iterator with the range specified in the parameters.
// returns an error if the range values (min or max) are invalid.
func (df *DataFrame) IteratorRange(min, max int) (*Iterator, error) {
	return newIterator(df, min, max)
}

// Order the dataframe rows using the newOrder array.
// Returns an error if the column name is not exists.
func (df *DataFrame) Order(newOrder ...OrderColumn) error {
	df.order = []internalOrderColumn{}
	for _, extOrder := range newOrder {
		// check if the colums exists.
		col, exists := df.getColumnByName(extOrder.Name)
		if !exists {
			return fmt.Errorf("The column %s doesn't exists", extOrder.Name)
		}

		df.order = append(df.order, internalOrderColumn{col, extOrder.Order})
	}

	return df.handler.order()
}
