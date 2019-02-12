package dataframe

import (
	"fmt"
)

type DataFrame struct {
	cIndexByName map[string]int
	columns      []column
	// @TODO cambiar el nombre de handler to dhandler
	handler DataHandler
	order   []internalOrderColumn
}

type DataHandler interface {
	Get(row int, column string) (Value, error)
	order() error
	Len() int
}

// getColumnByName function returns the DataFrame column that his name
// match with the `coln` param. The second param returned is a flag
// indicating if the column `coln` exists in DataFrame.
func (df *DataFrame) getColumnByName(coln string) (*column, bool) {
	pos, exists := df.cIndexByName[coln]

	if !exists {
		return nil, false
	}

	return &df.columns[pos], true
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
