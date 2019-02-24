package dataframe

import (
	"fmt"
)

// Row handles a row DataFrame.
type Row struct {
	// DataFrame instance ptr.
	df *DataFrame
	// Row index in DataFrame.
	index int
}

// newRow creates a new Row of the df DataFrame.
// If index param is more great or equal than DataFrame length, it return an error.
func newRow(df *DataFrame, index int) (Row, error) {
	if df.handler.Len() <= index {
		return Row{nil, 0}, fmt.Errorf("row %d out of range", index)
	}

	return Row{df, index}, nil
}

// Cell returns the value inside of the Row of the colname column.
// If the column does not exists, then returns an error.
func (r *Row) Cell(colname string) (Value, error) {
	return r.df.handler.Get(r.index, colname)
}
