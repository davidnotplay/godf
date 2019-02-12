package dataframe

import (
	"fmt"
)

type dataRow struct {
	df * DataFrame
	index int
}

// newRow create a new row of the table of the `df` dataframe.
// The row index is defined in the `index` param. If this index is more great or equal than
// dataframe length, it return an error.
func newRow(df *DataFrame, index int) (Row, error) {
	if df.handler.Len() <= index {
		return nil, fmt.Errorf("row %d out of range", index)
	}

	return &dataRow{df, index}, nil
}

// Cell returns the Value of the column `colname` and this row.
// It returns an error if the column does not exists.
func (r *dataRow) Cell(colname string) (Value, error) {
	return r.df.handler.Get(r.index, colname)
}


type Row interface{
	Cell(name string) (Value, error)
}
