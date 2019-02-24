/**
Package dataframe provides a DatFrame structure for handle table data.

Create a DataFrame from struct array

It can create a DataFrame using a struct array. The DataFrame columns will be defined in the
golang struct.

Example:
	// Go struct to defined the Dataframe struct.
	type myDataFrame struct {
		// The DataFrame will create a column, type int, with the name colA
		A int `colName:"colA"`
		// This field is public, but as it has not the colName tag,
		// it will be ignored by DataFrame
		B int
		// The DataFrame will create a column, type float32, with the name float
		C float32 `colName:"float"`
	}

	data := []myDataFrame{
		{0, 0, 0},
		{1, 1, 0.1},
		{2, 2, 0.2},
	}

	// Create the new DataFrame
	df, err := NewDataFrameFromStruct(data)

Valid Types

In the struct fields only are valid the next basic types:
	- int
	- int8
	- int16
	- int32
	- int64
	- uint
	- uint8
	- uint16
	- uint32
	- uint64
	- float32
	- float64
	- Complex64
	- complex128
	- string

Also it can use a struct, if it implements the Values interface:
	- IntType
	- UintType
	- FloatType
	- ComplexType
	- StringType

Example:
	// Custom struct
	type Thousand struct {
		value float64
	}

	// Implements the FloatType interface
	func (t *Thousand) String() {
		return fmt.Sprintf("%g", f.v)
	}

	func (f simpleFloatType) Value() float64 {
		return f.v / 1000
	}

	func (f simpleFloatType) Compare(v float64) Comparers {
		if f.v == v {
			return EQUAL
		} else if f.v < v {
			return LESS
		}

		return GREAT
	}

	// DataFrame struct
	type MyStruct {
		Country string `colname:"country"`
		// Custom type
		People Thousand `colname:"people"`
	}

	df, _ := NewDataFrameFromStruct(data)
*/
package dataframe

import (
	"fmt"
)

// DataHandler interface it is used to manipulate the data of DataFrame.
// It is designed to create different input data type for the DataFrame (struct, csv, ...)
type DataHandler interface {
	// Get returns the DataFrame Value of the row and column that match
	// with the function params.
	Get(row int, column string) (Value, error)
	// Len returns the DataFrame rows number.
	Len() int
	// Order the DataFrame rows in function of the DataFrame order field.
	Order() error
}

// DataFrame struct is the main struct in the package.
// It provides a set of methods to get and manipulate all data in dataframe.
type DataFrame struct {
	// cIndexByName cIndexByName field is a map to save the column name and his position.
	cIndexByName map[string]int
	// columns field is an array with the info of all columns.
	columns []column
	// handler field is the object that handles the data.
	// TODO: rename handler to dhandler.
	handler DataHandler
	// order array store the order of the data.
	order []internalOrderColumn
}

// getColumnByName returns the column info of the coln column.
// If the columns is not exists the returns false as second parameter.
func (df *DataFrame) getColumnByName(coln string) (*column, bool) {
	pos, exists := df.cIndexByName[coln]

	if !exists {
		return nil, false
	}

	return &df.columns[pos], true
}

// checkRange checks if the min and max params are valid range index.
// this min and max values are similar to the go sub-slice concept ([min:max]).
// Returns an error if there is an error, or nil if the parameters are valid.
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

// Column returns the values of a DataFrame column in an array.
// Returns an error if the column does not exists.
func (df *DataFrame) Column(colname string) ([]Value, error) {
	return df.ColumnRange(colname, 0, df.NumberRows())
}

// ColumnRange returns a values range of a DataFrame column in an array.
// Returns an error if the column does not exists or the range index is invalid.
func (df *DataFrame) ColumnRange(colname string, min, max int) ([]Value, error) {
	// Check if column colname exists.
	if _, ok := df.cIndexByName[colname]; !ok {
		return nil, fmt.Errorf("column %s not found", colname)
	}

	iterator, err := df.IteratorRange(min, max)
	if err != nil {
		// invalid range index.
		return nil, err
	}

	var values []Value
	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell(colname)
		values = append(values, value)
	}

	return values, nil
}

// Iterator returns a Iterator to access the data rows sequentially.
func (df *DataFrame) Iterator() *Iterator {
	iterator, _ := df.IteratorRange(0, df.NumberRows())
	return iterator
}

// IteratorRange creates a new Iterator with the range specified in the parameters.
// returns an error if the range values (min or max) are invalid.
func (df *DataFrame) IteratorRange(min, max int) (*Iterator, error) {
	return newIterator(df, min, max)
}

// Order orders the DataFrame rows using the newOrder array.
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

	return df.handler.Order()
}
