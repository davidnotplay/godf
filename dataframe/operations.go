package dataframe

import (
	"fmt"
)

// OperationBase is the base struct for all operations.
type OperationBase struct {
	colName string	// Column name
}

// OperationBaseInt is the base for all operations type int.
type OperationBaseInt struct {
	OperationBase
	Total int64	// total result of the operation.
}

// OperationBaseUint is the base for all operations type uint.
type OperationBaseUint struct {
	OperationBase
	Total uint64	// total result of the operation.
}

// OperationBaseFloat is the base for all operations type float.
type OperationBaseFloat struct {
	OperationBase
	Total float64	// total result of the operation.
}

// OperationBaseComplex is the base for all operations type complex.
type OperationBaseComplex struct {
	OperationBase
	Total complex128 // total result of the operation.
}

// OperationSumInt is a struct used to sum all values of a DataFrame column type int.
type OperatrionSumInt struct {
	OperationBaseInt
}

//F sum the value of the Cell Colname, fetched from r, with the total value: Total.
func (o *OperatrionSumInt)F(r *Row) error {
	v, _ := r.Cell(o.colName)
	result, _ := v.Int64()
	o.Total += result
	return nil
}

// OperationSumUint is a struct used to sum all values of a DataFrame column type uint.
type OperatrionSumUint struct {
	OperationBaseUint
}

//F sum the value of the Cell Colname, fetched from r, with the total value: Total.
func (o *OperatrionSumUint)F(r *Row) error {
	v, _ := r.Cell(o.colName)
	result, _ := v.Uint64()
	o.Total += result
	return nil
}

// OperationSumFloat is a struct used to sum all values of a DataFrame column type float.
type OperatrionSumFloat struct {
	OperationBaseFloat
}

//F sum the value of the Cell Colname, fetched from r, with the total value: Total.
func (o *OperatrionSumFloat)F(r *Row) error {
	v, _ := r.Cell(o.colName)
	result, _ := v.Float64()
	o.Total += result
	return nil
}

// OperatrionSumComplex is a struct used to sum all values of a DataFrame column type complex.
type OperatrionSumComplex struct {
	OperationBaseComplex
}

//F sum the value of the Cell Colname, fetched from r, with the total value: Total.
func (o *OperationBaseComplex)F(r *Row) error {
	v, _ := r.Cell(o.colName)
	result, _ := v.Complex128()
	o.Total += result
	return nil
}

// Operation Execute the func operation using the DataFrame rows between min and max
func (df *DataFrame) OperationRange(op Operation, min, max int) (error) {
	iterator, err := df.IteratorRange(min, max)
	if err != nil {
		return err
	}

	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		if err := op.F(&row); err != nil {
			return err
		}
	}

	return nil
}

// Operation execxute the func operation in all DataFrame rows.
func (df *DataFrame) Operation(op Operation) error {
	return df.OperationRange(op, 0, df.NumberRows())
}

// Sum sum values of the column colName, between rows min and max.
// Only it can use the function with columns that has a valid type.
// The value returned will depend of the colum type:
//	- int	  int64
//	- uint	  uint64
//	- float	  float32
//	- complex complex128
func (df *DataFrame)SumRange(colName string, min, max int) (interface{}, error) {
	column, exists := df.getColumnByName(colName)

	if !exists {
		return nil, fmt.Errorf("column %s not found", colName)
	}

	switch column.ctype {
	case INT:
		op := OperatrionSumInt{OperationBaseInt{OperationBase{colName}, 0}}
		err := df.OperationRange(&op, min, max)
		return op.Total, err
	case UINT:
		op := OperatrionSumUint{OperationBaseUint{OperationBase{colName}, 0}}
		err := df.OperationRange(&op, min, max)
		return op.Total, err
	case FLOAT:
		op := OperatrionSumFloat{OperationBaseFloat{OperationBase{colName}, 0}}
		err := df.OperationRange(&op, min, max)
		return op.Total, err
	case COMPLEX:
		op := OperatrionSumComplex{OperationBaseComplex{OperationBase{colName}, 0}}
		err := df.OperationRange(&op, min, max)
		return op.Total, err

	default:
		return nil, fmt.Errorf("Sum operation is invalid in column type %s", column.ctype)
	}
}


// Sum sum all values of te column colName. Only it can use the function with columns that has
// a valid type. The value returned will depend of the colum type:
//	- int	  int64
//	- uint	  uint64
//	- float	  float32
//	- complex complex128
func (df *DataFrame)Sum(colname string) (interface{}, error) {
	return df.SumRange(colname, 0, df.NumberRows())
}

/*
Operation interface it is used to craete custom operations with the DataFrame rows.
This interface is used in combination with the DataFrame method Operation and OperationRange.
*/
type Operation interface {
	// F Function will execute in each iteration. In each iteration it is passed the
	// current row of the DataFrame iterator.
	F(*Row) error
}

