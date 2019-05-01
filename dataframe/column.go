package dataframe

import (
	"fmt"
	"reflect"
)

// columnType indicates the basic type of the column.
type columnType string

// Constans with the valid basic types for the columns.
const (
	INT     columnType = "int"
	UINT    columnType = "uint"
	FLOAT   columnType = "float"
	COMPLEX columnType = "complex"
	STRING  columnType = "string"
)

// getColumnTypeFromString returns one of the columnType constant depending of the param.
// Whether `str` param isn't match with any columnType returns an error.
func getColumnTypeFromString(str string) (columnType, error) {
	coltype := columnType(str)

	switch coltype {
	case INT, UINT, FLOAT, COMPLEX, STRING:
		return coltype, nil
	default:
		return columnType(""), fmt.Errorf("%s is an invalid type", str)
	}
}

// getColumnTypeFromType gets a columType const depending of the t param.
// t params must contains one of the next types:
// 	- basic type: (int, uint, float, complex, string)
//	- struct or interface that implements a ValueType (IntType, FloatType...)
//	- Ptr to basic type: (int, uint, float, complex, string)
//	- Ptr to struct or interface that implements a ValueType (IntType, FloatType...)
//
// The function returns the columnType. One bool value, indicating if the the type of t param
// is a basic type. And an error if t contains and invalid type.
func getColumnTypeFromType(t reflect.Type) (columnType, bool, error) {
	k := t.Kind()

	// The type is a ptr. Check then the ptr element.
	if k == reflect.Ptr {
		return getColumnTypeFromType(t.Elem())
	}

	if k == reflect.Struct || k == reflect.Interface {
		// check if t implements some of the ValuesType
		if t.Implements(reflect.TypeOf((*IntType)(nil)).Elem()) {
			return INT, false, nil
		}
		if t.Implements(reflect.TypeOf((*UintType)(nil)).Elem()) {
			return UINT, false, nil
		}
		if t.Implements(reflect.TypeOf((*FloatType)(nil)).Elem()) {
			return FLOAT, false, nil
		}
		if t.Implements(reflect.TypeOf((*ComplexType)(nil)).Elem()) {
			return COMPLEX, false, nil
		}
		if t.Implements(reflect.TypeOf((*StringType)(nil)).Elem()) {
			return STRING, false, nil
		}

		return columnType(""), false, fmt.Errorf("type doesn't implements a ValueType")
	}

	switch k {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return INT, true, nil
	case reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8:
		return UINT, true, nil
	case reflect.Float64, reflect.Float32:
		return FLOAT, true, nil
	case reflect.Complex128, reflect.Complex64:
		return COMPLEX, true, nil
	case reflect.String:
		return STRING, true, nil

	default:
		return columnType(""), false, fmt.Errorf("%s type is invalid", k.String())
	}
}

// Kind returns the Kind type associate to the columnType constants.
// If columnType isn't one of the constants then the function throw a panic message.
func (c columnType) Kind() reflect.Kind {
	switch c {
	case INT:
		return reflect.Int
	case UINT:
		return reflect.Uint
	case FLOAT:
		return reflect.Float64
	case COMPLEX:
		return reflect.Complex128
	case STRING:
		return reflect.String
	default:
		panic("invalid column type")
	}
}

// column is the struct used in the DataFrame column.
type column struct {
	// column name
	name string
	// Column type
	ctype columnType
	// column position in dataframe
	index int
	// flag indicating if is a basic type.
	basicType bool
}

// orderType is the type used when it defines the order of the DataFrame rows.
type orderType int8

// The valid order types.
const (
	ASC  orderType = 1
	DESC orderType = 2
)

// internalOrderColumn is the internal struct used to stored the order of the DataFrame rows.
type internalOrderColumn struct {
	column *column
	order  orderType
}

/*
OrderColumn is the struct used to define the order of the DataFrame rows.

Example:
	data := struct{
		A int `colName:"a"`
		B int `colName:"b"`
	}{
		{1, 1},
		{2, 2},
		{1, 2},
	}

	df, _ := NewDataFrameFromStruct(data)

	df.Order(OrderColumn{"a", ASC}, OrderColumn{"b", DESC})
*/
type OrderColumn struct {
	Name  string    // Column name
	Order orderType // Order type
}

// checkColumnIsValid checks if the colName column exists and if it is the same type that
// ctype param.
func (df *DataFrame) checkColumnIsValid(colName string, ctype columnType) error {

	// Checking if the column exists.
	colIndex, exists := df.cIndexByName[colName]
	if !exists {
		return fmt.Errorf("column %s not found", colName)
	}

	// Checking if the column has got the correct type
	colInfo := df.columns[colIndex]
	if colInfo.ctype != ctype {
		return fmt.Errorf("column %s is not type %s", colName, ctype)
	}

	return nil
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


// ColumnAsIntRange returns the values between the rows min and max of the colName column as
// an array of integers.
func (df *DataFrame) ColumnAsIntRange(colname string, min, max int) ([]int64, error) {
	var values []int64
	err := df.checkColumnIsValid(colname, INT)

	if err != nil {
		return values, err
	}

	iterator, err := df.IteratorRange(min, max)
	if err != nil {
		return values, err
	}

	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell(colname)
		vNumber, _ := value.Int64()
		values = append(values, vNumber)
	}

	return values, nil
}

// ColumnAsInt returns the colName column as an array of integers.
func (df *DataFrame) ColumnAsInt(colname string) ([]int64, error) {
	return df.ColumnAsIntRange(colname, 0, df.NumberRows())
}

// ColumnAsUintRange returns the values between the rows min and max of the colName column as
// an array of unsinged integers.
func (df *DataFrame) ColumnAsUintRange(colname string, min, max int) ([]uint64, error) {
	var values []uint64
	err := df.checkColumnIsValid(colname, UINT)

	if err != nil {
		return values, err
	}

	iterator, err := df.IteratorRange(min, max)
	if err != nil {
		return values, err
	}


	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell(colname)
		vNumber, _ := value.Uint64()
		values = append(values, vNumber)
	}

	return values, nil

}

// ColumnAsUint returns the colName column as an array of unsigned integers.
func (df *DataFrame) ColumnAsUint(colname string) ([]uint64, error) {
	return df.ColumnAsUintRange(colname, 0, df.NumberRows())
}

// ColumnAsFloatRange returns the values between the rows min and max of the colName column as
// an array of floats.
func (df *DataFrame) ColumnAsFloatRange(colname string, min, max int) ([]float64, error) {
	var values []float64
	err := df.checkColumnIsValid(colname, FLOAT)

	if err != nil {
		return values, err
	}

	iterator, err := df.IteratorRange(min, max)
	if err != nil {
		return values, err
	}


	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell(colname)
		vNumber, _ := value.Float64()
		values = append(values, vNumber)
	}

	return values, nil

}

// ColumnAsFloat returns the colName column as an array of floats.
func (df *DataFrame) ColumnAsFloat(colname string) ([]float64, error) {
	return df.ColumnAsFloatRange(colname, 0, df.NumberRows())
}

// ColumnAsComplexRange returns the values between the rows min and max of the colName column as
// an array of complex numbers.
func (df *DataFrame) ColumnAsComplexRange(colname string, min, max int) ([]complex128, error) {
	var values []complex128
	err := df.checkColumnIsValid(colname, COMPLEX)

	if err != nil {
		return values, err
	}

	iterator, err := df.IteratorRange(min, max)
	if err != nil {
		return values, err
	}


	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell(colname)
		vNumber, _ := value.Complex128()
		values = append(values, vNumber)
	}

	return values, nil

}

// ColumnAsComplex returns the colName column as an array of complex numbers.
func (df *DataFrame) ColumnAsComplex(colname string) ([]complex128, error) {
	return df.ColumnAsComplexRange(colname, 0, df.NumberRows())
}

// ColumnAsStringRange returns the values between the rows min and max of the colName column as
// an array of strings.
func (df *DataFrame) ColumnAsStringRange(colname string, min, max int) ([]string, error) {
	var values []string
	err := df.checkColumnIsValid(colname, STRING)

	if err != nil {
		return values, err
	}

	iterator, err := df.IteratorRange(min, max)
	if err != nil {
		return values, err
	}


	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		value, _ := row.Cell(colname)
		vNumber, _ := value.Str()
		values = append(values, vNumber)
	}

	return values, nil

}

// ColumnAsString returns the colName column as an array of strings.
func (df *DataFrame) ColumnAsString(colname string) ([]string, error) {
	return df.ColumnAsStringRange(colname, 0, df.NumberRows())
}

