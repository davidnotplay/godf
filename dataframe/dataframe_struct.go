package dataframe

import (
	"fmt"
	"reflect"
)

type dataFrameStruct struct {
	dataFrameBase
	cIndexByName map[string]int
	columns	     []column
	data         []map[string]Value
}

// dataHasValidType check if `data` is:
//	- array
//	- slice
//	- array ptr
//	- slice ptr
func dataHasValidType(data interface{}) bool {
	t := reflect.TypeOf(data)
	tk := t.Kind()

	if tk == reflect.Array || tk == reflect.Slice {
		return true
	}

	if tk != reflect.Ptr {
		return false
	}

	tk = t.Elem().Kind()
	return tk == reflect.Array || tk == reflect.Slice
}

// getStructOfData get the struct type of the `data` *array*.
// param `data` must be:
//	- array with struct as datatype
//	- slice with struct as datatype
//	- array ptr with struct as datatype
//	- slice ptr with struct as datatype
//
// If `data` is not an *array* or the *array* datatype is not an struct then returns an error.
func getStructOfData(data interface{}) (reflect.Type, error) {
	if !dataHasValidType(data) {
		return nil, fmt.Errorf(
			"invalid data type. Valid type: array, array ptr, slice, slice ptr")
	}

	t := reflect.TypeOf(data)
	tk := t.Kind()

	if tk == reflect.Ptr {
		t = t.Elem().Elem()
	} else {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("the data type is not a struct")
	}

	return t, nil
}

// isExportableField Check if `sf` is an exportable struct field.
func isExportableField(sf reflect.StructField) bool {
	return sf.PkgPath == ""
}

// parseValue transforms fieldv in a value Type (IntType, FloatType...),
// it stores the value transformed  in a Value struct and returns the Value struct.
// The `col` param has info to transforms the fieldv in a Value struct.
func parseValue(fieldv reflect.Value, col column) (*Value, error) {
	var (
		value *Value
		err error
	)

	// fieldv should be a basic type (int, uint, float...)
	if col.basicType {
		switch col.ctype {
		case INT:
			value, err = newValue(simpleIntType{fieldv.Int()})
		case UINT:
			value, err = newValue(simpleUintType{fieldv.Uint()})
		case FLOAT:
			value, err = newValue(simpleFloatType{fieldv.Float()})
		case COMPLEX:
			value, err  = newValue(simpleComplexType{fieldv.Complex()})
		case STRING:
			value, err = newValue(simpleStringType{fieldv.String()})
		default:
			//col hasn't a valid columnType
			panic("invalid column type")
		}
	} else {
		// fieldv is a struct that must implements a ValueType
		value, err = newValue(fieldv.Interface())
	}

	if err != nil {
		return nil, fmt.Errorf("Parsing value: %s", err.Error())
	}

	return value, nil
}

/**
	@TODO comentar función
*/
func NewDataFrameFromStruct(data interface{}) (*dataFrameStruct, error) {
	dt, err := getStructOfData(data)

	if err != nil {
		return nil, err
	}

	var exists bool
	df := dataFrameStruct{}
	df.columns = []column{}
	df.cIndexByName = map[string]int{}

	// Generate the columns using the struct field tags.
	for i := 0; i < dt.NumField(); i++ {
		c := column{}
		field := dt.Field(i)
		fieldtag := field.Tag
		c.index = i

		if c.name, exists = fieldtag.Lookup("colName"); !exists {
			// Whether field hasn't colName tag, then it won't add to the dataframe.
			continue
		}

		if exists && !isExportableField(field) {
			return nil, fmt.Errorf("the column %s is unexportable", c.name)
		}


		if _, exists = df.cIndexByName[c.name]; exists {
			return nil, fmt.Errorf("the column %s is duplicated", c.name)
		}

		c.ctype, c.basicType, err = getColumnTypeFromType(field.Type)

		if err != nil {
			return nil, fmt.Errorf("in column %s: %s", c.name, err.Error())
		}

		df.columns = append(df.columns, c)
		df.cIndexByName[c.name] = len(df.columns) - 1
	}

	// parse values.
	dv := reflect.ValueOf(data)
	if dv.Type().Kind() == reflect.Ptr {
		dv = dv.Elem()
	}

	for i := 0; i < dv.Len(); i++ {
		rowSt := dv.Index(i)
		valuesRow := map[string]Value{}

		for _, col := range df.columns {
			value, _ := parseValue(rowSt.Field(col.index), col)
			valuesRow[col.name] = *value
		}

		df.data = append(df.data, valuesRow)
	}

	return &df, nil
}

// getColumnByName function returns the DataFrame column that his name
// match with the `coln` param. The second param returned is a flag
// indicating if the column `coln` exists in DataFrame.
func (df *dataFrameStruct)getColumnByName(coln string) (*column, bool) {
	pos, exists := df.cIndexByName[coln]

	if !exists {
		return nil, false
	}

	return &df.columns[pos], true
}

//ShowAllColumns func shows all dataframe columns.
func (df *dataFrameStruct)ShowAllColumns() {
	for i, _ := range  df.columns {
		df.columns[i].hidden = false
	}
}

// HideAllColumns func hides all dataframe columns
func (df *dataFrameStruct)HideAllColumns() {
	for i, _ := range  df.columns {
		df.columns[i].hidden = true
	}
}

// ShowColumns show the columns of the arguments.
// If one column of the argument doesn't exists, then returns an error.
func (df *dataFrameStruct)ShowColumns(columns ...string) error {
	df.HideAllColumns()

	for _, colname := range columns {
		col, exists := df.getColumnByName(colname)

		if ! exists {
			return fmt.Errorf("the column %s doesn't exists", colname)
		}

		col.hidden = false
	}

	return nil
}

// HideColumns hide columns of the arguments.
// If one column of the argument doesn't exists, then returns an error.
func (df *dataFrameStruct)HideColumns(columns ...string) error {
	df.ShowAllColumns()

	for _, colname := range columns {
		col, exists := df.getColumnByName(colname)

		if ! exists {
			return fmt.Errorf("the column %s doesn't exists", colname)
		}

		col.hidden = true
	}

	return nil
}


// Headers returns the columns header of dataframe.
// the columns hidden are ignored.
func (df *dataFrameStruct)Headers() []string {
	header := []string{}
	for _, col := range df.columns {
		if !col.hidden {
			header = append(header, col.name)
		}
	}

	return header
}
