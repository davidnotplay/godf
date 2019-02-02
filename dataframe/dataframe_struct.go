package dataframe

import (
	"fmt"
	"reflect"
)

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

/**
	@TODO comentar función
*/
func NewDataFrameFromStruct(data interface{}) (*DataFrame, error) {
	dt, err := getStructOfData(data)

	if err != nil {
		return nil, err
	}

	var exists bool
	df := DataFrame{}
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

	df.handler, _ = newDataHandlerStruct(&df, data)
	return &df, nil
}

type dataHandlerStruct struct {
	dataframe *DataFrame
	data	  []map[string]Value
	order	  []int
}

// makeRange makes an slice of consecutive numbers from min param to max param;
// both numbers included.
func makeRange(min, max int) []int{
	r := make([]int, max - min + 1)

	for i := range r {
		r[i] = min + i
	}

	return r
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
			value, err = newValue(simpleComplexType{fieldv.Complex()})
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

// newDataHandlerStruct func makes a new dataHandlerStruct using the *in* arguments as
// struct field.
func newDataHandlerStruct(df *DataFrame, data interface{})(*dataHandlerStruct, error){
	dv := reflect.ValueOf(data)
	dh := dataHandlerStruct{}
	dh.dataframe = df

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

		dh.data = append(dh.data, valuesRow)
	}

	dh.order = makeRange(0, len(dh.data) - 1)
	return &dh, nil
}
