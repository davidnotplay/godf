package dataframe

import (
	"fmt"
	"reflect"
	"sort"
)

// dataHasValidType checks if `data` is:
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

// getStructOfData gets the struct type of the `data` *array*.
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

// isExportableField checks if `sf` is an exportable struct field.
func isExportableField(sf reflect.StructField) bool {
	return sf.PkgPath == ""
}

// NewDataFrameFromStruct creates a new DataFrame using a struct array as data.
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
	df.order = []internalOrderColumn{}
	return &df, nil
}

// dataHandlerStruct struct handles the data stored in the struct array.
type dataHandlerStruct struct {
	// Ptr to the dataframe object.
	dataframe *DataFrame
	// Map array with all data in DataFrame.
	data []map[string]Value
	// Array with the function to order the DataFrame rows.
	orderFuncs []func(a Value, b Value) (Comparers, error)
}

// parseValue transforms fieldv in a *ValueTypes*, it stores the value transformed in a Value
// struct and returns the Value struct. The `col` param has info to transforms the fieldv.
func parseValue(fieldv reflect.Value, col column) (*Value, error) {
	var (
		value *Value
		err   error
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

// newDataHandlerStruct makes a new dataHandlerStruct using the arguments as struct field.
func newDataHandlerStruct(df *DataFrame, data interface{}) (*dataHandlerStruct, error) {
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

	return &dh, nil
}

// Get retrieves a concrete value from the DataFrame.
// If the row or the column is invalid then it returns an error.
func (dh *dataHandlerStruct) Get(row int, column string) (Value, error) {
	if len(dh.data) <= row {
		return Value{}, fmt.Errorf("row %d out of range", row)
	}

	if _, exists := dh.dataframe.cIndexByName[column]; !exists {
		return Value{}, fmt.Errorf("column %s not found", column)

	}

	return dh.data[row][column], nil
}

// prepareOrderFuncs makes the array orderFuncs in dataHandlerStruct.
// The comparer functions made depends of the DataFrame order defined in `dh.dataframe.order`
func (dh *dataHandlerStruct) prepareOrderFuncs() {
	oColumns := dh.dataframe.order
	dh.orderFuncs = []func(a, b Value) (Comparers, error){}

	for _, oc := range oColumns {
		var f func(a, b Value) (Comparers, error)

		switch oc.column.ctype {
		case INT:
			f = func(a, b Value) (Comparers, error) {
				i, _ := a.IntType()
				v, _ := b.Int64()
				return i.Compare(v), nil
			}

		case UINT:
			f = func(a, b Value) (Comparers, error) {
				i, _ := a.UintType()
				v, _ := b.Uint64()
				return i.Compare(v), nil
			}

		case FLOAT:
			f = func(a, b Value) (Comparers, error) {
				i, _ := a.FloatType()
				v, _ := b.Float64()
				return i.Compare(v), nil
			}

		case COMPLEX:
			f = func(a, b Value) (Comparers, error) {
				i, _ := a.ComplexType()
				v, _ := b.Complex128()
				return i.Compare(v), nil
			}
		case STRING:
			f = func(a, b Value) (Comparers, error) {
				i, _ := a.StringType()
				v, _ := b.Str()
				return i.Compare(v), nil
			}
		}

		dh.orderFuncs = append(dh.orderFuncs, f)
	}
}

// Len returns the number of rows in dataframe.
func (dh *dataHandlerStruct) Len() int {
	return len(dh.data)
}

// Swap swaps the i and j dataframe rows.
func (dh *dataHandlerStruct) Swap(i, j int) {
	dh.data[i], dh.data[j] = dh.data[j], dh.data[i]
}

// Less returns true if the row i is more less than j row.
// To compare both rows use the `orderFuncs` array.
func (dh *dataHandlerStruct) Less(i, j int) bool {
	for indx, f := range dh.orderFuncs {
		ocol := dh.dataframe.order[indx]
		valuei, _ := dh.Get(i, ocol.column.name)
		valuej, _ := dh.Get(j, ocol.column.name)
		comp, _ := f(valuei, valuej)

		switch comp {
		case EQUAL:
			continue
		case LESS:
			if ocol.order == ASC {
				return true
			}

			return false
		case GREAT:
			if ocol.order == ASC {
				return false
			}

			return true
		}
	}

	return false
}

// Order func Orders the dataframe rows using the order stored in `dh.dataframe.order`
func (dh *dataHandlerStruct) Order() error {
	if len(dh.dataframe.order) == 0 {
		return nil // there isn't order defined.
	}

	dh.prepareOrderFuncs()
	sort.Sort(dh)
	return nil
}
