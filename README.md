
Go DF
=====

GoDF is an implementation, in go,  of a DataFrame structure.
DataFrame is a structure to represents bidimensional data, as a table.

Usage
=====

Data from a struct
------------------
You can define the DataFrame data from the Go struct.

```go
// DataFrame struct
type Df struct {
	A int      `colName:"column1"`
	B float32  `colName:"column2"`
	// private field. It will not export.
	c float32    
	// public field, but it has not the tag colName. It will not export.
	C float32    
	D string  `colName:"column3"`
}

// Data for the DataFrame.
data := []Df{
	{1, 1.1, 1.1, "test 1"},
	{2, 2.2, 2.2, "test 2"},
}

// Make the dataframe.
df, err := NewDataFrameFromStruct(data)

```

Only it can export, from the Go struct, fields with valid types:
- int
- int64
- int32
- int16
- int8
- uint
- uint64
- uint32
- uint16
- uint8
- float64
- float32
- complex64
- complex128
- string

Define you custom type
----------------------

Also it can create custom types, implementing the interface **ValueTypes**
- IntType
- UintType
- FloatType
- ComplexType
- StringType


```go
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
```

*wip...*
