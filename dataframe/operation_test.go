package dataframe

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_DataFrame_OperationRange_Func(t *testing.T) {
	var df *DataFrame
	as := assert.New(t)

	if df = getDataFrameToColumns(t); df == nil {
		return
	}

	op := OperatrionSumInt{OperationBaseInt{OperationBase{"col A"}, 0}}
	err := df.OperationRange(&op, 0, 2)

	if err != nil {
		as.FailNowf("error in the operation", "error: %s", err.Error)
	}

	as.Equal(op.Total, int64(-3), "the value isn't match")


	// operation errors.
	err = df.OperationRange(&op, -1, 2)
	as.Equal(err.Error(), "index must be non-negative number")
}

func Test_DataFrame_Operation_Func(t *testing.T) {
	var df *DataFrame
	as := assert.New(t)

	if df = getDataFrameToColumns(t); df == nil {
		return
	}

	op := OperatrionSumInt{OperationBaseInt{OperationBase{"col A"}, 0}}
	err := df.Operation(&op)

	if err != nil {
		as.FailNowf("error in the operation", "error: %s", err.Error)
	}

	as.Equal(op.Total, int64(-45), "the value isn't match")
}

func Test_DataFrame_SumRange_func(t *testing.T) {
	var df *DataFrame
	as := assert.New(t)

	if df = getDataFrameToColumns(t); df == nil {
		return
	}

	// sum int
	value, err := df.SumRange("col A", 1, 3)
	if err != nil {
		println(err)
		as.FailNowf("error in operation. ", "error: %s", err.Error())
		return
	}
	as.Equal(value.(int64), int64(-5), "the value is different")

	// sum uint
	value, err = df.SumRange("col B", 1, 3)
	if err != nil {
		println(err)
		as.FailNowf("error in operation. ", "error: %s", err.Error())
		return
	}
	as.Equal(value.(uint64), uint64(5), "the value is different")

	// sum float
	value, err = df.SumRange("col C", 1, 3)
	if err != nil {
		println(err)
		as.FailNowf("error in operation. ", "error: %s", err.Error())
		return
	}
	as.Equal(float32(value.(float64)), float32(5.555), "the value is different")

	// sum float
	value, err = df.SumRange("col D", 1, 3)
	if err != nil {
		println(err)
		as.FailNowf("error in operation. ", "error: %s", err.Error())
		return
	}
	as.Equal(value.(complex128), complex128(5.5 -5.5i), "the value is different")

	// error column not found
	value, err = df.SumRange("not-exists", 1, 3)
	as.Nil(value, "when there is an error the value must be nil")
	as.Equal(err.Error(), "column not-exists not found", "invalid error message")


	// invalid column
	value, err = df.SumRange("col E", 1, 3)
	as.Nil(value, "when there is an error the value must be nil")
	as.Equal(
		err.Error(),
		"Sum operation is invalid in column type string",
		"invalid error message")
}

func Test_DataFrame_Sum_func(t *testing.T) {
	var df *DataFrame
	as := assert.New(t)

	if df = getDataFrameToColumns(t); df == nil {
		return
	}

	// sum int
	value, err := df.Sum("col A")
	if err != nil {
		println(err)
		as.FailNowf("error in operation. ", "error: %s", err.Error())
		return
	}
	as.Equal(value.(int64), int64(-45), "the value is different")

	// sum uint
	value, err = df.Sum("col B")
	if err != nil {
		println(err)
		as.FailNowf("error in operation. ", "error: %s", err.Error())
		return
	}
	as.Equal(value.(uint64), uint64(45), "the value is different")

	// sum float
	value, err = df.Sum("col C")
	if err != nil {
		println(err)
		as.FailNowf("error in operation. ", "error: %s", err.Error())
		return
	}
	as.Equal(float32(value.(float64)), float32(49.995), "the value is different")

	// sum float
	value, err = df.Sum("col D")
	if err != nil {
		println(err)
		as.FailNowf("error in operation. ", "error: %s", err.Error())
		return
	}
	as.Equal(value.(complex128), complex128(49.5 -49.5i), "the value is different")

	// error column not found
	value, err = df.Sum("not-exists")
	as.Nil(value, "when there is an error the value must be nil")
	as.Equal(err.Error(), "column not-exists not found", "invalid error message")


	// invalid column
	value, err = df.Sum("col E")
	as.Nil(value, "when there is an error the value must be nil")
	as.Equal(
		err.Error(),
		"Sum operation is invalid in column type string",
		"invalid error message")
}

func Test_DataFrame_MaxRange_func(t *testing.T) {
	var (
		df *DataFrame
		err error
		value interface{}
	)
	as := assert.New(t)

	if df = getDataFrameToColumns(t); df == nil {
		return
	}

	expectedR := map[string]interface{}{
		"col A": int64(-2),
		"col B": uint64(3),
		"col C": float32(3.333),
		"col D": 3.3 - 3.3i,
	}

	for colName, expected := range expectedR {
		value, err = df.MaxRange(colName, 1, 3)

		if err != nil {
			as.FailNowf("there an error", "error %s", err.Error)
		}

		switch v := value.(type) {
		case int64, uint64, complex128:
			as.Equalf(v, expected, "the value of colname %s isn't valid", colName)
		case float64:
			// float is special case. bad rounded.
			as.Equalf(
				float32(v),
				expected,
				"the value of colname %s isn't valid",
				colName,
			)
		default:
			as.FailNow("type not found")
		}
	}

	// error iterator
	expectedR = map[string]interface{}{
		"col A": int64(0),
		"col B": uint64(0),
		"col C": float64(0),
		"col D": 0 + 0i,
	}

	for colName, expected := range expectedR {
		value, err = df.MaxRange(colName, -1, 3)
		as.Equal(value, expected, "must be 0 because there is an error")
		as.Equal(
			err.Error(),
			"index must be non-negative number",
			"the error is invalid",
		)
	}

	// error invalid column
	value, err = df.MaxRange("invalid", 1, 3)
	as.Nil(value, "there is an error, value must be nil")
	as.Equal(
		err.Error(),
		"column invalid not found",
		"the error message does not match",
	)

}

func Test_DataFrame_Max_func(t *testing.T) {
	var (
		df *DataFrame
		err error
		value interface{}
	)
	as := assert.New(t)

	if df = getDataFrameToColumns(t); df == nil {
		return
	}

	expectedR := map[string]interface{}{
		"col A": int64(-1),
		"col B": uint64(9),
		"col C": float32(9.999),
		"col D": 9.9 - 9.9i,
	}

	for colName, expected := range expectedR {
		value, err = df.Max(colName)

		if err != nil {
			as.FailNowf("there an error", "error %s", err.Error)
		}

		switch v := value.(type) {
		case int64, uint64, complex128:
			as.Equalf(v, expected, "the value of colname %s isn't valid", colName)
		case float64:
			// float is special case. bad rounded.
			as.Equalf(float32(v), expected, "the value of colname %s isn't valid", colName)
		default:
			as.FailNow("type not found")
		}
	}

	// error invalid column
	value, err = df.MaxRange("invalid", 1, 3)
	as.Nil(value, "there is an error, value must be nil")
	as.Equal(
		err.Error(),
		"column invalid not found",
		"the error message does not match",
	)

}

func Test_DataFrame_MinRange_func(t *testing.T) {
	var (
		df *DataFrame
		err error
		value interface{}
	)
	as := assert.New(t)

	if df = getDataFrameToColumns(t); df == nil {
		return
	}

	expectedR := map[string]interface{}{
		"col A": int64(-3),
		"col B": uint64(2),
		"col C": float32(2.222),
		"col D": 2.2 - 2.2i,
	}

	for colName, expected := range expectedR {
		value, err = df.MinRange(colName, 1, 3)

		if err != nil {
			as.FailNowf("there an error", "error %s", err.Error)
		}

		switch v := value.(type) {
		case int64, uint64, complex128:
			as.Equalf(v, expected, "the value of colname %s isn't valid", colName)
		case float64:
			// float is special case. bad rounded.
			as.Equalf(
				float32(v),
				expected,
				"the value of colname %s isn't valid",
				colName,
			)
		default:
			as.FailNow("type not found")
		}
	}

	// error iterator
	expectedR = map[string]interface{}{
		"col A": int64(0),
		"col B": uint64(0),
		"col C": float64(0),
		"col D": 0 + 0i,
	}

	for colName, expected := range expectedR {
		value, err = df.MinRange(colName, -1, 3)
		as.Equal(value, expected, "must be 0 because there is an error")
		as.Equal(
			err.Error(),
			"index must be non-negative number",
			"the error is invalid",
		)
	}

	// error invalid column
	value, err = df.MinRange("invalid", 1, 3)
	as.Nil(value, "there is an error, value must be nil")
	as.Equal(
		err.Error(),
		"column invalid not found",
		"the error message does not match",
	)
}

func Test_DataFrame_Min_func(t *testing.T) {
	var (
		df *DataFrame
		err error
		value interface{}
	)
	as := assert.New(t)

	if df = getDataFrameToColumns(t); df == nil {
		return
	}

	expectedR := map[string]interface{}{
		"col A": int64(-9),
		"col B": uint64(1),
		"col C": float32(1.111),
		"col D": 1.1 - 1.1i,
	}

	for colName, expected := range expectedR {
		value, err = df.Min(colName)

		if err != nil {
			as.FailNowf("there an error", "error %s", err.Error)
		}

		switch v := value.(type) {
		case int64, uint64, complex128:
			as.Equalf(v, expected, "the value of colname %s isn't valid", colName)
		case float64:
			// float is special case. bad rounded.
			as.Equalf(
				float32(v),
				expected,
				"the value of colname %s isn't valid",
				colName,
			)
		default:
			as.FailNow("type not found")
		}
	}

	// error invalid column
	value, err = df.Min("invalid")
	as.Nil(value, "there is an error, value must be nil")
	as.Equal(
		err.Error(),
		"column invalid not found",
		"the error message does not match",
	)
}
