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
