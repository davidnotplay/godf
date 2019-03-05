package dataframe

import (
	"fmt"
	"testing"
)

type benchmarDFStruct struct {
	I int	     `colName:"integer"`
	F float64    `colName:"float"`
	C complex128 `colName:"complex"`
	S string     `colName:"str"`
}

func genData(number int) *[]benchmarDFStruct{
	var data []benchmarDFStruct

	for i:= 0; i < number; i++ {
		f := float64(i)
		row := benchmarDFStruct{
			i, f + (0.1*f), complex(f, f), fmt.Sprintf("test%d", i),
		}

		data = append(data, row)
	}

	return &data
}


func benchmarkNewDataFrame(rows int, b *testing.B) {
	data := genData(rows)

	for n := 0; n < b.N; n++ {
		NewDataFrameFromStruct(data)
	}
}

func BenchmarkNewDataFrame100(b *testing.B) {
	benchmarkNewDataFrame(100, b)
}

func BenchmarkNewDataFrame1000(b *testing.B) {
	benchmarkNewDataFrame(1000, b)
}

func BenchmarkNewDataFrame10000(b *testing.B) {
	benchmarkNewDataFrame(10000, b)
}

func BenchmarkNewDataFrame100000(b *testing.B) {
	benchmarkNewDataFrame(100000, b)
}
