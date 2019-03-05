package dataframe

import (
	"encoding/csv"
	"errors"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type mockDataCsv struct {
	S  string     `colName:"string"`
	I  int        `colName:"integer"`
	F  float32    `colName:"float"`
	C  complex128 `colName:"complex"`
	XS string     `colName:"X string"`
}

func makeDataFrameMockDataCsv(t *testing.T) (df *DataFrame, md []mockDataCsv) {
	var err error
	md = []mockDataCsv{
		{"test 1 \"quote\"", 1, 1.1111, 1 - 1i, ""},
		{"test 2 \"quote\"", 2, 2.2222, 2 - 2i, "~"},
		{"test 3 \"quote\"", 3, 3.3333, 3 - 3i, "说/説"},
		{"test 4 \"quote\"", 4, 4.4444, 4 - 4i, "/#2i\"\""},
		{"test 5 \"quote\"", 5, 5.5555, 5 - 5i, "ÐÝÞðýþ"},
		{"test 6 \"quote\"", 6, 6.6666, 6 - 6i, "   "},
		{"test 7 \"quote\"", 7, 7.7777, 7 - 7i, "3223"},
		{"test 8 \"quote\"", 8, 8.8888, 8 - 8i, "~½¬¬¬"},
		{"test 9 \"quote\"", 9, 9.9999, 9 - 9i, "Ñandú"},
	}

	df, err = NewDataFrameFromStruct(md)

	if err != nil {
		as := assert.New(t)
		as.FailNow("error creating DataFrame", "error: %s", err.Error())
	}

	return
}

func getCsvTestPath(filename string) string {
	return filepath.Join("..", "csv_test", filename)
}

func createCsvFile(t *testing.T, filename string, rdonly bool) *os.File {
	var flag int

	if !rdonly {
		flag = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	} else {
		flag = os.O_RDONLY | os.O_CREATE
	}

	filename = getCsvTestPath(filename)
	f, err := os.OpenFile(filename, flag, 0644)

	if err != nil {
		assert.FailNowf(
			t, "error opening the file",
			"file: %s. error: %s", filename, err.Error())
		return nil
	}

	return f
}

func checkCsv(filename string, df *DataFrame, config *CsvConfig, t *testing.T) bool {
	as := assert.New(t)
	f, err := os.OpenFile(filename, os.O_RDONLY, 0644)

	if err != nil {
		as.FailNowf("error opening the file", "error: %s", err.Error())
		return false
	}

	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = config.Comma

	headers, err := reader.Read()

	if err != nil {
		as.FailNowf("error fetching the header", "error: %s", err.Error())
		return false
	}

	// check csv header
	for _, colName := range config.Columns {
		as.Containsf(headers, colName, "the column name %s does not exist", colName)
	}

	var csvRows [][]string

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			as.FailNowf("error fetching the row", "error: %s", err.Error())
			return false
		}

		csvRows = append(csvRows, row)
	}

	// first check the array size
	as.Equal(config.Range.Max-config.Range.Min, len(csvRows), "Different sizes")

	for rowIndx, csvRow := range csvRows[config.Range.Min:config.Range.Max] {
		for indxColumn, colName := range config.Columns {
			expected, _ := df.handler.Get(rowIndx, colName)
			actual := csvRow[indxColumn]
			as.Equal(expected.String(), actual)
		}
	}

	return true
}

func closeAndRemoveFile(t *testing.T, f *os.File) {
	filename := f.Name()
	f.Close()
	err := os.Remove(filename)
	if err != nil {
		assert.FailNowf(
			t, "error removing file",
			"file %s. error: %s", f.Name(), err.Error())
	}
}

func Test_DataFrame_exportCsvFile_func(t *testing.T) {
	var (
		df  *DataFrame
		f   *os.File
		err error
	)
	as := assert.New(t)

	if f = createCsvFile(t, "test1.csv", false); f == nil {
		return
	}

	defer closeAndRemoveFile(t, f)

	// create DataFarme
	if df, _ = makeDataFrameMockDataCsv(t); df == nil {
		return
	}

	// check create a csv file
	config := CsvConfig{
		Comma:   ';',
		Range:   CsvRowRange{0, df.NumberRows()},
		Columns: df.Headers(),
	}

	if err = df.ExportCsvFile(f, &config); err != nil {
		as.FailNowf(
			"error exporting csv file",
			"file: %s. error: %s", f.Name(), err.Error())
		return
	}

	checkCsv(f.Name(), df, &config, t)

	// check custom columns
	f.Truncate(0)
	f.Seek(0, 0)
	config.Columns = []string{"string", "integer", "X string"}
	if err = df.ExportCsvFile(f, &config); err != nil {
		as.FailNowf(
			"error exporting csv file",
			"file: %s. error: %s", f.Name(), err.Error())
		return
	}

	checkCsv(f.Name(), df, &config, t)

	// check custom min and max
	f.Truncate(0)
	f.Seek(0, 0)
	config.Columns = df.Headers()
	config.Range = CsvRowRange{2, 6}
	if err = df.ExportCsvFile(f, &config); err != nil {
		as.FailNowf(
			"error exporting csv file",
			"file: %s. error: %s", f.Name(), err.Error())
		return
	}

	// Custom Comma
	f.Truncate(0)
	f.Seek(0, 0)
	config.Range = CsvRowRange{0, df.NumberRows()}
	config.Comma = '|'
	if err = df.ExportCsvFile(f, &config); err != nil {
		as.FailNowf(
			"error exporting csv file",
			"file: %s. error: %s", f.Name(), err.Error())
		return
	}

	// UseCRLF
	f.Truncate(0)
	f.Seek(0, 0)
	config.UseCRLF = true
	if err = df.ExportCsvFile(f, &config); err != nil {
		as.FailNowf(
			"error exporting csv file",
			"file: %s. error: %s", f.Name(), err.Error())
		return
	}
}

func Test_DataFrame_exportCsvFile_func_error(t *testing.T) {
	var (
		df  *DataFrame
		f   *os.File
		err error
	)
	as := assert.New(t)
	gerr := func(errstr string) string {
		return (&ErrorCsvFile{f.Name(), errors.New(errstr)}).String()
	}

	if f = createCsvFile(t, "test1.csv", false); f == nil {
		return
	}

	defer closeAndRemoveFile(t, f)

	// create DataFarme
	if df, _ = makeDataFrameMockDataCsv(t); df == nil {
		return
	}

	// Columns empty error
	config := CsvConfig{
		Comma:   ';',
		Range:   CsvRowRange{0, df.NumberRows()},
		Columns: []string{},
	}

	err = df.ExportCsvFile(f, &config)
	as.Equal(
		err.Error(),
		gerr("in csv config, the Columns string array is empty"),
		"the error mesagge does not match")

	// Add an invalid column
	config = CsvConfig{
		Comma:   ';',
		Range:   CsvRowRange{0, df.NumberRows()},
		Columns: []string{"invalid"},
	}
	err = df.ExportCsvFile(f, &config)
	as.Equal(
		err.Error(),
		gerr("in csv config, column invalid not found"),
		"the error mesagge does not match")

	// iterator range
	config = CsvConfig{
		Comma:   ';',
		Range:   CsvRowRange{-1, 2},
		Columns: []string{"string"},
	}
	err = df.ExportCsvFile(f, &config)
	as.Equal(
		err.Error(),
		gerr("index must be non-negative number"),
		"the error mesagge does not match")
}

func Test_DataFrame_exportCsvFileDefault_func(t *testing.T) {
	var (
		df  *DataFrame
		f   *os.File
		err error
	)
	as := assert.New(t)

	if f = createCsvFile(t, "test1_default.csv", false); f == nil {
		return
	}

	defer closeAndRemoveFile(t, f)

	// create DataFarme
	if df, _ = makeDataFrameMockDataCsv(t); df == nil {
		return
	}

	// Columns empty error
	config := CsvConfig{
		Comma:   ';',
		Range:   CsvRowRange{0, df.NumberRows()},
		Columns: df.Headers(),
	}

	if err = df.ExportCsvFileDefault(f); err != nil {
		as.FailNowf(
			"error exporting csv file",
			"file: %s. error: %s", f.Name(), err.Error())
		return
	}

	checkCsv(f.Name(), df, &config, t)
}
