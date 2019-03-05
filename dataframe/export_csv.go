package dataframe

import (
	// "bufio"
	"encoding/csv"
	"fmt"
	"os"
)

// CsvRowRange struct is used to define the range of rows in DataFrame that will be
// exported in csv. It is similar to range in go slices.
type CsvRowRange struct {
	Min int // Min Dataframe row position.
	Max int // Max Dataframe row position.
}

// CsvConfig struct is used to define the options to export the DataFrame in a csv file.
type CsvConfig struct {
	// contains the csv column separator.
	Comma rune
	// True to use \r\n as line terminator.
	UseCRLF bool
	// DataFrame column names will be exported.
	Columns []string
	// Range of DataFrame rows will be exported.
	Range CsvRowRange
}

// ErrorCsvFile is a struct to define the errors exporting the DataFrame in a csv file.
type ErrorCsvFile struct {
	filename string // csv filename
	err      error  // Error raised exporting the file.
}

// String returns the error as a string.
func (e *ErrorCsvFile) String() string {
	str := fmt.Sprintf("error exporting the %s file: %s", e.filename, e.err.Error())
	return str
}

func (e *ErrorCsvFile) Error() string {
	return e.String()
}

// ExportCsvFile exports the DataFrame rows in the f file as a csv file, using the conf config.
func (df *DataFrame) ExportCsvFile(f *os.File, conf *CsvConfig) error {
	if len(conf.Columns) == 0 {
		err := fmt.Errorf("in csv config, the Columns string array is empty")
		return &ErrorCsvFile{f.Name(), err}
	}

	for _, colName := range conf.Columns {
		if _, ok := df.cIndexByName[colName]; !ok {
			err := fmt.Errorf("in csv config, column %s not found", colName)
			return &ErrorCsvFile{f.Name(), err}
		}
	}

	// make the iterator
	iterator, err := df.IteratorRange(conf.Range.Min, conf.Range.Max)
	if err != nil {
		return &ErrorCsvFile{f.Name(), err}
	}

	// make the csv writer
	writer := csv.NewWriter(f)
	writer.Comma = conf.Comma
	writer.UseCRLF = conf.UseCRLF

	// insert the csv header
	if err := writer.Write(conf.Columns); err != nil {
		return &ErrorCsvFile{f.Name(), err}
	}

	// iterate the DataFrame
	for row, cont := iterator.Next(); cont; row, cont = iterator.Next() {
		csvRow := []string{}

		for _, colName := range conf.Columns {
			value, _ := row.Cell(colName)
			csvRow = append(csvRow, value.String())
		}

		if err := writer.Write(csvRow); err != nil {
			return &ErrorCsvFile{f.Name(), err}
		}

	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return &ErrorCsvFile{f.Name(), err}
	}

	return nil
}

// ExportCsvFileDefault exports the DataFrame rows in the f file as a csv file,
// using the default config.
//	- Comma:   ; character,
//	- UseCRLF: false
//	- Columns: All columns
//	- Range:   All rows
func (df *DataFrame) ExportCsvFileDefault(f *os.File) error {
	config := CsvConfig{
		Comma:   ';',
		UseCRLF: false,
		Columns: df.Headers(),
		Range:   CsvRowRange{0, df.NumberRows()},
	}

	return df.ExportCsvFile(f, &config)
}
