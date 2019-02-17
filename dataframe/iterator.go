package dataframe

// Iterator allows walk all DataFrame rows easily.
type Iterator struct {
	// DataFrame instance ptr.
	df *DataFrame
	// internal index indicating the row position in DataFrame.
	pos int
	// min position where it will start the iterator.
	min int
	// max position where it will finish the iterator.
	max int
}

// newIterator create a new iterator.
// It returns an error if the min or max values are invalid.
func newIterator(df *DataFrame, min, max int) (*Iterator, error) {
	if err := df.checkRange(min, max); err != nil {
		return nil, err
	}
	return &Iterator{df, min, min, max}, nil
}

// Next returns the current row of the iterator and advance one position.
// Whether the iterator can not advance more (is in the last row), then it returns false
// as second argument.
func (it *Iterator) Next()(Row, bool) {
	row := it.Current()
	if row.df == nil {
		return row, false
	}
	it.pos++
	return row, true
}

// Current returns the current row.
func (it *Iterator) Current() Row {
	if it.pos >= it.max {
		return Row{nil, 0}
	}

	row, _ := newRow(it.df, it.pos)
	return row
}

// Reset func resets the iterator.
func (it *Iterator) Reset() {
	it.pos = it.min
}

//Position returns the current iterator position.
func (it *Iterator) Position() int {
	return it.pos
}

