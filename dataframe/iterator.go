package dataframe

// Iterator allows walk all DataFrame rows easily.
type Iterator struct {
	// DataFrame instance ptr.
	df *DataFrame
	// internal ptr indicating the row position in DataFrame.
	pos int
}

// newIterator creates a new Iterator.
func newIterator(df *DataFrame) Iterator {
	return Iterator{df, 0}
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
	if it.pos >= it.df.handler.Len() {
		return Row{nil, 0}
	}

	row, _ := newRow(it.df, it.pos)
	return row
}

// Reset func resets the iterator.
func (it *Iterator) Reset() {
	it.pos = 0
}

//Position returns the current iterator position.
func (it *Iterator) Position() int {
	return it.pos
}

