package dataframe


type dataFrameBase struct {
}

type DataFrame interface {
	Headers() []string
	ShowAllColumns()
	ShowColumns(columns ...string) error
	HideAllColumns()
	HideColumns(columns ...string) error
}

