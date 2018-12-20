package datatable

type Column struct {
	Name   string
	Type   string
	Length int
}

type Row struct {
	Columns     []Column
	ColumnCount int
}

type DataTable struct {
	Name        string
	Rows        []Row
	RowCount    int
	ColumnCount int
}
