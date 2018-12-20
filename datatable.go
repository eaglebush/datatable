package datatable

//Column - a column in the data table
type Column struct {
	Name   string
	Type   string
	Length int
}

//Row - a row in the data table
type Row struct {
	Columns     []Column
	ColumnCount int
}

//DataTable - the object
type DataTable struct {
	Name        string
	Rows        []Row
	RowCount    int
	ColumnCount int
}

var columns []Column
var rows []Row

func (dt *DataTable) NewDataTable(Name string) DataTable {
	columns = []Column{}
	rows = []Row{}

	dt = &DataTable{}
	dt.Name = Name
	dt.ColumnCount = 0
	dt.RowCount = 0
	dt.Rows = rows

	return *dt
}

func (dt *DataTable) AddColumn(Name string, Type type, Length int) {

}

func (dt *.DataTable) AddColumnRange (Columns []Column) {

}
