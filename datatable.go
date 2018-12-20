package datatable

import (
	"reflect"
	"strings"
)

//Column - a column in the data table
type Column struct {
	Name   string
	Type   reflect.Type
	Length int
}

//Row - a row in the data table
type Row struct {
	Cells       []Cell
	ColumnCount int
}

//Cell - a location for the value
type Cell struct {
	ColumnIndex int
	RowIndex    int
	Value       interface{}
}

//DataTable - the object
type DataTable struct {
	Name        string
	Columns     []Column
	Rows        []Row
	RowCount    int
	ColumnCount int
}

//NewDataTable - create a new datatable
func NewDataTable(Name string) *DataTable {
	//columns = []Column{}
	//dt.Rows = []Row{}

	dt := &DataTable{}
	dt.Name = Name
	dt.ColumnCount = 0
	dt.RowCount = 0

	return dt
}

//AddColumn - add a new column to the data table
func (dt *DataTable) AddColumn(name string, vartype reflect.Type, length int) {
	exists := false
	for _, col := range dt.Columns {
		if strings.ToLower(col.Name) == strings.ToLower(name) {
			exists = true
			break
		}
	}

	if !exists {
		col := Column{Name: name, Type: vartype, Length: length}
		dt.Columns = append(dt.Columns, col)
		dt.resizeCells()
	}
}

//AddColumns - add a new columns to the data table
func (dt *DataTable) AddColumns(newcolumns []Column) {
	/* loop and check for duplicate column */
	var exists bool
	for f, ncol := range newcolumns {
		exists = false
		for _, col := range dt.Columns {
			if strings.ToLower(col.Name) == strings.ToLower(ncol.Name) {
				exists = true
				break
			}
		}

		if !exists {
			dt.Columns = append(dt.Columns, newcolumns[f])
			dt.resizeCells()
		}
	}
}

// AddRow - add a row to the current rows
func (dt *DataTable) AddRow(row Row) {
	idx := len(dt.Rows) + 1

	var r Row
	r.ColumnCount = len(row.Cells)
	r.Cells = make([]Cell, r.ColumnCount)
	copy(r.Cells, row.Cells)

	/* Adjust row index */
	for _, c := range r.Cells {
		c.RowIndex = idx
	}

	dt.Rows = append(dt.Rows, row)
	dt.RowCount = len(dt.Rows)
}

// AddRows - adds a range of rows to the current data table
func (dt *DataTable) AddRows(rows []Row) {
	for _, r := range rows {
		dt.AddRow(r)
	}
}

// NewRow - returns a new row based on column structure
func (dt *DataTable) NewRow() Row {
	tmp := make([]Cell, len(dt.Columns)) /* Create cells */

	return Row{Cells: tmp, ColumnCount: len(tmp)}
}

func (dt *DataTable) resizeCells() {
	for i, r := range dt.Rows {
		r.Cells = append(r.Cells, Cell{
			ColumnIndex: len(dt.Columns) - 1,
			RowIndex:    i,
			Value:       nil})
	}
}
