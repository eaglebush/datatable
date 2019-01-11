package datatable

import (
	"reflect"
	"strings"
	"time"
)

//Column - a column in the data table
type Column struct {
	Name   string
	Type   reflect.Type
	Length int64
}

//Row - a row in the data table
type Row struct {
	Cells       []Cell
	ColumnCount int
}

//Cell - a location for the value
type Cell struct {
	ColumnName  string
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
	dt := &DataTable{}
	dt.Name = Name
	dt.ColumnCount = 0
	dt.RowCount = 0

	return dt
}

//AddColumn - add a new column to the data table
func (dt *DataTable) AddColumn(name string, vartype reflect.Type, length int64) {
	exists := false
	name = strings.ToLower(name)
	for _, col := range dt.Columns {
		if strings.ToLower(col.Name) == name {
			exists = true
			break
		}
	}

	if !exists {
		col := Column{Name: name, Type: vartype, Length: length}
		dt.Columns = append(dt.Columns, col)
		dt.resizeCells()
		dt.ColumnCount = len(dt.Columns)
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
	dt.ColumnCount = len(dt.Columns)
}

// AddRow - add a row to the current rows
func (dt *DataTable) AddRow(row Row) {
	idx := len(dt.Rows)

	var r Row
	r.ColumnCount = row.ColumnCount
	r.Cells = append(r.Cells, row.Cells...)

	/* Adjust row index */
	for i := range row.Cells {
		r.Cells[i].RowIndex = idx
		r.Cells[i].ColumnIndex = i
	}

	dt.Rows = append(dt.Rows, r)
	dt.RowCount = len(dt.Rows)
}

// AddRows - adds a range of rows to the current data table
func (dt *DataTable) AddRows(rows []Row) {
	lastcnt := dt.RowCount
	cnt := len(rows)
	combcnt := lastcnt + cnt

	dt.Rows = append(dt.Rows, rows...)
	for f := lastcnt; f < combcnt; f++ {
		for g := 0; g < dt.ColumnCount; g++ {
			dt.Rows[f].Cells[g].RowIndex = f
			dt.Rows[f].Cells[g].ColumnIndex = g
		}
	}
	dt.RowCount = combcnt
	rows = nil
}

// NewRow - returns a new row based on column structure
func (dt *DataTable) NewRow() Row {
	r := Row{Cells: make([]Cell, len(dt.Columns)), ColumnCount: len(dt.Columns)}
	for i, cl := range dt.Columns {
		r.Cells[i].ColumnIndex = i
		r.Cells[i].ColumnName = cl.Name
	}
	return r
}

// resizeCells for AddColumn and AddColumns
func (dt *DataTable) resizeCells() {
	for i, r := range dt.Rows {
		r.Cells = append(r.Cells, Cell{
			ColumnIndex: len(dt.Columns) - 1,
			RowIndex:    i,
			Value:       nil})
	}
}

//Value - get row cell values
func (rw *Row) Value(index interface{}) interface{} {
	tname := reflect.TypeOf(index).Name()

	idx := -1

	if tname == "int" {
		idx = index.(int)
	}

	if tname == "string" {
		kname := strings.ToLower(index.(string))
		for i, c := range rw.Cells {
			if strings.ToLower(c.ColumnName) == kname {
				idx = i
				break
			}
		}
	}

	if idx != -1 {
		c := rw.Cells[idx]
		if c.Value != nil {
			v := strings.ToLower(reflect.TypeOf(c.Value).String())
			switch v {
			case "[]uint8":
				return string(c.Value.([]uint8))
			}
		}
		return c.Value
	}

	return nil
}

//ValueString - return the value as string or a default empty string if the value is null
func (rw *Row) ValueString(index interface{}) string {
	ret := rw.Value(index)

	if ret == nil {
		return ""
	}

	return ret.(string)
}

//ValueTime - return the value as time.Time or a default empty time.Time if the value is null
func (rw *Row) ValueTime(index interface{}) time.Time {
	ret := rw.Value(index)

	if ret == nil {
		return time.Time{}
	}

	return ret.(time.Time)
}

//ValueBool - return the value as boolean or a false if the value is null
func (rw *Row) ValueBool(index interface{}) bool {
	ret := rw.Value(index)

	if ret == nil {
		return false
	}

	return ret.(bool)
}

//ValueFloat64 - return the value as float64 or a 0 if the value is null
func (rw *Row) ValueFloat64(index interface{}) float64 {
	ret := rw.Value(index)

	if ret == nil {
		return 0
	}

	return ret.(float64)
}

//ValueFloat32 - return the value as float32 or a 0 if the value is null
func (rw *Row) ValueFloat32(index interface{}) float32 {
	ret := rw.Value(index)

	if ret == nil {
		return 0
	}

	return ret.(float32)
}

//ValueInt - return the value as int or a 0 if the value is null
func (rw *Row) ValueInt(index interface{}) int {
	ret := rw.Value(index)

	if ret == nil {
		return 0
	}

	return ret.(int)
}

//ValueInt16 - return the value as int16 or a 0 if the value is null
func (rw *Row) ValueInt16(index interface{}) int16 {
	ret := rw.Value(index)

	if ret == nil {
		return 0
	}

	return ret.(int16)
}

//ValueInt32 - return the value as int32 or a 0 if the value is null
func (rw *Row) ValueInt32(index interface{}) int32 {
	ret := rw.Value(index)

	if ret == nil {
		return 0
	}

	return ret.(int32)
}

//ValueInt64 - return the value as int64 or a 0 if the value is null
func (rw *Row) ValueInt64(index interface{}) int64 {
	ret := rw.Value(index)

	if ret == nil {
		return 0
	}

	return ret.(int64)
}

//ValueByte - return the value as byte or a 0 if the value is null
func (rw *Row) ValueByte(index interface{}) byte {
	ret := rw.Value(index)

	if ret == nil {
		return 0
	}

	return ret.(byte)
}
