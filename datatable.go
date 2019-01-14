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

//SetValue - sets a variable with a value from the row specified by an index
func (rw *Row) SetValue(Variable interface{}, FieldIndex interface{}) {
	fv := rw.Value(FieldIndex)

	varbl := reflect.ValueOf(Variable).Elem() //Get the reflection value of the Variable to set value later

	switch varbl.Kind() {
	case reflect.Int:
		setIntValue(varbl, fv)
	case reflect.Int8:
		setInt8Value(varbl, fv)
	case reflect.Int16:
		setInt16Value(varbl, fv)
	case reflect.Int32:
		setInt32Value(varbl, fv)
	case reflect.Int64:
		setInt64Value(varbl, fv)
	case reflect.Uint:
		setUIntValue(varbl, fv)
	case reflect.Uint8:
		setUInt8Value(varbl, fv)
	case reflect.Uint16:
		setUInt16Value(varbl, fv)
	case reflect.Uint32:
		setUInt32Value(varbl, fv)
	case reflect.Uint64:
		setUInt64Value(varbl, fv)
	case reflect.Float32:
		setFloat32Value(varbl, fv)
	case reflect.Float64:
		setFloat64Value(varbl, fv)
	case reflect.Bool:
		b := fv.(bool)
		c := reflect.ValueOf(b)
		varbl.Set(c)
	case reflect.String:
		b := fv.(string)
		c := reflect.ValueOf(b)
		varbl.Set(c)
	}
	/*
		switch v.Interface().(type) {
		case int:
			setIntValue(v, fv)
		default:

		}
	*/

}

func setIntValue(varField reflect.Value, value interface{}) {
	fval := reflect.ValueOf(value)
	var b int

	switch fval.Kind() {
	case reflect.Int:
		b = value.(int)
	case reflect.Int8:
		b = int(value.(int8))
	case reflect.Int16:
		b = int(value.(int16))
	case reflect.Int32:
		b = int(value.(int32))
	case reflect.Int64:
		b = int(value.(int64))
	case reflect.Uint:
		b = int(value.(uint))
	case reflect.Uint8:
		b = int(value.(uint8))
	case reflect.Uint16:
		b = int(value.(uint16))
	case reflect.Uint32:
		b = int(value.(uint32))
	case reflect.Uint64:
		b = int(value.(uint64))
	case reflect.Float32:
		b = int(value.(float32))
	case reflect.Float64:
		b = int(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setInt8Value(varField reflect.Value, value interface{}) {
	fval := reflect.ValueOf(value)
	var b int8

	switch fval.Kind() {
	case reflect.Int8:
		b = value.(int8)
	case reflect.Int:
		b = int8(value.(int))
	case reflect.Int16:
		b = int8(value.(int16))
	case reflect.Int32:
		b = int8(value.(int32))
	case reflect.Int64:
		b = int8(value.(int64))
	case reflect.Uint:
		b = int8(value.(uint))
	case reflect.Uint8:
		b = int8(value.(uint8))
	case reflect.Uint16:
		b = int8(value.(uint16))
	case reflect.Uint32:
		b = int8(value.(uint32))
	case reflect.Uint64:
		b = int8(value.(uint64))
	case reflect.Float32:
		b = int8(value.(float32))
	case reflect.Float64:
		b = int8(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setInt16Value(varField reflect.Value, value interface{}) {
	fval := reflect.ValueOf(value)
	var b int16

	switch fval.Kind() {
	case reflect.Int16:
		b = value.(int16)
	case reflect.Int:
		b = int16(value.(int))
	case reflect.Int8:
		b = int16(value.(int8))
	case reflect.Int32:
		b = int16(value.(int32))
	case reflect.Int64:
		b = int16(value.(int64))
	case reflect.Uint:
		b = int16(value.(uint))
	case reflect.Uint8:
		b = int16(value.(uint8))
	case reflect.Uint16:
		b = int16(value.(uint16))
	case reflect.Uint32:
		b = int16(value.(uint32))
	case reflect.Uint64:
		b = int16(value.(uint64))
	case reflect.Float32:
		b = int16(value.(uint32))
	case reflect.Float64:
		b = int16(value.(uint64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setInt32Value(varField reflect.Value, value interface{}) {
	fval := reflect.ValueOf(value)
	var b int32

	switch fval.Kind() {
	case reflect.Int32:
		b = value.(int32)
	case reflect.Int:
		b = int32(value.(int))
	case reflect.Int8:
		b = int32(value.(int8))
	case reflect.Int16:
		b = int32(value.(int16))
	case reflect.Int64:
		b = int32(value.(int64))
	case reflect.Uint:
		b = int32(value.(uint))
	case reflect.Uint8:
		b = int32(value.(uint8))
	case reflect.Uint16:
		b = int32(value.(uint16))
	case reflect.Uint32:
		b = int32(value.(uint32))
	case reflect.Uint64:
		b = int32(value.(uint64))
	case reflect.Float32:
		b = int32(value.(float32))
	case reflect.Float64:
		b = int32(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setInt64Value(varField reflect.Value, value interface{}) {
	fval := reflect.ValueOf(value)
	var b int64

	switch fval.Kind() {
	case reflect.Int64:
		b = value.(int64)
	case reflect.Int:
		b = int64(value.(int))
	case reflect.Int8:
		b = int64(value.(int8))
	case reflect.Int16:
		b = int64(value.(int16))
	case reflect.Int32:
		b = int64(value.(int32))
	case reflect.Uint:
		b = int64(value.(uint))
	case reflect.Uint8:
		b = int64(value.(uint8))
	case reflect.Uint16:
		b = int64(value.(uint16))
	case reflect.Uint32:
		b = int64(value.(uint32))
	case reflect.Uint64:
		b = int64(value.(uint64))
	case reflect.Float32:
		b = int64(value.(uint32))
	case reflect.Float64:
		b = int64(value.(uint64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setUIntValue(varField reflect.Value, value interface{}) {
	fval := reflect.ValueOf(value)
	var b uint

	switch fval.Kind() {
	case reflect.Uint:
		b = value.(uint)
	case reflect.Int8:
		b = uint(value.(int8))
	case reflect.Int16:
		b = uint(value.(int16))
	case reflect.Int32:
		b = uint(value.(int32))
	case reflect.Int64:
		b = uint(value.(int64))
	case reflect.Int:
		b = uint(value.(int))
	case reflect.Uint8:
		b = uint(value.(uint8))
	case reflect.Uint16:
		b = uint(value.(uint16))
	case reflect.Uint32:
		b = uint(value.(uint32))
	case reflect.Uint64:
		b = uint(value.(uint64))
	case reflect.Float32:
		b = uint(value.(float32))
	case reflect.Float64:
		b = uint(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setUInt8Value(varField reflect.Value, value interface{}) {
	fval := reflect.ValueOf(value)
	var b uint8

	switch fval.Kind() {
	case reflect.Uint8:
		b = value.(uint8)
	case reflect.Int:
		b = uint8(value.(int))
	case reflect.Int16:
		b = uint8(value.(int16))
	case reflect.Int32:
		b = uint8(value.(int32))
	case reflect.Int64:
		b = uint8(value.(int64))
	case reflect.Uint:
		b = uint8(value.(uint))
	case reflect.Int8:
		b = uint8(value.(int8))
	case reflect.Uint16:
		b = uint8(value.(uint16))
	case reflect.Uint32:
		b = uint8(value.(uint32))
	case reflect.Uint64:
		b = uint8(value.(uint64))
	case reflect.Float32:
		b = uint8(value.(float32))
	case reflect.Float64:
		b = uint8(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setUInt16Value(varField reflect.Value, value interface{}) {
	fval := reflect.ValueOf(value)
	var b uint16

	switch fval.Kind() {
	case reflect.Uint16:
		b = value.(uint16)
	case reflect.Int:
		b = uint16(value.(int))
	case reflect.Int8:
		b = uint16(value.(int8))
	case reflect.Int32:
		b = uint16(value.(int32))
	case reflect.Int64:
		b = uint16(value.(int64))
	case reflect.Uint:
		b = uint16(value.(uint))
	case reflect.Uint8:
		b = uint16(value.(uint8))
	case reflect.Int16:
		b = uint16(value.(int16))
	case reflect.Uint32:
		b = uint16(value.(uint32))
	case reflect.Uint64:
		b = uint16(value.(uint64))
	case reflect.Float32:
		b = uint16(value.(float32))
	case reflect.Float64:
		b = uint16(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setUInt32Value(varField reflect.Value, value interface{}) {
	fval := reflect.ValueOf(value)
	var b uint32

	switch fval.Kind() {
	case reflect.Uint32:
		b = value.(uint32)
	case reflect.Int:
		b = uint32(value.(int))
	case reflect.Int8:
		b = uint32(value.(int8))
	case reflect.Int16:
		b = uint32(value.(int16))
	case reflect.Int64:
		b = uint32(value.(int64))
	case reflect.Uint:
		b = uint32(value.(uint))
	case reflect.Uint8:
		b = uint32(value.(uint8))
	case reflect.Uint16:
		b = uint32(value.(uint16))
	case reflect.Int32:
		b = uint32(value.(int32))
	case reflect.Uint64:
		b = uint32(value.(uint64))
	case reflect.Float32:
		b = uint32(value.(float32))
	case reflect.Float64:
		b = uint32(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setUInt64Value(varField reflect.Value, value interface{}) {
	fval := reflect.ValueOf(value)
	var b uint64

	switch fval.Kind() {
	case reflect.Uint64:
		b = value.(uint64)
	case reflect.Int:
		b = uint64(value.(int))
	case reflect.Int8:
		b = uint64(value.(int8))
	case reflect.Int16:
		b = uint64(value.(int16))
	case reflect.Int32:
		b = uint64(value.(int32))
	case reflect.Uint:
		b = uint64(value.(uint))
	case reflect.Uint8:
		b = uint64(value.(uint8))
	case reflect.Uint16:
		b = uint64(value.(uint16))
	case reflect.Uint32:
		b = uint64(value.(uint32))
	case reflect.Int64:
		b = uint64(value.(int64))
	case reflect.Float32:
		b = uint64(value.(float32))
	case reflect.Float64:
		b = uint64(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setFloat32Value(varField reflect.Value, value interface{}) {
	fval := reflect.ValueOf(value)
	var b float32

	switch fval.Kind() {
	case reflect.Float32:
		b = value.(float32)
	case reflect.Int:
		b = float32(value.(int))
	case reflect.Int8:
		b = float32(value.(int8))
	case reflect.Int16:
		b = float32(value.(int16))
	case reflect.Int32:
		b = float32(value.(int32))
	case reflect.Uint:
		b = float32(value.(uint))
	case reflect.Uint8:
		b = float32(value.(uint8))
	case reflect.Uint16:
		b = float32(value.(uint16))
	case reflect.Uint32:
		b = float32(value.(uint32))
	case reflect.Int64:
		b = float32(value.(int64))
	case reflect.Uint64:
		b = float32(value.(uint64))
	case reflect.Float64:
		b = float32(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setFloat64Value(varField reflect.Value, value interface{}) {
	fval := reflect.ValueOf(value)
	var b float64

	switch fval.Kind() {
	case reflect.Float64:
		b = value.(float64)
	case reflect.Int:
		b = float64(value.(int))
	case reflect.Int8:
		b = float64(value.(int8))
	case reflect.Int16:
		b = float64(value.(int16))
	case reflect.Int32:
		b = float64(value.(int32))
	case reflect.Uint:
		b = float64(value.(uint))
	case reflect.Uint8:
		b = float64(value.(uint8))
	case reflect.Uint16:
		b = float64(value.(uint16))
	case reflect.Uint32:
		b = float64(value.(uint32))
	case reflect.Int64:
		b = float64(value.(int64))
	case reflect.Uint64:
		b = float64(value.(uint64))
	case reflect.Float32:
		b = float64(value.(float32))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
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
