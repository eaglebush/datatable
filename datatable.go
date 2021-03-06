package datatable

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

//Column - a column in the data table
type Column struct {
	Name   string
	Type   reflect.Type
	DBType string
	Length int64
}

//Row - a row in the data table
type Row struct {
	Cells                   []Cell
	ColumnCount             int
	sqlRows                 *sql.Rows //Pointer to internal sql.Rows as a result from GetDataReader() call
	tmpRows                 []interface{}
	cellsInited             bool          //internal variable for Next() as a result from GetDataReader() call
	ResultRows              []interface{} //raw variable to as a result for calling Next() in a GetDataReader() call
	currentColumnNamesIndex map[string]int
}

//Cell - a location for the value
type Cell struct {
	ColumnName   string
	ColumnIndex  int
	RowIndex     int
	DBColumnType string
	Value        interface{}
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
	return &DataTable{Name: Name}
}

//AddColumn - add a new column to the data table
func (dt *DataTable) AddColumn(name string, vartype reflect.Type, length int64, DBType string) {
	exists := false
	name = strings.ToLower(name)
	for _, col := range dt.Columns {
		if strings.ToLower(col.Name) == name {
			exists = true
			break
		}
	}

	if !exists {
		col := Column{Name: name, Type: vartype, Length: length, DBType: DBType}
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
/*
func (dt *DataTable) AddRow(row Row) {
	var r Row
	r.ColumnCount = row.ColumnCount
	r.Cells = append(r.Cells, row.Cells...)

	// Adjust row index
	for i := range row.Cells {
		r.Cells[i].RowIndex = dt.RowCount
		r.Cells[i].ColumnIndex = i
	}

	dt.Rows = append(dt.Rows, r)
	dt.RowCount = dt.RowCount + 1
	//log.Println(dt.RowCount)
}
*/

// AddRow - add a row to the current rows
func (dt *DataTable) AddRow(row *Row) {
	var r Row
	r.ColumnCount = row.ColumnCount
	r.Cells = append(r.Cells, row.Cells...)
	r.currentColumnNamesIndex = make(map[string]int)

	/* Adjust row index */
	for i := range row.Cells {
		r.Cells[i].RowIndex = dt.RowCount
		r.Cells[i].ColumnIndex = i
		r.currentColumnNamesIndex[strings.ToLower(row.Cells[i].ColumnName)] = i
	}

	dt.Rows = append(dt.Rows, r)
	dt.RowCount = dt.RowCount + 1
	//log.Println(dt.RowCount)
}

// AddRows - adds a range of rows to the current data table
func (dt *DataTable) AddRows(rows []Row) {
	lastcnt := dt.RowCount
	cnt := len(rows)
	dt.RowCount = lastcnt + cnt
	dt.Rows = append(dt.Rows, rows...)

	for f := lastcnt; f < dt.RowCount; f++ {
		for g := 0; g < dt.ColumnCount; g++ {
			dt.Rows[f].Cells[g].RowIndex = f
			dt.Rows[f].Cells[g].ColumnIndex = g
		}
	}

	rows = nil
}

// NewRow - returns a new row based on column structure
func (dt *DataTable) NewRow() Row {
	colcnt := len(dt.Columns)
	r := Row{Cells: make([]Cell, colcnt), ColumnCount: colcnt}
	r.currentColumnNamesIndex = make(map[string]int)

	for i, cl := range dt.Columns {
		r.Cells[i].ColumnIndex = i
		r.Cells[i].ColumnName = cl.Name
		r.Cells[i].DBColumnType = cl.DBType
		r.currentColumnNamesIndex[strings.ToLower(cl.Name)] = i
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

//SetSQLRow - sets a pointer to an sql.Row object to allow next row reading
func (rw *Row) SetSQLRow(rows *sql.Rows) {
	rw.sqlRows = rows
}

//Next - gets the next row from a GetDataReader function call
func (rw *Row) Next() bool {
	if rw.sqlRows == nil {
		return false
	}

	if !rw.sqlRows.Next() {
		return false
	}

	//colcnt := len(cols)

	if !rw.cellsInited {
		rw.currentColumnNamesIndex = make(map[string]int)

		cols, _ := rw.sqlRows.Columns()
		colt, _ := rw.sqlRows.ColumnTypes()
		colcnt := len(cols)

		rw.ResultRows = make([]interface{}, colcnt)
		rw.tmpRows = make([]interface{}, colcnt)
		rw.Cells = make([]Cell, colcnt)
		rw.ColumnCount = colcnt

		for i := 0; i < len(cols); i++ {
			rw.Cells[i].ColumnIndex = i
			rw.Cells[i].ColumnName = cols[i]
			rw.Cells[i].DBColumnType = colt[i].DatabaseTypeName()

			rw.currentColumnNamesIndex[strings.ToLower(cols[i])] = i

			// Initialize rows
			rw.ResultRows[i] = new(interface{})
			rw.tmpRows[i] = new(interface{})
		}

		rw.cellsInited = true
	}

	err := rw.sqlRows.Scan(rw.tmpRows...)
	if err != nil {
		log.Println("Error Next: " + err.Error())
		return false
	}

	ccnt := rw.ColumnCount
	for i := 0; i < ccnt; i++ {
		v := rw.tmpRows[i].(*interface{})
		if *v != nil {
			switch rw.Cells[i].DBColumnType {
			case "DECIMAL":
				f, _ := strconv.ParseFloat(string((*v).([]uint8)), 64)
				rw.ResultRows[i] = &f
				rw.Cells[i].Value = f
			default:
				rw.Cells[i].Value = *v
				rw.ResultRows[i] = v
			}
		} else {
			rw.Cells[i].Value = nil
		}
	}

	return true
}

//Close - closes sqlRow from a GetDataReader function call. Also resets the values in its cells
func (rw *Row) Close() {
	rw.cellsInited = false
	rw.ResultRows = rw.ResultRows[:0]
	for i := 0; i < len(rw.Cells); i++ {
		rw.Cells[i].Value = nil
	}
	rw.Cells = rw.Cells[:0]
	if rw.sqlRows != nil {
		rw.sqlRows.Close()
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
		var ok bool
		idx, ok = rw.currentColumnNamesIndex[index.(string)]
		if !ok {
			idx = -1
			if rw.currentColumnNamesIndex == nil {
				rw.currentColumnNamesIndex = make(map[string]int)
			}
			kname := strings.ToLower(index.(string))
			for i, c := range rw.Cells {
				if strings.ToLower(c.ColumnName) == kname {
					idx = i

					rw.currentColumnNamesIndex[kname] = i
					break
				}
			}
		}
	}

	if idx != -1 {
		c := rw.Cells[idx]
		if c.Value != nil {
			v := strings.ToLower(reflect.TypeOf(c.Value).String())

			switch v {
			case "[]uint8":
				switch strings.ToUpper(c.DBColumnType) {
				case "IMAGE":
					return c.Value.([]uint8)
				default:
					return string(c.Value.([]uint8))
				}
			}
		}
		return c.Value
	}

	return nil
}

// ValueByOrdinal - get values by ordinal index
func (rw *Row) ValueByOrdinal(index *int) interface{} {
	if *index > len(rw.Cells) {
		return nil
	}
	c := rw.Cells[*index]
	if c.Value != nil {
		v := strings.ToLower(reflect.TypeOf(c.Value).String())
		switch v {
		case "[]uint8":
			switch strings.ToUpper(c.DBColumnType) {
			case "IMAGE":
				return c.Value.([]uint8)
			default:
				return string(c.Value.([]uint8))
			}
		}
	}
	return c.Value
}

// ValueByName - get values by column name index
func (rw *Row) ValueByName(index *string) interface{} {
	idx := -1

	kname := strings.ToLower(*index)
	var ok bool
	idx, ok = rw.currentColumnNamesIndex[kname]
	if !ok {
		if rw.currentColumnNamesIndex == nil {
			rw.currentColumnNamesIndex = make(map[string]int)
		}
		idx = -1
		for i := range rw.Cells {
			if strings.ToLower(rw.Cells[i].ColumnName) == kname {
				idx = i
				rw.currentColumnNamesIndex[kname] = i
				break
			}
		}
	}

	if idx == -1 {
		return nil
	}

	c := rw.Cells[idx]
	if c.Value != nil {
		v := strings.ToLower(reflect.TypeOf(c.Value).String())
		switch v {
		case "[]uint8":
			switch strings.ToUpper(c.DBColumnType) {
			case "IMAGE":
				return c.Value.([]uint8)
			case "DECIMAL":
				str := string(c.Value.([]uint8))
				flt, _ := strconv.ParseFloat(str, 64)
				return flt
			default:
				return string(c.Value.([]uint8))
			}
		}
	}
	return c.Value
}

// SetValueByOrd - sets a struct item with a value from the row specified by an index ordinal
func (rw *Row) SetValueByOrd(StructItem interface{}, FieldIndex int) {
	fv := rw.ValueByOrdinal(&FieldIndex)
	varbl := reflect.ValueOf(StructItem).Elem() //Get the reflection value of the Variable to set value later
	v := varbl.Interface()                      //convert back to interface type to allow type checking

	switch t := v.(type) {
	case int:
		setIntValue(varbl, fv)
	case int8:
		setInt8Value(varbl, fv)
	case int16:
		setInt16Value(varbl, fv)
	case int32:
		setInt32Value(varbl, fv)
	case int64:
		setInt64Value(varbl, fv)
	case uint:
		setUIntValue(varbl, fv)
	case uint8:
		setUInt8Value(varbl, fv)
	case uint16:
		setUInt16Value(varbl, fv)
	case uint32:
		setUInt32Value(varbl, fv)
	case uint64:
		setUInt64Value(varbl, fv)
	case float32:
		setFloat32Value(varbl, fv)
	case float64:
		setFloat64Value(varbl, fv)
	case bool:
		setBoolValue(varbl, fv)
	case string:
		setStringValue(varbl, fv)
	case time.Time:
		setTimeValue(varbl, fv)
	case []uint8:
		c := reflect.ValueOf(fv)
		varbl.Set(c)
	default:
		fmt.Println(t)
	}
}

//SetValue - sets a struct item with a value from the row specified by an index
func (rw *Row) SetValue(StructItem interface{}, FieldIndex string) {
	fv := rw.ValueByName(&FieldIndex)
	varbl := reflect.ValueOf(StructItem).Elem() //Get the reflection value of the Variable to set value later
	v := varbl.Interface()                      //convert back to interface type to allow type checking

	switch t := v.(type) {
	case int:
		setIntValue(varbl, fv)
	case int8:
		setInt8Value(varbl, fv)
	case int16:
		setInt16Value(varbl, fv)
	case int32:
		setInt32Value(varbl, fv)
	case int64:
		setInt64Value(varbl, fv)
	case uint:
		setUIntValue(varbl, fv)
	case uint8:
		setUInt8Value(varbl, fv)
	case uint16:
		setUInt16Value(varbl, fv)
	case uint32:
		setUInt32Value(varbl, fv)
	case uint64:
		setUInt64Value(varbl, fv)
	case float32:
		setFloat32Value(varbl, fv)
	case float64:
		setFloat64Value(varbl, fv)
	case bool:
		setBoolValue(varbl, fv)
	case string:
		setStringValue(varbl, fv)
	case time.Time:
		setTimeValue(varbl, fv)
	case []uint8:
		c := reflect.ValueOf(fv)
		varbl.Set(c)
	default:
		fmt.Println(t)
	}

}

func setIntValue(varField reflect.Value, value interface{}) {

	var b int

	switch value.(type) {
	case int:
		b = value.(int)
	case int8:
		b = int(value.(int8))
	case int16:
		b = int(value.(int16))
	case int32:
		b = int(value.(int32))
	case int64:
		b = int(value.(int64))
	case uint:
		b = int(value.(uint))
	case uint8:
		b = int(value.(uint8))
	case uint16:
		b = int(value.(uint16))
	case uint32:
		b = int(value.(uint32))
	case uint64:
		b = int(value.(uint64))
	case float32:
		b = int(value.(float32))
	case float64:
		b = int(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setInt8Value(varField reflect.Value, value interface{}) {

	var b int8

	switch value.(type) {
	case int8:
		b = value.(int8)
	case int:
		b = int8(value.(int))
	case int16:
		b = int8(value.(int16))
	case int32:
		b = int8(value.(int32))
	case int64:
		b = int8(value.(int64))
	case uint:
		b = int8(value.(uint))
	case uint8:
		b = int8(value.(uint8))
	case uint16:
		b = int8(value.(uint16))
	case uint32:
		b = int8(value.(uint32))
	case uint64:
		b = int8(value.(uint64))
	case float32:
		b = int8(value.(float32))
	case float64:
		b = int8(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setInt16Value(varField reflect.Value, value interface{}) {

	var b int16

	switch value.(type) {
	case int16:
		b = value.(int16)
	case int:
		b = int16(value.(int))
	case int8:
		b = int16(value.(int8))
	case int32:
		b = int16(value.(int32))
	case int64:
		b = int16(value.(int64))
	case uint:
		b = int16(value.(uint))
	case uint8:
		b = int16(value.(uint8))
	case uint16:
		b = int16(value.(uint16))
	case uint32:
		b = int16(value.(uint32))
	case uint64:
		b = int16(value.(uint64))
	case float32:
		b = int16(value.(uint32))
	case float64:
		b = int16(value.(uint64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setInt32Value(varField reflect.Value, value interface{}) {

	var b int32

	switch value.(type) {
	case int32:
		b = value.(int32)
	case int:
		b = int32(value.(int))
	case int8:
		b = int32(value.(int8))
	case int16:
		b = int32(value.(int16))
	case int64:
		b = int32(value.(int64))
	case uint:
		b = int32(value.(uint))
	case uint8:
		b = int32(value.(uint8))
	case uint16:
		b = int32(value.(uint16))
	case uint32:
		b = int32(value.(uint32))
	case uint64:
		b = int32(value.(uint64))
	case float32:
		b = int32(value.(float32))
	case float64:
		b = int32(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setInt64Value(varField reflect.Value, value interface{}) {
	var b int64

	switch value.(type) {
	case int64:
		b = value.(int64)
	case int:
		b = int64(value.(int))
	case int8:
		b = int64(value.(int8))
	case int16:
		b = int64(value.(int16))
	case int32:
		b = int64(value.(int32))
	case uint:
		b = int64(value.(uint))
	case uint8:
		b = int64(value.(uint8))
	case uint16:
		b = int64(value.(uint16))
	case uint32:
		b = int64(value.(uint32))
	case uint64:
		b = int64(value.(uint64))
	case float32:
		b = int64(value.(uint32))
	case float64:
		b = int64(value.(uint64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setUIntValue(varField reflect.Value, value interface{}) {

	var b uint

	switch value.(type) {
	case uint:
		b = value.(uint)
	case int8:
		b = uint(value.(int8))
	case int16:
		b = uint(value.(int16))
	case int32:
		b = uint(value.(int32))
	case int64:
		b = uint(value.(int64))
	case int:
		b = uint(value.(int))
	case uint8:
		b = uint(value.(uint8))
	case uint16:
		b = uint(value.(uint16))
	case uint32:
		b = uint(value.(uint32))
	case uint64:
		b = uint(value.(uint64))
	case float32:
		b = uint(value.(float32))
	case float64:
		b = uint(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setUInt8Value(varField reflect.Value, value interface{}) {
	var b uint8

	switch value.(type) {
	case uint8:
		b = value.(uint8)
	case int:
		b = uint8(value.(int))
	case int16:
		b = uint8(value.(int16))
	case int32:
		b = uint8(value.(int32))
	case int64:
		b = uint8(value.(int64))
	case uint:
		b = uint8(value.(uint))
	case int8:
		b = uint8(value.(int8))
	case uint16:
		b = uint8(value.(uint16))
	case uint32:
		b = uint8(value.(uint32))
	case uint64:
		b = uint8(value.(uint64))
	case float32:
		b = uint8(value.(float32))
	case float64:
		b = uint8(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setUInt16Value(varField reflect.Value, value interface{}) {
	var b uint16

	switch value.(type) {
	case uint16:
		b = value.(uint16)
	case int:
		b = uint16(value.(int))
	case int8:
		b = uint16(value.(int8))
	case int32:
		b = uint16(value.(int32))
	case int64:
		b = uint16(value.(int64))
	case uint:
		b = uint16(value.(uint))
	case uint8:
		b = uint16(value.(uint8))
	case int16:
		b = uint16(value.(int16))
	case uint32:
		b = uint16(value.(uint32))
	case uint64:
		b = uint16(value.(uint64))
	case float32:
		b = uint16(value.(float32))
	case float64:
		b = uint16(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setUInt32Value(varField reflect.Value, value interface{}) {
	var b uint32

	switch value.(type) {
	case uint32:
		b = value.(uint32)
	case int:
		b = uint32(value.(int))
	case int8:
		b = uint32(value.(int8))
	case int16:
		b = uint32(value.(int16))
	case int64:
		b = uint32(value.(int64))
	case uint:
		b = uint32(value.(uint))
	case uint8:
		b = uint32(value.(uint8))
	case uint16:
		b = uint32(value.(uint16))
	case int32:
		b = uint32(value.(int32))
	case uint64:
		b = uint32(value.(uint64))
	case float32:
		b = uint32(value.(float32))
	case float64:
		b = uint32(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setUInt64Value(varField reflect.Value, value interface{}) {
	var b uint64

	switch value.(type) {
	case uint64:
		b = value.(uint64)
	case int:
		b = uint64(value.(int))
	case int8:
		b = uint64(value.(int8))
	case int16:
		b = uint64(value.(int16))
	case int32:
		b = uint64(value.(int32))
	case int64:
		b = uint64(value.(int64))
	case uint:
		b = uint64(value.(uint))
	case uint8:
		b = uint64(value.(uint8))
	case uint16:
		b = uint64(value.(uint16))
	case uint32:
		b = uint64(value.(uint32))
	case float32:
		b = uint64(value.(float32))
	case float64:
		b = uint64(value.(float64))
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setFloat32Value(varField reflect.Value, value interface{}) {
	var b float32

	b = 0.0
	switch value.(type) {
	case float32:
		b = value.(float32)
	case int:
		b = float32(value.(int))
	case int8:
		b = float32(value.(int8))
	case int16:
		b = float32(value.(int16))
	case int32:
		b = float32(value.(int32))
	case int64:
		b = float32(value.(int64))
	case uint:
		b = float32(value.(uint))
	case uint8:
		b = float32(value.(uint8))
	case uint16:
		b = float32(value.(uint16))
	case uint32:
		b = float32(value.(uint32))
	case uint64:
		b = float32(value.(uint64))
	case float64:
		b = float32(value.(float64))
	default:
		if value != nil {
			str := value.(string)
			c, _ := strconv.ParseFloat(str, 32)
			b = float32(c)
		}
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setFloat64Value(varField reflect.Value, value interface{}) {
	var b float64

	b = 0.0
	switch value.(type) {
	case float64:
		b = value.(float64)
	case int:
		b = float64(value.(int))
	case int8:
		b = float64(value.(int8))
	case int16:
		b = float64(value.(int16))
	case int32:
		b = float64(value.(int32))
	case int64:
		b = float64(value.(int64))
	case uint:
		b = float64(value.(uint))
	case uint8:
		b = float64(value.(uint8))
	case uint16:
		b = float64(value.(uint16))
	case uint32:
		b = float64(value.(uint32))
	case uint64:
		b = float64(value.(uint64))
	case float32:
		b = float64(value.(float32))
	default:
		if value != nil {
			str := value.(string)
			b, _ = strconv.ParseFloat(str, 64)
		}
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setStringValue(varField reflect.Value, value interface{}) {
	var b string

	switch value.(type) {
	case string:
		b = value.(string)
	case int:
		b = strconv.FormatInt(int64(value.(int)), 10)
	case int8:
		b = strconv.FormatInt(int64(value.(int8)), 10)
	case int16:
		b = strconv.FormatInt(int64(value.(int16)), 10)
	case int32:
		b = strconv.FormatInt(int64(value.(int32)), 10)
	case int64:
		b = strconv.FormatInt(value.(int64), 10)
	case uint:
		b = strconv.FormatUint(uint64(value.(uint)), 10)
	case uint8:
		b = strconv.FormatUint(uint64(value.(uint8)), 10)
	case uint16:
		b = strconv.FormatUint(uint64(value.(int16)), 10)
	case uint32:
		b = strconv.FormatUint(uint64(value.(int32)), 10)
	case uint64:
		b = strconv.FormatUint(uint64(value.(uint64)), 10)
	case float32:
		b = fmt.Sprintf("%f", value.(float32))
	case float64:
		b = fmt.Sprintf("%f", value.(float32))
	case bool:
		b = "false"
		s := strings.ToLower(value.(string))
		if len(s) > 0 {
			if s == "true" || s == "on" || s == "yes" || s == "1" || s == "-1" {
				b = "true"
			}
		}
	case time.Time:
		b = "'" + value.(time.Time).Format(time.RFC3339) + "'"
	}

	c := reflect.ValueOf(b)
	varField.Set(c)

}

func setBoolValue(varField reflect.Value, value interface{}) {
	var b bool

	b = false

	switch value.(type) {
	case string:
		s := strings.ToLower(value.(string))
		if len(s) > 0 {
			if s == "true" || s == "on" || s == "yes" || s == "1" || s == "-1" {
				b = true
			}
		}
	case int:
		s := value.(int)
		if s >= 1 || s <= -1 {
			b = true
		}
	case int8:
		s := value.(int8)
		if s >= 1 || s <= -1 {
			b = true
		}
	case int16:
		s := value.(int16)
		if s >= 1 || s <= -1 {
			b = true
		}
	case int64:
		s := value.(int64)
		if s >= 1 || s <= -1 {
			b = true
		}
	case uint:
		s := value.(uint)
		if s >= 1 {
			b = true
		}
	case uint8:
		s := value.(uint8)
		if s >= 1 {
			b = true
		}
	case uint16:
		s := value.(uint16)
		if s >= 1 {
			b = true
		}
	case int32:
		s := value.(int32)
		if s >= 1 || s <= -1 {
			b = true
		}
	case uint64:
		s := value.(uint64)
		if s >= 1 {
			b = true
		}
	case float32:
		s := value.(float32)
		if s >= 1 || s <= -1 {
			b = true
		}
	case float64:
		s := value.(float64)
		if s >= 1 || s <= -1 {
			b = true
		}
	case bool:
		b = value.(bool)
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

func setTimeValue(varField reflect.Value, value interface{}) {
	var b time.Time

	switch value.(type) {
	case string:
		s := value.(string)
		b, _ = time.Parse(time.RFC3339, s)
	case time.Time:
		b = value.(time.Time)
	}

	c := reflect.ValueOf(b)
	varField.Set(c)
}

//ValueString - return the value as string or a default empty string if the value is null
func (rw *Row) ValueString(index string) string {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return ""
	}

	return ret.(string)
}

//ValuePtrString - return the value as pointer to string or nil if the value is null
func (rw *Row) ValuePtrString(index string) *string {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(string)
	return &pret
}

//ValueTime - return the value as time.Time or a default empty time.Time if the value is null
func (rw *Row) ValueTime(index string) time.Time {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return time.Time{}
	}

	return ret.(time.Time)
}

//ValuePtrTime - return the value as pointer to time.Time or nil if the value is null
func (rw *Row) ValuePtrTime(index string) *time.Time {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(time.Time)
	return &pret
}

// ValueBool - return the value as boolean or a false if the value is null.
// This can also be used for columns that resembles a boolean value, and converting it to boolean.
// Column values such as `true`, 'on', 'yes', '1' or 1 and -1 are converted to true value.
// Anything else than these are converted to false
func (rw *Row) ValueBool(index string) bool {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return false
	}

	// Many SQL database does not agree on boolean datatype. So, we just convert them to string and have them evaluate
	s := anyToString(ret)
	if s == "true" || s == "on" || s == "yes" || s == "1" || s == "-1" {
		return true
	}

	return false
}

// ValuePtrBool - return the value as pointer to boolean or nil if the value is null
// This can also be used for columns that resembles a boolean value, and converting it to boolean.
// Column values such as `true`, 'on', 'yes', '1' or 1 and -1 are converted to true value.
// Anything else than these are converted to false
func (rw *Row) ValuePtrBool(index string) *bool {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return nil
	}

	pret := false

	// Many SQL database does not agree on boolean datatype. So, we just convert them to string and have them evaluate
	s := anyToString(ret)
	if s == "true" || s == "on" || s == "yes" || s == "1" || s == "-1" {
		pret = true
	}

	return &pret
}

//ValueFloat64 - return the value as float64 or a 0 if the value is null
func (rw *Row) ValueFloat64(index string) float64 {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return 0
	}

	return ret.(float64)
}

//ValuePtrFloat64 - return the value as pointer to float64 or a nil if the value is null
func (rw *Row) ValuePtrFloat64(index string) *float64 {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(float64)
	return &pret
}

//ValueFloat32 - return the value as float32 or a 0 if the value is null
func (rw *Row) ValueFloat32(index string) float32 {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return 0
	}

	return ret.(float32)
}

//ValuePtrFloat32 - return the value as pointer to float32 or a nil if the value is null
func (rw *Row) ValuePtrFloat32(index string) *float32 {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(float32)
	return &pret
}

//ValueInt - return the value as int or a 0 if the value is null
func (rw *Row) ValueInt(index string) int {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return 0
	}

	return ret.(int)
}

//ValuePtrInt - return the value as pointer to int or a nil if the value is null
func (rw *Row) ValuePtrInt(index string) *int {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(int)
	return &pret
}

//ValueInt16 - return the value as int16 or a 0 if the value is null
func (rw *Row) ValueInt16(index string) int16 {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return 0
	}

	return ret.(int16)
}

//ValuePtrInt16 - return the value as pointer to int16 or a nil if the value is null
func (rw *Row) ValuePtrInt16(index string) *int16 {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(int16)
	return &pret
}

//ValueInt32 - return the value as int32 or a 0 if the value is null
func (rw *Row) ValueInt32(index string) int32 {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return 0
	}

	return ret.(int32)
}

//ValuePtrInt32 - return the value as pointer to int32 or a nil if the value is null
func (rw *Row) ValuePtrInt32(index string) *int32 {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(int32)
	return &pret
}

//ValueInt64 - return the value as int64 or a 0 if the value is null
func (rw *Row) ValueInt64(index string) int64 {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return 0
	}

	return ret.(int64)
}

//ValuePtrInt64 - return the value as pointer to int64 or a nil if the value is null
func (rw *Row) ValuePtrInt64(index string) *int64 {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(int64)
	return &pret
}

//ValueByte - return the value as byte or a 0 if the value is null
func (rw *Row) ValueByte(index string) byte {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return 0
	}

	return ret.(byte)
}

//ValuePtrByte - return the value as pointer to byte or a nil if the value is null
func (rw *Row) ValuePtrByte(index string) *byte {
	ret := rw.ValueByName(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(byte)
	return &pret
}

//ValueStringOrd - return the value as string or a default empty string if the value is null by ordinal
func (rw *Row) ValueStringOrd(index int) string {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return ""
	}

	return ret.(string)
}

//ValuePtrStringOrd - return the value as pointer to string or nil if the value is null
func (rw *Row) ValuePtrStringOrd(index int) *string {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(string)
	return &pret
}

//ValueTimeOrd - return the value as time.Time or a default empty time.Time if the value is null by ordinal
func (rw *Row) ValueTimeOrd(index int) time.Time {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return time.Time{}
	}

	return ret.(time.Time)
}

//ValuePtrTimeOrd - return the value as pointer time.Time or a nil if the value is null by ordinal
func (rw *Row) ValuePtrTimeOrd(index int) *time.Time {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(time.Time)
	return &pret
}

//ValueBoolOrd - return the value as boolean or a false if the value is null by ordinal
func (rw *Row) ValueBoolOrd(index int) bool {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return false
	}

	return ret.(bool)
}

//ValuePtrBoolOrd - return the value as pointer to boolean or a nil if the value is null by ordinal
func (rw *Row) ValuePtrBoolOrd(index int) *bool {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(bool)
	return &pret
}

//ValueFloat64Ord - return the value as float64 or a 0 if the value is null by ordinal
func (rw *Row) ValueFloat64Ord(index int) float64 {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return 0
	}

	return ret.(float64)
}

//ValuePtrFloat64Ord - return the value as pointer to float64 or a nil if the value is null by ordinal
func (rw *Row) ValuePtrFloat64Ord(index int) *float64 {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(float64)
	return &pret
}

//ValueFloat32Ord - return the value as float32 or a 0 if the value is null by ordinal
func (rw *Row) ValueFloat32Ord(index int) float32 {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return 0
	}

	return ret.(float32)
}

//ValuePtrFloat32Ord - return the value as pointer float32 or a nil if the value is null by ordinal
func (rw *Row) ValuePtrFloat32Ord(index int) *float32 {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(float32)
	return &pret
}

//ValueIntOrd - return the value as int or a 0 if the value is null by ordinal
func (rw *Row) ValueIntOrd(index int) int {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return 0
	}

	return ret.(int)
}

//ValuePtrIntOrd - return the value as pointer to int or a nil if the value is null by ordinal
func (rw *Row) ValuePtrIntOrd(index int) *int {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(int)
	return &pret
}

//ValueInt16Ord - return the value as int16 or a 0 if the value is null by ordinal
func (rw *Row) ValueInt16Ord(index int) int16 {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return 0
	}

	return ret.(int16)
}

//ValuePtrInt16Ord - return the value as pointer to int16 or a nil if the value is null by ordinal
func (rw *Row) ValuePtrInt16Ord(index int) *int16 {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(int16)
	return &pret
}

//ValueInt32Ord - return the value as int32 or a 0 if the value is null by ordinal
func (rw *Row) ValueInt32Ord(index int) int32 {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return 0
	}

	return ret.(int32)
}

//ValuePtrInt32Ord - return the value as pointer to int32 or a nil if the value is null by ordinal
func (rw *Row) ValuePtrInt32Ord(index int) *int32 {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(int32)
	return &pret
}

//ValueInt64Ord - return the value as int64 or a 0 if the value is null by ordinal
func (rw *Row) ValueInt64Ord(index int) int64 {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return 0
	}

	return ret.(int64)
}

//ValuePtrInt64Ord - return the value as pointer to int64 or a nil if the value is null by ordinal
func (rw *Row) ValuePtrInt64Ord(index int) *int64 {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(int64)
	return &pret
}

//ValueByteOrd - return the value as byte or a 0 if the value is null by ordinal
func (rw *Row) ValueByteOrd(index int) byte {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return 0
	}

	return ret.(byte)
}

//ValuePtrByteOrd - return the value as pointer to byte or a nil if the value is null by ordinal
func (rw *Row) ValuePtrByteOrd(index int) *byte {
	ret := rw.ValueByOrdinal(&index)

	if ret == nil {
		return nil
	}

	pret := ret.(byte)
	return &pret
}

func anyToString(value interface{}) string {
	var b string

	switch value.(type) {
	case string:
		b = value.(string)
	case int:
		b = strconv.FormatInt(int64(value.(int)), 10)
	case int8:
		b = strconv.FormatInt(int64(value.(int8)), 10)
	case int16:
		b = strconv.FormatInt(int64(value.(int16)), 10)
	case int32:
		b = strconv.FormatInt(int64(value.(int32)), 10)
	case int64:
		b = strconv.FormatInt(value.(int64), 10)
	case uint:
		b = strconv.FormatUint(uint64(value.(uint)), 10)
	case uint8:
		b = strconv.FormatUint(uint64(value.(uint8)), 10)
	case uint16:
		b = strconv.FormatUint(uint64(value.(uint16)), 10)
	case uint32:
		b = strconv.FormatUint(uint64(value.(uint32)), 10)
	case uint64:
		b = strconv.FormatUint(uint64(value.(uint64)), 10)
	case float32:
		b = fmt.Sprintf("%f", value.(float32))
	case float64:
		b = fmt.Sprintf("%f", value.(float64))
	case bool:
		b = "false"
		t, ok := value.(bool)
		if ok {
			b = strconv.FormatBool(t)
		}
	case time.Time:
		b = "'" + value.(time.Time).Format(time.RFC3339) + "'"
	}

	return b
}
