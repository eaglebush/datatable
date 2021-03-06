package datatable

import (
	"log"
	"reflect"
	"strconv"
	"testing"
)

func TestRowAdding(t *testing.T) {
	dt := NewDataTable("Simon")

	dt.AddColumn("ID", reflect.TypeOf(0), 0, "")
	dt.AddColumn("Code", reflect.TypeOf(""), 12, "")
	dt.AddColumn("Name", reflect.TypeOf(""), 15, "")

	var r Row

	r = dt.NewRow()
	r.Cells[0].Value = 1
	r.Cells[1].Value = "Test1"
	r.Cells[2].Value = "Test1 Name"
	dt.AddRow(&r)

	r = dt.NewRow()
	r.Cells[0].Value = 2
	r.Cells[1].Value = "Test2"
	r.Cells[2].Value = "Test2 Name"
	dt.AddRow(&r)

	r = dt.NewRow()
	r.Cells[0].Value = 3
	r.Cells[1].Value = "Test3"
	r.Cells[2].Value = "Test3 Name"
	dt.AddRow(&r)

	log.Printf("Table Name: %s\r\n", dt.Name)
	for i, rw := range dt.Rows {

		log.Printf("Row %d : ", i)
		for j, co := range rw.Cells {
			log.Printf("Column %s: %v, RowIndex: %d, ColumnIndex %d", dt.Columns[j].Name, co.Value, co.RowIndex, co.ColumnIndex)
		}
		log.Printf("\r\n")
	}
}

func TestRowGetValue(t *testing.T) {
	dt := NewDataTable("Simon")

	dt.AddColumns([]Column{
		{Name: "ID", Type: reflect.TypeOf(0), Length: 0},
		{Name: "Code", Type: reflect.TypeOf(""), Length: 12},
		{Name: "Name", Type: reflect.TypeOf(""), Length: 15},
	})

	var r Row
	var rs []Row

	r = dt.NewRow()
	r.Cells[0].Value = 0
	r.Cells[1].Value = "Test0"
	r.Cells[2].Value = "Test0 Name"
	dt.AddRow(&r)

	r = dt.NewRow()
	r.Cells[0].Value = 1
	r.Cells[1].Value = "Test1"
	r.Cells[2].Value = "Test1 Name"
	rs = append(rs, r)

	r = dt.NewRow()
	r.Cells[0].Value = 2
	r.Cells[1].Value = "Test2"
	r.Cells[2].Value = "Test2 Name"
	rs = append(rs, r)

	r = dt.NewRow()
	r.Cells[0].Value = 3
	r.Cells[1].Value = "Test3"
	r.Cells[2].Value = "Test3 Name"
	rs = append(rs, r)

	dt.AddRows(rs)
	log.Printf("Table Name: %s\r\n", dt.Name)
	for i, rw := range dt.Rows {
		log.Printf("Row %d: ", i)

		for _, co := range rw.Cells {
			//log.Printf("Column %s: %v, RowIndex: %d, ColumnIndex %d", co.ColumnName, rw.GetValue(co.ColumnName), co.RowIndex, co.ColumnIndex)
			log.Printf("Column %s: %v, RowIndex: %d, ColumnIndex %d", co.ColumnName, rw.Value(co.ColumnIndex), co.RowIndex, co.ColumnIndex)
		}
		log.Println()
	}

	log.Println("Getting value by column name index: ")
	for i, rw := range dt.Rows {
		log.Printf("Row %d: ", i)
		//log.Printf("Column %s: %v, RowIndex: %d, ColumnIndex %d", co.ColumnName, rw.GetValue(co.ColumnName), co.RowIndex, co.ColumnIndex)
		log.Printf("Column %s: %v", "Code", rw.ValueString("Code"))
		log.Printf("Column %s: %v", "Name", rw.ValueString("Name"))

		log.Println()
	}

	log.Println("Getting value by ordinal index: ")
	for i, rw := range dt.Rows {
		log.Printf("Row %d: ", i)
		//log.Printf("Column %s: %v, RowIndex: %d, ColumnIndex %d", co.ColumnName, rw.GetValue(co.ColumnName), co.RowIndex, co.ColumnIndex)
		log.Printf("Column %s: %v", "Code", rw.ValueStringOrd(1))
		log.Printf("Column %s: %v", "Name", rw.ValueStringOrd(2))

		log.Println()
	}
}

func TestRowAddingRange(t *testing.T) {
	dt := NewDataTable("Simon")

	dt.AddColumns([]Column{
		{Name: "ID", Type: reflect.TypeOf(0), Length: 0},
		{Name: "Code", Type: reflect.TypeOf(""), Length: 12},
		{Name: "Name", Type: reflect.TypeOf(""), Length: 15},
	})

	var r Row
	var rs []Row

	r = dt.NewRow()
	r.Cells[0].Value = 0
	r.Cells[1].Value = "Test0"
	r.Cells[2].Value = "Test0 Name"
	dt.AddRow(&r)

	r = dt.NewRow()
	r.Cells[0].Value = 1
	r.Cells[1].Value = "Test1"
	r.Cells[2].Value = "Test1 Name"
	rs = append(rs, r)

	r = dt.NewRow()
	r.Cells[0].Value = 2
	r.Cells[1].Value = "Test2"
	r.Cells[2].Value = "Test2 Name"
	rs = append(rs, r)

	r = dt.NewRow()
	r.Cells[0].Value = 3
	r.Cells[1].Value = "Test3"
	r.Cells[2].Value = "Test3 Name"
	rs = append(rs, r)

	dt.AddRows(rs)
	log.Printf("Table Name: %s\r\n", dt.Name)
	for i, rw := range dt.Rows {
		log.Printf("Row %d: ", i)
		for j, co := range rw.Cells {
			log.Printf("Column %s: %v, RowIndex: %d, ColumnIndex %d", dt.Columns[j].Name, co.Value, co.RowIndex, co.ColumnIndex)
		}
		log.Println()
	}
}

func TestRowSetValue(t *testing.T) {
	var i int64

	dtr := NewDataTable("RowsAffected")
	dtr.AddColumn("Affected", reflect.TypeOf(i), 0, "int")
	dtr.AddColumn("LastInsertId", reflect.TypeOf(i), 0, "int")

	ra := 1
	li := 123

	r := dtr.NewRow()
	r.SetValueByOrd(ra, 0)
	r.SetValueByOrd(li, 1)
	dtr.AddRow(&r)
}

func BenchmarkBulkRowAdding(b *testing.B) {
	dt := NewDataTable("Simon")

	dt.AddColumns([]Column{
		{Name: "ID", Type: reflect.TypeOf(0), Length: 0},
		{Name: "Code", Type: reflect.TypeOf(""), Length: 12},
		{Name: "Name", Type: reflect.TypeOf(""), Length: 15},
	})

	var r Row

	for i := 0; i < 50000; i++ {
		r = dt.NewRow()
		r.Cells[0].Value = i
		r.Cells[1].Value = "Test" + strconv.Itoa(i)
		r.Cells[2].Value = "Test" + strconv.Itoa(i) + " Name"
		dt.AddRow(&r)
	}

	log.Printf("Table Name: %s, RowCount %d\r\n", dt.Name, dt.RowCount)

	/*
		for i, rw := range dt.Rows {
			log.Printf("Row %d : ", i)
			for j, co := range rw.Cells {
				log.Printf("Column %s: %v, RowIndex: %d, ColumnIndex %d", dt.Columns[j].Name, co.Value, co.RowIndex, co.ColumnIndex)
			}
			log.Println()
		}
	*/
}

func BenchmarkBulkRowAddingRange(b *testing.B) {
	dt := NewDataTable("Simon")

	dt.AddColumns([]Column{
		{Name: "ID", Type: reflect.TypeOf(0), Length: 0},
		{Name: "Code", Type: reflect.TypeOf(""), Length: 12},
		{Name: "Name", Type: reflect.TypeOf(""), Length: 15},
	})

	var r Row
	var rs []Row

	for i := 0; i < 50000; i++ {
		r = dt.NewRow()
		r.Cells[0].Value = i
		r.Cells[1].Value = "Test" + strconv.Itoa(i)
		r.Cells[2].Value = "Test" + strconv.Itoa(i) + " Name"
		rs = append(rs, r)
	}

	dt.AddRows(rs)

	log.Printf("Table Name: %s, RowCount %d\r\n", dt.Name, dt.RowCount)

	/*
		for i, rw := range dt.Rows {
			log.Printf("Row %d : ", i)
			for j, co := range rw.Cells {
				log.Printf("Column %s: %v, RowIndex: %d, ColumnIndex %d", dt.Columns[j].Name, co.Value, co.RowIndex, co.ColumnIndex)
			}
			log.Println()
		}
	*/

}
