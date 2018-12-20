package datatable

import (
	"log"
	"reflect"
	"testing"
)

func TestRowAdding(t *testing.T) {
	dt := NewDataTable("Simon")

	dt.AddColumn("ID", reflect.TypeOf(0), 0)
	dt.AddColumn("Code", reflect.TypeOf(""), 12)
	dt.AddColumn("Name", reflect.TypeOf(""), 15)

	var r Row

	r = dt.NewRow()
	r.Cells[0].Value = 1
	r.Cells[1].Value = "Test1"
	r.Cells[2].Value = "Test1 Name"
	dt.AddRow(r)

	r = dt.NewRow()
	r.Cells[0].Value = 2
	r.Cells[1].Value = "Test2"
	r.Cells[2].Value = "Test2 Name"
	dt.AddRow(r)

	r = dt.NewRow()
	r.Cells[0].Value = 3
	r.Cells[1].Value = "Test3"
	r.Cells[2].Value = "Test3 Name"
	dt.AddRow(r)

	log.Printf("Table Name: %s\r\n", dt.Name)
	for i, rw := range dt.Rows {
		log.Printf("Row %d : ", i)
		for j, co := range rw.Cells {
			log.Printf("Column %s: %v, RowIndex: %d, ColumnIndex %d", dt.Columns[j].Name, co.Value, co.RowIndex, co.ColumnIndex)
		}
		log.Println()
	}
}

func TestRowAddingRange(t *testing.T) {
	dt := NewDataTable("Simon")
	dt.AddColumns([]Column{
		Column{Name: "ID", Type: reflect.TypeOf(0), Length: 0},
		Column{Name: "Code", Type: reflect.TypeOf(""), Length: 12},
		Column{Name: "Name", Type: reflect.TypeOf(""), Length: 15},
	})

	var r Row
	var rs []Row

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
