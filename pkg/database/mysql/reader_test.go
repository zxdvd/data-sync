package postgres

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestNewReader(t *testing.T) {
	mysqluri := "pigai_custom:pigai_custom@tcp(mysql.smartstudy.tech:3307)/pigai_custom?collation=utf8mb4_general_ci"
	r := NewReader(mysqluri, "r1")
	if err := r.Open(); err != nil {
		t.Fatal(err)
	}
	t.Log(r)
	if err := r.DB().Ping(); err != nil {
		t.Fatal(err)
	}

	br := NewBatchReader(r, 20, "id")
	br.SetSelectColumns([]string{"cast(id AS CHAR) as id1", "texta", "0 as n1"})
	_, err := br.BulkRead()
	if err != nil {
		panic(err)
		t.Fatal(err)
	}
}
