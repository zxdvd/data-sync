package postgres

import "testing"

func TestNewWriter(t *testing.T) {
	pguri := "postgres://postgres@localhost:5432/test1?sslmode=disable"
	w := NewWriter(pguri, "w1",
		[]string{"id"})
	if err := w.Open(); err != nil {
		t.Fatal(err)
	}
	if err := w.DB().Ping(); err != nil {
		t.Fatal(err)
	}

	rows := [][]interface{}{
		[]interface{}{3, "hello"},
		[]interface{}{2, "world"},
	}

	if err := w.BulkInsert(rows); err != nil {
		t.Fatal(err)
	}
}
