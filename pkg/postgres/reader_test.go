package postgres

import "testing"

func TestNewReader(t *testing.T) {
	pguri := "postgres://postgres@localhost:5432/test1?sslmode=disable"
	r := NewReader(pguri, "r1")
	if err := r.Open(); err != nil {
		t.Fatal(err)
	}
	t.Log(r)
	if err := r.DB().Ping(); err != nil {
		t.Fatal(err)
	}

	br := NewBatchReader(r, 20, "id, created_at")
	br.SetColumns([]string{"id::text as id1", "created_at", "a", "0 as n1"})
	_, err := br.BulkReader()
	if err != nil {
		panic(err)
		t.Fatal(err)
	}
}
