package postgres

import "testing"

func TestNewReader(t *testing.T) {
	pguri := "postgres://postgres@localhost:5432/test1?sslmode=disable"
	r := NewReader(pguri, "r1",
		map[string]string{
			"id1": "id::text",
		})
	if err := r.Open(); err != nil {
		t.Fatal(err)
	}
	t.Log(r)
	if err := r.DB.Ping(); err != nil {
		t.Fatal(err)
	}

	br := NewBatchReader(r, 20, "id, created_at")
	br.SetColumns([]string{"id", "created_at", "a"})
	_, err := br.Read()
	if err != nil {
		t.Fatal(err)
	}
}
