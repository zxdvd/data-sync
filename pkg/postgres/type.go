package postgres

import (
	"database/sql"

	_ "github.com/lib/pq"
)

type conn struct {
	uri string
	db  *sql.DB
}

type table struct {
	*conn
	tablename string
	columns   []column
	pks       []string
}

type reader struct {
	table
	// [id, A1, BB]
	columns     []string
	columnTypes []*sql.ColumnType
}

type batchReader struct {
	*reader
	batchsize int
	orderby   string
	pos       int
}
