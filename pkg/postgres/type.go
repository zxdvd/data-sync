package postgres

import (
	"context"
	"database/sql"

	_ "github.com/lib/pq"
)

type conn struct {
	uri string
	DB  *sql.DB
}

func (c *conn) Open() error {
	db, err := sql.Open("postgres", c.uri)
	c.DB = db
	return err
}

type reader struct {
	*conn
	tablename string
	ctx       context.Context
	// select a as A1, b as BB
	colAsMap map[string]string
	// [id, A1, BB]
	columns []string
}

type column struct {
	name string
	typ  string
}

type writer struct {
	*conn
	tablename    string
	ctx          context.Context
	columns      []column
	pks          []string
	createOption string
}

type batchReader struct {
	*reader
	batchsize int
	orderby   string
	pos       int
}
