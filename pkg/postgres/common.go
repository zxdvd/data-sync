package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/zxdvd/data-sync/pkg/common"
	utilsql "github.com/zxdvd/data-sync/pkg/utils/sql"
)

const dialect = "postgres"

func (c *conn) Dialect() string {
	return dialect
}

func (c *conn) Open() error {
	db, err := sql.Open(dialect, c.uri)
	c.db = db
	return err
}

func (c *conn) Quote(relation string) string {
	return "\"" + relation + "\""
}

func (c *conn) DB() *sql.DB {
	return c.db
}

var _ common.Conn = &conn{}

func (col column) Type() string {
	return strings.ToLower(col.DatabaseTypeName())
}

func (col column) ToSTDType() string {
	switch col.Type() {
	case "varchar", "text":
		return "text"
	case "int4", "int", "integer":
		return "integer"
	default:
		return "text"
	}
}

func (col column) TypeFromSTD(std string) string {
	switch std {
	case "text":
		return "text"
	case "integer":
		return "integer"
	default:
		return "text"
	}
}

var _ common.Column = column{}

func (t *table) GetSQLCreateTable() string {
	var s strings.Builder
	fmt.Fprintf(&s, "CREATE TABLE %s (", t.Quote(t.tablename))

	lines := make([]string, len(t.columns)+1)
	for i, col := range t.columns {
		lines[i] = t.Quote(col.Name()) + col.DatabaseTypeName()
	}
	// deal with PRIMARY KEY (a,b)
	if len(t.pks) > 0 {
		pks_ := make([]string, len(t.pks))
		for i, pk := range t.pks {
			pks_[i] = t.Quote(pk.Name())
		}
		pkline := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pks_, ","))
		lines = append(lines, pkline)
	}
	fmt.Fprintf(&s, "%s)", strings.Join(lines, ","))
	return s.String()
}

func (t *table) CreateTable() error {
	q := t.GetSQLCreateTable()
	_, _, err := utilsql.Query(t.DB(), q)
	return err

}
