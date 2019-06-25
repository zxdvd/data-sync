package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/zxdvd/data-sync/pkg/database/common"
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

type column struct {
	*sql.ColumnType
	name string
	typ  string
}

func (col column) Dialect() string {
	return dialect
}

func (col column) Type() string {
	if col.typ != "" {
		return col.typ
	}
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

func (t *table) Exists() (bool, error) {
	q := fmt.Sprintf("SELECT to_regclass('%s')", t.Quote(t.tablename))
	var relationName sql.NullString
	err := t.DB().QueryRow(q).Scan(&relationName)
	if err != nil {
		return false, err
	}
	if relationName.Valid {
		return len(relationName.String) > 0, nil
	}
	return false, nil
}

func (t *table) DropTable(cascade bool) error {
	q := fmt.Sprintf("DROP TABLE %s ", t.Quote(t.tablename))
	if cascade {
		q += " CASCADE"
	}
	_, err := t.DB().Exec(q)
	return err
}

func (t *table) AllColumns() ([]common.Column, error) {
	q := fmt.Sprintf("SELECT * FROM %s WHERE 1=0", t.Quote(t.tablename))
	colTypes, _, err := utilsql.Query(t.DB(), q)
	return convertColumnTypes(colTypes), err
}

func (t *table) GetSQLCreateTable() string {
	var s strings.Builder
	fmt.Fprintf(&s, "CREATE TABLE %s (", t.Quote(t.tablename))

	lines := make([]string, len(t.columns))
	for i, col := range t.columns {
		lines[i] = t.Quote(col.Name()) + " " + col.Type()
	}
	// deal with PRIMARY KEY (a,b)
	if len(t.pks) > 0 {
		pkline := fmt.Sprintf("PRIMARY KEY (%s)", QuoteAndJoin(t.pks))
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

func (t *table) SetColumnTypes(colTypes []common.Column) {
	columns := make([]column, len(colTypes))
	for i, colType := range colTypes {
		pgColumn := columns[i]
		if colType.Dialect() == pgColumn.Dialect() {
			columns[i] = colType.(column)
		} else {
			stdType := colType.ToSTDType()
			pgColumn.name = colType.Name()
			pgColumn.typ = pgColumn.TypeFromSTD(stdType)
			columns[i] = pgColumn
		}
	}
	t.columns = columns
}

func QuoteAndJoin(cols []string) string {
	return "\"" + strings.Join(cols, ",") + "\""
}

func convertColumnTypes(cols []*sql.ColumnType) []common.Column {
	columns := make([]common.Column, len(cols))
	for i, colType := range cols {
		columns[i] = column{ColumnType: colType}
	}
	return columns

}
