package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/zxdvd/data-sync/pkg/common"
)

func NewWriter(uri string, tablename string, columns []map[string]string, pks []string) *writer {
	columns_ := make([]column, len(columns))
	for i, col := range columns {
		columns_[i] = column{
			name: col["name"],
			typ:  col["type"],
		}
	}
	conn_ := &conn{uri: uri}
	return &writer{
		conn:      conn_,
		tablename: tablename,
		columns:   columns_,
		pks:       pks,
	}
}

func getColumnStr(columns []string) string {
	return "\"" + strings.Join(columns, `","`) + "\""
}

func (w *writer) BulkInsert(rows [][]interface{}) error {
	var q strings.Builder
	fmt.Fprintf(&q, "INSERT INTO %s (%s) VALUES", w.tablename, getColumnStr(w.GetColumns()))

	pos := 0
	var expandedRows []interface{}
	for i := 0; i < len(rows); i++ {
		expandedRows = append(expandedRows, rows[i]...)
		if i != 0 {
			q.WriteString(",")
		}
		row := rows[i]
		params := make([]string, len(row))
		for j := 0; j < len(row); j++ {
			pos++
			params[j] = "$" + strconv.Itoa(pos)
		}
		fmt.Fprintf(&q, "(%s)", strings.Join(params, ","))

	}
	var result interface{}
	err := w.conn.DB.QueryRow(q.String(), expandedRows...).Scan(&result)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	return nil
}

func (w *writer) Insert(row []interface{}) error {
	cols := w.GetColumns()
	positionParams := make([]string, len(cols), len(cols))
	for i, _ := range cols {
		positionParams[i] = "$" + strconv.Itoa(i+1)
	}
	q := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`,
		w.tablename, strings.Join(cols, ","),
		strings.Join(positionParams, ","))
	log.Println(q)
	// q = "insert abc"
	var result interface{}
	err := w.conn.DB.QueryRow(q, row...).Scan(&result)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	log.Println("result", result)
	return nil
}

func (w *writer) GetColumns() []string {
	cols := make([]string, len(w.columns))
	for i, col := range w.columns {
		cols[i] = col.name
	}
	return cols
}

var _ common.Writer = &writer{}
