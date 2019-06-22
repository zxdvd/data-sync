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

func (w *writer) BulkInsert(rows [][]interface{}) error {
	for _, row := range rows {
		if err := w.Insert(row); err != nil {
			return err
		}
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
