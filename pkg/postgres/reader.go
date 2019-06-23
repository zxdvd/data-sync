package postgres

import (
	"fmt"
	"strings"

	"github.com/zxdvd/data-sync/pkg/common"
	"github.com/zxdvd/data-sync/pkg/utils/sql"
)

func NewReader(uri string, tablename string) *reader {
	conn_ := &conn{uri: uri}
	return &reader{
		table: table{conn: conn_, tablename: tablename},
	}
}

func NewBatchReader(r *reader, size int, orderby string) *batchReader {
	return &batchReader{
		reader:    r,
		batchsize: size,
		orderby:   orderby,
	}
}

func (r *reader) SetSelectColumns(columns []string) {
	r.columns = columns
}

func (r *reader) getSelectSql() string {
	q := fmt.Sprintf(`SELECT %s FROM "%s" `,
		// no need to quote here, since there maybe raw sql functions
		strings.Join(r.columns, `,`),
		r.tablename)
	return q
}

func (r *reader) ColumnTypes() ([]common.Column, error) {
	q := r.getSelectSql() + " WHERE 1=0"
	colTypes, _, err := sql.Query(r.DB(), q)
	r.columnTypes = colTypes
	cols := convertColumnTypes(colTypes)
	return cols, err
}

var _ common.Reader = &reader{}

func (br *batchReader) SetPosition(pos int) {
	br.pos = pos
}

func (br *batchReader) BulkRead() ([][]interface{}, error) {
	q := br.getSelectSql() + fmt.Sprintf("ORDER BY %s OFFSET %d LIMIT %d", br.orderby, br.pos, br.batchsize)
	// TODO query result
	_, rows, err := sql.Query(br.DB(), q)
	if err != nil {
		return nil, err
	}
	br.pos += len(rows)
	return rows, nil
}

var _ common.BulkReader = &batchReader{}
