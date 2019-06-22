package postgres

import (
	"fmt"
	"strings"

	"github.com/zxdvd/data-sync/pkg/common"
)

func NewReader(uri string, tablename string, colsAsMap map[string]string) *reader {
	conn_ := &conn{uri: uri}
	return &reader{
		conn:      conn_,
		tablename: tablename,
		colAsMap:  colsAsMap,
	}
}

func NewBatchReader(r *reader, size int, orderby string) *batchReader {
	return &batchReader{
		reader:    r,
		batchsize: size,
		orderby:   orderby,
	}
}

func (r *reader) SetColumns(columns []string) {
	cols := make([]string, len(columns), len(columns))
	for i, col := range columns {
		if val, ok := r.colAsMap[col]; ok {
			cols[i] = fmt.Sprintf(`%s AS "%s"`, val, col)
		} else {
			cols[i] = col
		}
	}
	r.columns = cols
}

func (br *batchReader) SetPosition(pos int) {
	br.pos = pos
}

func (br *batchReader) Read() ([][]interface{}, error) {
	q := fmt.Sprintf(`SELECT "%s" FROM "%s" ORDER BY %s offset %d limit %d`,
		strings.Join(br.columns, `","`),
		br.tablename,
		br.orderby,
		br.pos, br.batchsize)
	// TODO query result
	fmt.Println(q)
	rows, err := br.conn.DB.Query(q)
	if err != nil {
		return nil, err
	}
	results := make([][]interface{}, 0)
	for rows.Next() {
		vals := make([]interface{}, len(br.columns))
		valpoints := make([]interface{}, len(br.columns))
		for i, _ := range vals {
			valpoints[i] = &vals[i]
		}
		if err := rows.Scan(valpoints...); err != nil {
			return nil, err
		}
		results = append(results, vals)
	}
	return results, nil
}

var _ common.BatchReader = &batchReader{}
