package sql

import (
	"database/sql"
)

func Query(db *sql.DB, sql string, args ...interface{}) ([]*sql.ColumnType, [][]interface{}, error) {
	rows, err := db.Query(sql, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	var result [][]interface{}
	colVals := make([]interface{}, len(cols))
	colPointers := make([]interface{}, len(cols))
	for rows.Next() {
		for i, _ := range colVals {
			colPointers[i] = &colVals[i]
		}
		if err := rows.Scan(colPointers...); err != nil {
			return nil, nil, err
		}
		// need to copy colVals
		result = append(result, append([]interface{}(nil), colVals...))
	}
	return cols, result, nil
}
