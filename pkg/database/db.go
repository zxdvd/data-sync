package database

import (
	"github.com/zxdvd/data-sync/pkg/database/common"
	mysql "github.com/zxdvd/data-sync/pkg/database/mysql"
	pg "github.com/zxdvd/data-sync/pkg/database/postgres"
)

func NewBulkReader(dialect, uri, tablename string, batchsize int, orderby string) common.BulkReader {
	switch dialect {
	case "postgres":
		return pg.NewBatchReader(pg.NewReader(uri, tablename), batchsize, orderby)
	case "mysql":
		return mysql.NewBatchReader(mysql.NewReader(uri, tablename), batchsize, orderby)
	default:
		return nil
	}
}

func NewBulkWriter(dialect, uri, tablename string, pks []string) common.BulkWriter {
	switch dialect {
	case "postgres":
		return pg.NewWriter(uri, tablename, pks)
	case "mysql":
		return mysql.NewWriter(uri, tablename, pks)
	default:
		return nil

	}
}
