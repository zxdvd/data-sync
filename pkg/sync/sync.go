package sync

import (
	"errors"

	"github.com/zxdvd/data-sync/pkg/common"
	"github.com/zxdvd/data-sync/pkg/conf"
	pg "github.com/zxdvd/data-sync/pkg/postgres"
)

type bulkSync struct {
	reader         common.BulkReader
	writer         common.BulkWriter
	columns        []string
	curPos         int
	batchsize      int
	orderby        string
	columnOpt      conf.ColumnOptions
	createTableOpt conf.CreateTableOptions
}

func NewReader(dialect, uri, tablename string) common.Reader {
	if dialect == "postgres" {
		return pg.NewReader(uri, tablename)
	}
	return nil
}

func NewWriter(dialect, uri, tablename string, pks []string) common.Writer {
	return NewBulkWriter(dialect, uri, tablename, pks)
}

func NewBulkReader(dialect, uri, tablename string, batchsize int, orderby string) common.BulkReader {
	switch dialect {
	case "postgres":
		return pg.NewBatchReader(pg.NewReader(uri, tablename), batchsize, orderby)
	default:
		return nil
	}
}

func NewBulkWriter(dialect, uri, tablename string, pks []string) common.BulkWriter {
	if dialect == "postgres" {
		return pg.NewWriter(uri, tablename, pks)
	}
	return nil
}

func SetupColumnOptions(r common.Reader, opt conf.ColumnOptions) ([]common.Column, error) {
	columnNames := make([]string, 0)
	if opt.Selectall {
		allcolumns, err := r.AllColumns()
		if err != nil {
			return nil, err
		}
	outer:
		for _, col := range allcolumns {
			name := col.Name()
			// skip if in opt.Columns, we'll add it later
			if _, ok := opt.Columns[name]; ok {
				continue outer
			}
			// deal with excluded columns
			for _, exclude := range opt.Excludes {
				if name == exclude {
					continue outer
				}
			}
			columnNames = append(columnNames, r.Quote(name))
		}
	}
	for colname, selectAs := range opt.Columns {
		col := selectAs + " AS " + r.Quote(colname)
		columnNames = append(columnNames, col)
	}
	if len(columnNames) == 0 {
		return nil, errors.New("must specify at least one column")
	}

	r.SetSelectColumns(columnNames)
	return r.ColumnTypes()
}

func SetupTargetTable(w common.Writer, opt conf.CreateTableOptions, columns []common.Column) error {
	if !opt.Create {
		// do not create new table, so return directly
		return nil
	}
	existed, err := w.Exists()
	if err != nil {
		return err
	}
	if existed {
		if !opt.DropExisted {
			// do not drop table
			return nil
		}
		// need to drop table
		err := w.DropTable(opt.DropCascade)
		if err != nil {
			return err
		}
	}
	w.SetColumnTypes(columns)
	err = w.CreateTable()
	return err
}

func NewSync(task *conf.SyncTask) *bulkSync {
	sourcedb := task.SourceDB
	reader := NewBulkReader(sourcedb.Dialect, sourcedb.Uri, task.Sourcetable, task.Batchsize, task.Orderby)
	targetdb := task.TargetDB
	writer := NewBulkWriter(targetdb.Dialect, targetdb.Uri, task.Targettable, task.CreateTableOptions.PKs)
	return &bulkSync{
		reader:         reader,
		writer:         writer,
		batchsize:      task.Batchsize,
		orderby:        task.Orderby,
		columnOpt:      task.ColumnOptions,
		createTableOpt: task.CreateTableOptions}
}

func (s *bulkSync) Setup() error {
	columns, err := SetupColumnOptions(s.reader, s.columnOpt)
	if err != nil {
		return err
	}
	err = SetupTargetTable(s.writer, s.createTableOpt, columns)
	return err
}

func (s *bulkSync) BulkSyncData() error {
	for {
		rows, err := s.reader.BulkRead()
		if err != nil {
			return err
		}
		// no data
		if len(rows) == 0 {
			break
		}
		err = s.writer.BulkInsert(rows)
		if err != nil {
			return err
		}
		s.curPos += len(rows)
	}
	return nil
}
