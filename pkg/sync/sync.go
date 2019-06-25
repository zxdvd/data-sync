package sync

import (
	"github.com/pkg/errors"

	"github.com/zxdvd/data-sync/pkg/conf"
	db "github.com/zxdvd/data-sync/pkg/database"
	"github.com/zxdvd/data-sync/pkg/database/common"
	"go.uber.org/zap"
)

var defaultlogger *zap.Logger

func init() {
	var err error
	defaultlogger, err = zap.NewProduction()
	if err != nil {
		panic(err)
	}

}

type bulkSync struct {
	reader         common.BulkReader
	writer         common.BulkWriter
	columns        []string
	curPos         int
	batchsize      int
	orderby        string
	columnOpt      conf.ColumnOptions
	createTableOpt conf.CreateTableOptions
	log            *zap.Logger
}

func SetupColumnOptions(r common.Reader, opt conf.ColumnOptions) ([]common.Column, error) {
	columnNames := make([]string, 0)
	if opt.Selectall {
		allcolumns, err := r.AllColumns()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get all columns")
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
		w.SetColumnTypes(columns)
		return nil
	}
	existed, err := w.Exists()
	if err != nil {
		return errors.Wrap(err, "error to check table exists")
	}
	if existed {
		if !opt.DropExisted {
			// you choose create table, but NOT drop existed, so CONFLICT
			return errors.New("create conflict with not drop existed")
		}
		// need to drop table
		err := w.DropTable(opt.DropCascade)
		if err != nil {
			return errors.Wrap(err, "error to drop table")
		}
	}
	w.SetColumnTypes(columns)
	err = w.CreateTable()
	return errors.Wrap(err, "failed to create table")
}

func NewSync(task *conf.SyncTask) (*bulkSync, error) {
	sourcedb := task.SourceDB
	if task.Batchsize <= 0 {
		return nil, errors.New("batchsize must great than 0")
	}
	if task.Orderby == "" {
		return nil, errors.New("must specify orderby of batch")
	}
	if sourcedb == nil {
		return nil, errors.New("sourcedb should not be empty")
	}
	reader := db.NewBulkReader(sourcedb.Dialect, sourcedb.Uri, task.Sourcetable, task.Batchsize, task.Orderby)
	targetdb := task.TargetDB
	if targetdb == nil {
		return nil, errors.New("targetdb should not be empty")
	}
	writer := db.NewBulkWriter(targetdb.Dialect, targetdb.Uri, task.Targettable, task.CreateTableOptions.PKs)

	return &bulkSync{
		reader:         reader,
		writer:         writer,
		batchsize:      task.Batchsize,
		orderby:        task.Orderby,
		columnOpt:      task.ColumnOptions,
		createTableOpt: task.CreateTableOptions,
		log:            defaultlogger,
	}, nil
}

func (s *bulkSync) Setup() error {
	err := s.reader.Open()
	if err != nil {
		return errors.Wrap(err, "failed to open reader")
	}
	err = s.writer.Open()
	if err != nil {
		return errors.Wrap(err, "failed to open writer")
	}
	columns, err := SetupColumnOptions(s.reader, s.columnOpt)
	if err != nil {
		return errors.Wrap(err, "failed to setup column options")
	}
	err = SetupTargetTable(s.writer, s.createTableOpt, columns)
	return errors.Wrap(err, "failed to setup target table")
}

func (s *bulkSync) BulkSyncData() error {
	s.log.Info("begin bulk sync data")
	for {
		rows, err := s.reader.BulkRead()
		s.log.Info("rows count: ", zap.Int("count", len(rows)))
		if err != nil {
			return errors.Wrap(err, "failed to BulkRead")
		}
		// no data
		if len(rows) == 0 {
			break
		}
		err = s.writer.BulkInsert(rows)
		if err != nil {
			return errors.Wrap(err, "failed to BulkInsert")
		}
		s.curPos += len(rows)
	}
	return nil
}

func (s *bulkSync) Sync() error {
	err := s.Setup()
	if err != nil {
		return errors.Wrap(err, "failed to setup")
	}
	err = s.BulkSyncData()
	if err != nil {
		return errors.Wrap(err, "failed to BulkSyncData")
	}
	return nil
}
