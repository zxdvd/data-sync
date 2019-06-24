package sync

import (
	"testing"

	"github.com/zxdvd/data-sync/pkg/conf"
)

func TestSync(t *testing.T) {
	pguri := "postgres://postgres@localhost:5432/test1?sslmode=disable"
	dbhost := conf.DBHost{
		Dialect: "postgres",
		Uri:     pguri,
	}
	task := conf.SyncTask{
		Name:        "task1",
		Sourcetable: "r1",
		SourceDB:    &dbhost,
		Targettable: "w1",
		TargetDB:    &dbhost,
		ColumnOptions: conf.ColumnOptions{
			Selectall: true,
		},
		CreateTableOptions: conf.CreateTableOptions{
			Create:      true,
			DropExisted: true,
			DropCascade: true,
			PKs:         []string{"id"},
		},
		Batchsize: 5,
		Orderby:   "id",
	}
	bulkSync, err := NewSync(&task)
	if err != nil {
		t.Fatal(err)
	}
	err = bulkSync.Setup()
	if err != nil {
		t.Fatal(err)
	}
	err = bulkSync.BulkSyncData()
	if err != nil {
		t.Fatal(err)
	}

}
