package main

import (
	"flag"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/zxdvd/data-sync/pkg/conf"
	dbsync "github.com/zxdvd/data-sync/pkg/sync"
	"go.uber.org/zap"
)

var logger *zap.Logger

func init() {
	var err error
	logger, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
}

func main() {
	configfile := flag.String("config", "", "config file path")
	taskname := flag.String("task", "", "run a specific task")
	flag.Parse()
	if *configfile == "" {
		logger.Fatal("must specify a config file")
	}
	config, err := conf.LoadConf(*configfile)
	if err != nil {
		logger.Fatal("parse config error", zap.Error(err))
	}
	if len(config.SyncTasks) == 0 {
		logger.Fatal("no tasks")
	}
	var wg sync.WaitGroup
	for name, task := range config.SyncTasks {
		if task.Disabled {
			logger.Info("skip disabled task " + name)
			continue
		}
		if *taskname != "" && *taskname != name {
			continue
		}
		wg.Add(1)
		go func(t *conf.SyncTask) {
			defer wg.Done()
			bulkSync, err := dbsync.NewSync(t)
			if err != nil {
				panic(errors.Wrap(err, "failed to NewSync"))
			}
			if err := bulkSync.Sync(); err != nil {
				panic(errors.Wrap(err, "failed to sync"))
			}
		}(task)
	}
	wg.Wait()
	logger.Info("all task finished.")
}
