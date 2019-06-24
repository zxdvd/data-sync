package main

import (
	"flag"
	"log"
	"sync"

	"github.com/zxdvd/data-sync/pkg/conf"
	dbsync "github.com/zxdvd/data-sync/pkg/sync"
)

func synctask(t *conf.SyncTask) error {
	bulkSync, err := dbsync.NewSync(t)
	if err != nil {
		return err
	}
	err = bulkSync.Setup()
	if err != nil {
		return err
	}
	err = bulkSync.BulkSyncData()
	if err != nil {
		return err
	}
	return nil
}

func main() {
	configfile := flag.String("config", "", "config file path")
	flag.Parse()
	if *configfile == "" {
		log.Fatal("must specify a config file")
	}
	config, err := conf.LoadConf(*configfile)
	if err != nil {
		log.Fatal(err)
	}
	if len(config.SyncTasks) == 0 {
		log.Fatal("no tasks")
	}
	var wg sync.WaitGroup
	for _, task := range config.SyncTasks {
		wg.Add(1)
		go func(t *conf.SyncTask) {
			defer wg.Done()
			err := synctask(t)
			if err != nil {
				log.Fatal(err)
			}
		}(task)
	}
	wg.Wait()
	log.Println("all task finished.")
}
