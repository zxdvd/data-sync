package conf

import (
	"fmt"
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

type DBHost struct {
	Dialect  string
	Uri      string
	Readonly bool
}

type ColumnOptions struct {
	Selectall bool
	Columns   map[string]string
	Excludes  []string
}

type CreateTableOptions struct {
	Create      bool
	DropExisted bool
	DropCascade bool
	PKs         []string
}

type SyncTask struct {
	Name         string
	Sourcetable  string
	Sourcedbname string  `yaml:"sourcedb"`
	SourceDB     *DBHost `yaml:"-"`
	Targettable  string
	Targetdbname string  `yaml:"targetdb"`
	TargetDB     *DBHost `yaml:"-"`
	ColumnOptions
	CreateTableOptions
	Batchsize int
	Orderby   string
}

type Conf struct {
	DBHosts   map[string]*DBHost   `yaml:"databases"`
	SyncTasks map[string]*SyncTask `yaml:"tasks"`
}

func LoadConf(filepath string) (*Conf, error) {
	content, err := ioutil.ReadFile(filepath)
	if err != nil {
		return nil, err
	}
	var conf Conf
	err = yaml.Unmarshal(content, &conf)
	if err != nil {
		return nil, err
	}
	for k, _ := range conf.SyncTasks {
		task := conf.SyncTasks[k]
		if host, ok := conf.DBHosts[task.Sourcedbname]; ok {
			task.SourceDB = host
		} else {
			return nil, fmt.Errorf("sourcedb %s not found", task.Sourcedbname)
		}
		if host, ok := conf.DBHosts[task.Targetdbname]; ok {
			if host.Readonly {
				return nil, fmt.Errorf("should not set readonly host %s as targetdb", task.Targetdbname)
			}
			task.TargetDB = host
		} else {
			return nil, fmt.Errorf("targetdb %s not found", task.Targetdbname)
		}
	}
	return &conf, err
}
