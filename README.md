## data-sync
A tool to sync table between databases (postgres, mysql).

### Config
Write a simple yaml config about data source and target to sync tables.

demo tasks.yaml
``` yaml
databases:
  db1:
    dialect: postgres
    uri: postgres://postgres@localhost:5432/test1?sslmode=disable
  db2:
    dialect: postgres
    uri: postgres://postgres@localhost:5432/test1?sslmode=disable
    readonly: false

tasks:
  task1:
    sourcetable: r1
    sourcedb: db1
    targettable: w1
    targetdb: db2
    column_options:
      selectall: true
      # columns:
      #   id: id
      #   name1: name
      #   name2: name::text
      #   name3: substring(name, 1, 5)
    create_table_options:
      create: yes
      drop_existed: yes
      drop_cascade: yes
      pks:
        - id
    batchsize: 5
    orderby: id
```

### Run
  go run cmd/main.go --config TASKS.yml

### TODO
* better log
* incremental sync
