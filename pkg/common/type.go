package common

type Conn interface {
	Dialect() string
	Open() error
	Quote(string) string
}

type Reader interface {
	Dialect() string
	SetSelectColumns([]string)
	ColumnTypes() ([]Column, error)
}

type BulkReader interface {
	Reader
	BulkRead() ([][]interface{}, error)
}

type Writer interface {
	Dialect() string
	SetColumnNames([]string)
	SetColumnTypes([]Column)
	Insert(rows []interface{}) error
	CreateTable() error
}

type BulkWriter interface {
	Writer
	BulkInsert(rows [][]interface{}) error
}

type Column interface {
	Type() string
	Name() string
	ToSTDType() string
	TypeFromSTD(std string) string
}

type Table interface {
	Conn
	CreateTable() error
}
