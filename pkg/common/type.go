package common

type Conn interface {
	Dialect() string
	Open() error
	Quote(string) string
}

type Reader interface {
	Table
	SetSelectColumns([]string)
	ColumnTypes() ([]Column, error)
}

type BulkReader interface {
	Reader
	BulkRead() ([][]interface{}, error)
}

type Writer interface {
	Table
	SetColumnNames([]string)
	SetColumnTypes([]Column)
	Insert(rows []interface{}) error
}

type BulkWriter interface {
	Writer
	BulkInsert(rows [][]interface{}) error
}

type Column interface {
	Dialect() string
	Type() string
	Name() string
	ToSTDType() string
	TypeFromSTD(std string) string
}

type Table interface {
	Conn
	Exists() (bool, error)
	CreateTable() error
	DropTable(cascade bool) error
	AllColumns() ([]Column, error)
}
