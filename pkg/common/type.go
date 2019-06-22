package common

type BatchReader interface {
	Read() ([][]interface{}, error)
}

type Writer interface {
	BulkInsert(rows [][]interface{}) error
	Insert(rows []interface{}) error
}
