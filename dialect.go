package gouin

import (
	"database/sql"
)

// borrowed from otira https://github.com/gnewton/otira/blob/master/dialect.go
type Dialect interface {
	InsertPreparedStatementSql(table string, fields []*Field) (string, error)
	DeleteByPKPreparedStatementSql(table string, pk string, needsQuotes bool) (string, error)
	SelectOneRecordByPKPreparedStatementSql(table string, fields []*Field, pk string, needsQuotes bool) (string, error)
	CreateTableSql(table string, fields []*Field, pk string) (string, error)
	SetEnforceForeignKeys(bool)
	GetEnforceForeignKeys() bool

	SetPragmas([]string)
	GetPragmas() []string

	OpenDB(dataSourceName string) (*sql.DB, error)
}
