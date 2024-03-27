package internal

const (
	COL_UNIQUE = iota + 1
	COL_NOTNULL
	COL_NOTNULLUNIQUE
	COL_PRIMARY
	COL_ROWID
)

type insertType uint8

const (
	COL_I_PRIMARYNULL insertType = iota
	COL_I_PRIMARYVALUED
	COL_I_VALUED
	COL_I_NULL
)

/*
ColumnType values are define in parser.go
*/
type Column struct {
	columnName       string
	columnType       uint8 //data type
	columnSize       uint8 //size of column in database in bytes
	columnConstraint uint8
	columnOffset     int
	columnIndex      int
}

type ResultColumn struct {
	Name       string
	ColumnType uint8
}

type InsertColumn struct {
	columnSize  uint8
	colType     insertType
	dataType    uint8
	insertIndex int
}
