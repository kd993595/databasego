package internal

import (
	"encoding/binary"
	"fmt"
	"strings"
)

/*
TableName must be checked to be within 2 bytes range
ColumnNames must have length within 1 byte range
Columns slice must preserve order
*/
type Table struct {
	Columns       []Column
	Name          string
	lastRowId     int64
	rowEmptyBytes uint64
	lastPage      uint64
}

/*
ColumnType values are define in parser.go
*/
type Column struct {
	columnName       string
	columnType       uint8
	columnSize       uint8
	columnConstraint uint8
}

type Cell []byte

const (
	COL_UNIQUE = iota + 1
	COL_NOTNULL
	COL_NOTNULLUNIQUE
	COL_PRIMARY
	COL_ROWID
)

func (t *Table) toBytes() []byte {
	buf := make([]byte, 0)
	//table name put into bytes buffer
	nameInBytes := []byte(t.Name)
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(nameInBytes)))
	buf = append(buf, nameInBytes...)
	//rowid 8 bytes put into bytes buffer
	buf = binary.LittleEndian.AppendUint64(buf, uint64(t.lastRowId))
	//columns put into bytes buffer
	for i := 0; i < len(t.Columns); i++ {
		buf = append(buf, t.Columns[i].columnConstraint)  //one byte for type of column it is
		buf = append(buf, uint8(t.Columns[i].columnType)) //will be one byte since values between 0-255
		buf = append(buf, t.Columns[i].columnSize)        //one byte for column size field
		tempbytes := []byte(t.Columns[i].columnName)      //column name converted to bytes
		buf = append(buf, byte(len(tempbytes)))           //1 byte for column name length
		buf = append(buf, tempbytes...)                   //column name in bytes appended
	}
	return buf
}

func fromBytes(buf []byte) Table {
	t := Table{}
	tableNameSize := binary.LittleEndian.Uint16(buf[0:2])
	t.Name = string(buf[2 : 2+tableNameSize])
	byteIndex := int(2 + tableNameSize)
	t.lastRowId = int64(binary.LittleEndian.Uint64(buf[byteIndex : byteIndex+8]))
	byteIndex += 8
	t.Columns = make([]Column, 0)

	for byteIndex < len(buf) {
		newColumn := Column{}
		newColumn.columnConstraint = buf[byteIndex]
		byteIndex += 1
		newColumn.columnType = buf[byteIndex]
		byteIndex += 1

		columnSize := buf[byteIndex] //size of column in database in bytes
		byteIndex += 1

		columnNameSize := buf[byteIndex]
		byteIndex += 1

		columnName := string(buf[byteIndex : byteIndex+int(columnNameSize)])
		byteIndex += int(columnNameSize)

		newColumn.columnName = columnName
		newColumn.columnSize = columnSize
		t.Columns = append(t.Columns, newColumn)
	}
	return t
}

func (t *Table) GenerateRowBytes() uint64 {
	if t.rowEmptyBytes == 0 {
		bytelength := 0
		for _, val := range t.Columns {
			bytelength += int(val.columnSize)
		}
		t.rowEmptyBytes = uint64(bytelength)
	}
	return t.rowEmptyBytes
}

func (t *Table) ToString() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Name: %s\n", t.Name))
	for _, col := range t.Columns {
		sb.WriteString(fmt.Sprintf("Column: %s\n", col.columnName))
		sb.WriteString(fmt.Sprintf("ColumnType: %d\n", col.columnType))
		sb.WriteString(fmt.Sprintf("ColumnSize: %d\n", col.columnSize))
		sb.WriteString(fmt.Sprintf("ColumnConstraint: %d\n", col.columnConstraint))
	}
	return sb.String()
}
