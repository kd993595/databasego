package internal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
)

type Backend struct {
	dir      string
	mainFile *os.File
	tables   []Table
}

func CreateNewDatabase(dir string) *Backend {
	buf := make([]byte, 100) //reserves first hundred bytes of main file for header
	headername := []byte("Fusedb format 1\x00")
	copy(buf[0:16], headername)
	binary.LittleEndian.PutUint16(buf[16:18], uint16(PAGESIZE))

	f, err := os.Create(filepath.Join(dir, "main.db"))
	if err != nil {
		panic("unexpected error creating new main database file")
	}
	defer f.Close()
	_, err = f.Write(buf)
	if err != nil {
		panic(err)
	}
	return &Backend{dir: dir, mainFile: f, tables: make([]Table, 0)}
}

func OpenExistingDatabase(dir string) (*Backend, error) {
	b := Backend{dir: dir}

	f, err := os.Open(filepath.Join(dir, "main.db"))
	if os.IsNotExist(err) {
		return nil, errors.New("database directory was tampered with, main file gone")
	} else if err != nil {
		return nil, err
	}
	defer f.Close()
	b.mainFile = f

	headerBuf := make([]byte, 100)
	_, err = f.Read(headerBuf)
	if err != nil {
		return nil, err
	}

	if string(headerBuf[0:16]) != "Fusedb format 1\x00" {
		return nil, errors.New("database file tampered with unrecognized version")
	}
	f.Seek(100, 0)
	tablePage := make([]byte, PAGESIZE)
	_, err = f.Read(tablePage)
	if err == io.EOF {
		return &b, nil //means no tables made
	}
	if err != nil {
		return nil, err
	}

	b.tables = make([]Table, 0)
	byteIndex := 0
	for byteIndex < len(tablePage) {
		tableSize := binary.LittleEndian.Uint32(tablePage[byteIndex : byteIndex+4])
		if tableSize == 0 {
			break
		}
		byteIndex += 4
		tmpTable := fromBytes(tablePage[byteIndex : byteIndex+int(tableSize)])
		byteIndex += int(tableSize)
		b.tables = append(b.tables, tmpTable)
	}

	for _, tab := range b.tables { //debugging table info
		fmt.Println(tab.ToString())
	}

	return &b, nil
}

func (b *Backend) CreateTable(q Query) error {
	exists := b.checkTableExist(q)
	if exists {
		return errors.New("Table name already exists")
	}
	newtable := Table{lastRowId: 0}
	newtable.Name = q.TableName
	newtable.Columns = make([]Column, len(q.TableConstruction))

	for i, construct := range q.TableConstruction {
		newColumn := Column{}
		newColumn.columnName = construct[0]
		switch construct[1] { //Uses reserved types list in parser.go
		case "INT":
			newColumn.columnType = INT
			newColumn.columnSize = 8 //(bytes)
			if len(construct) <= 2 {
				break
			}
			isPrimary := false
			for _, val := range construct[2:] {
				if !isConstraint(val) {
					return fmt.Errorf("CREATE: unsupported token in create field for column: %s", newColumn.columnName)
				}
				if val == "PRIMARY KEY" {
					newColumn.columnConstraint = COL_PRIMARY
					isPrimary = true
				}
			}
			if isPrimary {
				break
			}
			if len(construct) == 4 {
				newColumn.columnConstraint = COL_NOTNULLUNIQUE
			} else if construct[3] == "UNIQUE" {
				newColumn.columnConstraint = COL_UNIQUE
			} else if construct[3] == "NOT NULL" {
				newColumn.columnConstraint = COL_NOTNULL
			}
		case "FLOAT":
			newColumn.columnType = FLOAT
			newColumn.columnSize = 8 //(bytes)
			if len(construct) <= 2 {
				break
			}
			isPrimary := false
			for _, val := range construct[2:] {
				if !isConstraint(val) {
					return fmt.Errorf("CREATE: unsupported token in create field for column: %s", newColumn.columnName)
				}
				if val == "PRIMARY KEY" {
					newColumn.columnConstraint = COL_PRIMARY
					isPrimary = true
				}
			}
			if isPrimary {
				break
			}
			if len(construct) == 4 {
				newColumn.columnConstraint = COL_NOTNULLUNIQUE
			} else if construct[3] == "UNIQUE" {
				newColumn.columnConstraint = COL_UNIQUE
			} else if construct[3] == "NOT NULL" {
				newColumn.columnConstraint = COL_NOTNULL
			}
		case "BOOL":
			newColumn.columnType = BOOL
			newColumn.columnSize = 1 //(bytes)
			if len(construct) <= 2 {
				break
			}
			for _, val := range construct[2:] {
				if !isConstraint(val) {
					return fmt.Errorf("CREATE: unsupported token in create field for column: %s", newColumn.columnName)
				}
				if val == "PRIMARY KEY" {
					return errors.New("CREATE: cannot make primary field on BOOL column")
				}
				if val == "UNIQUE" {
					return errors.New("CREATE: cannot make unique field on BOOL column")
				}
			}
			if construct[2] == "NOT NULL" {
				newColumn.columnConstraint = COL_NOTNULL
			}

		case "CHAR":
			newColumn.columnType = CHAR
			if len(construct) < 3 {
				return errors.New("size needed for char field in table")
			}
			fieldSize, err := strconv.Atoi(construct[2])
			if err != nil {
				return errors.Join(err, errors.New("error in table construction of size of CHAR field"))
			}
			if !(fieldSize >= 1 && fieldSize <= 255) {
				return errors.New("size for char field must be between 1 and 255")
			}
			newColumn.columnSize = byte(fieldSize)
			if len(construct) <= 3 {
				break
			}
			isPrimary := false
			for _, val := range construct[3:] {
				if !isConstraint(val) {
					return fmt.Errorf("CREATE: unsupported token in create field for column: %s", newColumn.columnName)
				}
				if val == "PRIMARY KEY" {
					newColumn.columnConstraint = COL_PRIMARY
					isPrimary = true
				}
			}
			if isPrimary {
				break
			}
			if len(construct) == 4 {
				newColumn.columnConstraint = COL_NOTNULLUNIQUE
			} else if construct[3] == "UNIQUE" {
				newColumn.columnConstraint = COL_UNIQUE
			} else if construct[3] == "NOT NULL" {
				newColumn.columnConstraint = COL_NOTNULL
			}
		default:
			return errors.ErrUnsupported
		}
		newtable.Columns[i] = newColumn
	}

	primary := 0
	for _, col := range newtable.Columns {
		if col.columnConstraint == COL_PRIMARY {
			primary += 1
		}
		if col.columnConstraint == COL_PRIMARY && col.columnType == INT {
			col.columnConstraint = COL_ROWID
		}
	}
	if primary != 1 {
		return errors.New("CREATE: must have exactly one primary field")
	}

	_, err := os.Create(filepath.Join(b.dir, fmt.Sprintf("%s.db", newtable.Name)))
	if err != nil {
		return err
	}

	b.tables = append(b.tables, newtable)
	b.writeTablesToDisk()
	return nil
}

func (b *Backend) Insert(q Query) error { //use md5 for checksum
	exists := b.checkTableExist(q)
	if !exists {
		return errors.New("Table does not exist")
	}

	var tableToInsert Table
	for i := range b.tables {
		if q.TableName == b.tables[i].Name {
			tableToInsert = b.tables[i]
		}
	}

	allrows := make([][]byte, 0)

	for _, val := range q.Inserts {
		nullColumns := InitializeBitSet(uint64(len(tableToInsert.Columns)))
		rowInsert := make([]byte, tableToInsert.GenerateRowBytes()+nullColumns.Size())

		byteIndex := nullColumns.Size()
		columnsAdded := 0
		for j, col := range tableToInsert.Columns {
			previous := columnsAdded
			for i := 0; i < len(val); i++ {
				if col.columnName != val[i] {
					continue
				}
				var b []byte = make([]byte, 0)
				switch col.columnType {
				case INT:
					n, err := strconv.Atoi(val[i])
					if err != nil {
						return errors.Join(errors.New("Insert Query failed: "), err)
					}
					b = binary.LittleEndian.AppendUint64(b, uint64(n))
				case FLOAT:
					n, err := strconv.ParseFloat(val[i], 64)
					if err != nil {
						return errors.Join(errors.New("Insert Query failed: "), err)
					}
					b = binary.LittleEndian.AppendUint64(b, math.Float64bits(n))
				case BOOL:
					n, err := strconv.ParseBool(val[i])
					if err != nil {
						return errors.Join(errors.New("Insert Query failed: "), err)
					}
					if n {
						b = append(b, byte(1))
					} else {
						b = append(b, byte(1))
					}
				case CHAR:
					n := []byte(val[i])
					if len(n) > int(col.columnSize) {
						return errors.Join(errors.New("Insert Query failed: "), errors.New("string to insert larger than allowed"))
					}
					b = append(b, n...)
				}
				copy(rowInsert[byteIndex:byteIndex+uint64(col.columnSize)], b)
				columnsAdded += 1
			}
			if previous == columnsAdded {
				//no columns added
				if col.columnType == COL_ROWID {
					n := tableToInsert.lastRowId + 1
					b := make([]byte, 8)
					binary.LittleEndian.PutUint64(b, uint64(n))
					copy(rowInsert[byteIndex:byteIndex+uint64(col.columnSize)], b)
				} else {
					nullColumns.setBit(j)
				}
			}
			byteIndex += uint64(col.columnSize)
		}
		copy(rowInsert[0:], nullColumns.bytes)
		if columnsAdded != len(val) {
			return errors.New("At Insert: columns given some may not exist failed insert")
		}
		allrows = append(allrows, rowInsert)
	}

	return nil
}

func (b *Backend) checkTableExist(q Query) bool {
	for i := range b.tables {
		if q.TableName == b.tables[i].Name {
			return true
		}
	}
	return false
}

func (b *Backend) writeTablesToDisk() {
	buf := make([]byte, PAGESIZE)
	byteIndex := 0
	for _, table := range b.tables {
		tmpBuf := table.toBytes()
		binary.LittleEndian.PutUint32(buf[byteIndex:byteIndex+4], uint32(len(tmpBuf)))
		byteIndex += 4
		copy(buf[byteIndex:], tmpBuf)
		byteIndex += len(tmpBuf)
	}

	f, err := os.OpenFile(b.mainFile.Name(), os.O_WRONLY, 0700)
	if err != nil {
		panic(err)
	}
	_, err = f.WriteAt(buf, 100)
	if err != nil {
		panic(err)
	}
}
