package internal

import (
	"bytes"
	"crypto/md5"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Backend struct {
	dir        string
	mainFile   *os.File
	tables     []Table
	bufferPool *BufferPoolManager
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
	return &Backend{dir: dir, mainFile: f, tables: make([]Table, 0), bufferPool: NewBufferPool(dir)}
}

func OpenExistingDatabase(dir string) (*Backend, error) {
	b := Backend{dir: dir, bufferPool: NewBufferPool(dir)}

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
		tmpTable.GenerateFields()
		byteIndex += int(tableSize)
		b.tables = append(b.tables, tmpTable)
	}

	for i, tab := range b.tables {
		n, m, err := b.GetTableParams(tab)
		if err != nil {
			return nil, err
		}
		b.tables[i].lastPage = n
		b.tables[i].lastRowId = m
		b.bufferPool.NewPool(tab.Name, b.dir)
	}

	return &b, nil
}

func (b *Backend) GetTableParams(table Table) (uint64, int64, error) {
	tabledir := filepath.Join(b.dir, fmt.Sprintf("%s.db", table.Name))
	f, err := os.Open(tabledir)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	lastPage := fi.Size()/PAGESIZE - 1
	f.Seek(lastPage*PAGESIZE, 0)
	buf := [PAGESIZE]byte{}
	_, err = f.Read(buf[:])
	if err != nil {
		return 0, 0, err
	}
	rowNums := binary.LittleEndian.Uint16(buf[8:10])
	offset := 26 + rowNums*uint16(table.GenerateRowBytes())
	columnBits := InitializeBitSet(uint64(len(table.Columns)))
	offset += uint16(columnBits.Size())
	for _, col := range table.Columns {
		if col.columnConstraint == COL_ROWID {
			break
		} else {
			offset += uint16(col.columnSize)
		}
	}
	rowid := binary.LittleEndian.Uint64(buf[offset : offset+8])
	return uint64(lastPage), int64(rowid), nil
}

func (b *Backend) CreateTable(q Query) error { //rewrite
	_, exists := b.checkTableExist(q)
	if exists {
		return errors.New("Table already exist")
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

	f, err := os.Create(filepath.Join(b.dir, fmt.Sprintf("%s.db", newtable.Name)))
	if err != nil {
		return err
	}

	buf := [PAGESIZE]byte{}
	binary.LittleEndian.PutUint64(buf[0:8], 0)  //pagenum
	binary.LittleEndian.PutUint16(buf[8:10], 0) //rownums
	checksum := md5.Sum(buf[26:])
	copy(buf[10:26], checksum[:])
	_, err = f.Write(buf[:])
	if err != nil {
		return err
	}

	newtable.lastPage = 0
	newtable.lastRowId = 0
	newtable.GenerateFields()
	b.tables = append(b.tables, newtable)
	b.writeTablesToDisk()
	b.bufferPool.NewPool(newtable.Name, b.dir)
	return nil
}

func (b *Backend) Insert(q Query) error {
	tableToInsert, ok := b.checkTableExist(q)
	if !ok {
		return errors.New("Table does not exist")
	}

	allrows := make([][]byte, 0)
	pageid := PageID(tableToInsert.lastPage)
	lastrownum := tableToInsert.lastRowId
	insertColumns := make([]InsertColumn, len(tableToInsert.Columns))
	queryCols := make([]string, len(q.Fields))
	copy(queryCols, q.Fields)

	for i, col := range tableToInsert.Columns {
		insertColumns[i].columnSize = col.columnSize
		insertColumns[i].dataType = col.columnType
		isNull := true
		for j := range queryCols {
			if queryCols[j] == col.columnName {
				queryCols = removeColField(queryCols, j)
				isNull = false
				insertColumns[i].insertIndex = j
				break
			}
		}
		if isNull && col.columnConstraint == COL_ROWID { //supposes primary key is rowid
			insertColumns[i].colType = COL_I_PRIMARYNULL
		} else if !isNull && col.columnConstraint == COL_ROWID {
			insertColumns[i].colType = COL_I_PRIMARYVALUED
		} else if isNull {
			insertColumns[i].colType = COL_I_NULL
		} else if !isNull {
			insertColumns[i].colType = COL_I_VALUED
		}
	}
	if len(queryCols) > 0 {
		return fmt.Errorf("Columns may not exist: %s", strings.Join(queryCols, " - "))
	}

	for _, val := range q.Inserts {
		nullColumns := InitializeBitSet(uint64(len(tableToInsert.Columns) + 1))
		nullColumns.setBit(len(tableToInsert.Columns) + 1)
		rowInsert := make([]byte, 0, tableToInsert.GenerateRowBytes()+nullColumns.Size())
		byteIndex := nullColumns.Size()

		for j := range insertColumns {
			var b []byte = make([]byte, 0)

			if insertColumns[j].colType == COL_I_PRIMARYVALUED {
				n, err := strconv.Atoi(val[insertColumns[j].insertIndex])
				if err != nil {
					return errors.Join(errors.New("Insert Query failed: "), err)
				}
				lastrownum = int64(n)
				b = binary.LittleEndian.AppendUint64(b, uint64(n))
			} else if insertColumns[j].colType == COL_I_PRIMARYNULL {
				n := lastrownum + 1
				b = binary.LittleEndian.AppendUint64(b, uint64(n))
			} else if insertColumns[j].colType == COL_I_VALUED {
				switch insertColumns[j].dataType {
				case INT:
					n, err := strconv.Atoi(val[insertColumns[j].insertIndex])
					if err != nil {
						return errors.Join(errors.New("Insert Query failed: "), err)
					}
					b = binary.LittleEndian.AppendUint64(b, uint64(n))
				case FLOAT:
					n, err := strconv.ParseFloat(val[insertColumns[j].insertIndex], 64)
					if err != nil {
						return errors.Join(errors.New("Insert Query failed: "), err)
					}
					b = binary.LittleEndian.AppendUint64(b, math.Float64bits(n))
				case BOOL:
					n, err := strconv.ParseBool(val[insertColumns[j].insertIndex])
					if err != nil {
						return errors.Join(errors.New("Insert Query failed: "), err)
					}
					if n {
						b = append(b, byte(1))
					} else {
						b = append(b, byte(0))
					}
				case CHAR:
					n := []byte(val[insertColumns[j].insertIndex])
					if len(n) > int(insertColumns[j].columnSize) {
						return errors.Join(errors.New("Insert Query failed: "), errors.New("string to insert larger than allowed"))
					}
					b = append(b, n...)
				}
			} else if insertColumns[j].colType == COL_I_NULL {
				nullColumns.setBit(j)
			}

			copy(rowInsert[byteIndex:], b)
			byteIndex += uint64(insertColumns[j].columnSize)
		}
		copy(rowInsert[0:], nullColumns.bytes)
		allrows = append(allrows, rowInsert)
	}

	n, err := b.bufferPool.InsertData(tableToInsert.Name, pageid, allrows)
	if err != nil {
		return err
	}
	for i := range b.tables {
		if q.TableName == b.tables[i].Name {
			b.tables[i].lastPage = uint64(n)
			b.tables[i].lastRowId = int64(lastrownum)
		}
	}
	return nil
}

func (b *Backend) checkTableExist(q Query) (Table, bool) {
	for i := range b.tables {
		if q.TableName == b.tables[i].Name {
			return b.tables[i], true
		}
	}
	return Table{}, false
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

func (b *Backend) Select(q Query) (driver.Rows, error) {
	tmpTable, ok := b.checkTableExist(q)
	if !ok {
		return nil, errors.New("Table does not exist")
	}

	columnsRequest := []Column{}
	bitsetsize := (len(tmpTable.Columns) + 8 - 1) / 8
	tmpcol := q.Fields
	if tmpcol[0] == "*" {
		columnsRequest = append(columnsRequest, tmpTable.Columns...)
	} else {
		for _, col := range tmpTable.Columns {
			for i, str := range tmpcol {
				if col.columnName == str {
					columnsRequest = append(columnsRequest, col)
					tmpcol = removeColField(tmpcol, i)
					continue
				}
			}
		}
		if len(tmpcol) != 0 {
			return nil, fmt.Errorf("Columns not in table: %s", strings.Join(tmpcol, " "))
		}
	}

	rows := &Rows{index: 0, columns: []ResultColumn{}}
	for _, col := range columnsRequest {
		rows.columns = append(rows.columns, ResultColumn{Name: col.columnName, ColumnType: col.columnType})
	}
	rows.rows = make([][]Cell, 0)
	rowsize := tmpTable.GenerateRowBytes()
	rowbitset := InitializeBitSet(uint64(bitsetsize))

	startPage := PageID(0)
	endPage := PageID(tmpTable.lastPage)
	pages := b.bufferPool.SelectDataRange(tmpTable.Name, startPage, endPage)

	for i, page := range pages {
		if page == nil {
			return nil, fmt.Errorf("page %d has been corrupted", i)
		}
		rowNums := binary.LittleEndian.Uint16(page.buf[8:10])
		checksum := page.buf[10:26]

		checksumcheck := md5.Sum(page.buf[26:])
		if !bytes.Equal(checksum, checksumcheck[:]) {
			return nil, fmt.Errorf("page %d has been corrupted", i)
		}
		offset := 26
		for j := 0; j < int(rowNums); j++ {
			row := make([]Cell, len(columnsRequest))
			tmprow := page.buf[offset : rowsize+uint64(bitsetsize)]
			rowbitset.fromBytes(tmprow[:bitsetsize])
			for k, col := range columnsRequest {
				if rowbitset.hasBit(col.columnIndex) {
					row[k] = nil
					continue
				}
				celloffset := bitsetsize + col.columnOffset
				row[k] = tmprow[celloffset : celloffset+int(col.columnSize)]
			}
			rows.rows = append(rows.rows, row)
			offset += int(rowsize) + bitsetsize
		}
		b.bufferPool.UnpinPage(tmpTable.Name, page.slotid)
	}

	return rows, nil
}

func removeColField(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}
