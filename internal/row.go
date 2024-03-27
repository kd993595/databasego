package internal

import (
	"database/sql/driver"
	"io"
)

/*
Result from sql select statement in driver query
Must use pointer for interface to be properly implements and allow pointer to struct
Implements driver.Rows
*/
type Rows struct {
	columns []ResultColumn //should be result column holding name and type
	index   uint64
	rows    [][]Cell //holds all the values returned from the queries which is slice of array of bytes
}

func (r *Rows) Columns() []string {
	columnString := make([]string, 0, len(r.columns))
	for _, c := range r.columns {
		columnString = append(columnString, c.Name)
	}
	return columnString
}

func (r *Rows) Close() error {
	r.index = uint64(len(r.rows))
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.index >= uint64(len(r.rows)) {
		return io.EOF
	}

	row := r.rows[r.index]

	for idx, cell := range row {
		typ := r.columns[idx].ColumnType
		switch typ {
		case INT:
			if cell == nil {
				dest[idx] = nil
			}
			dest[idx] = cell.AsInt()
		case CHAR:
			if cell == nil {
				dest[idx] = nil
			}
			dest[idx] = cell.AsString()
		case FLOAT:
			if cell == nil {
				dest[idx] = nil
			}
			dest[idx] = cell.AsFloat()
		case BOOL:
			if cell == nil {
				dest[idx] = nil
			}
			dest[idx] = cell.AsBool()
		}
	}

	r.index++

	return nil
}
