package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTable(t *testing.T) {
	var tests = []struct {
		name  string
		input Table
		want  Table
	}{
		// the table itself
		{"Table equality test 1", Table{
			Name: "MyTable2024",
			Columns: []Column{
				{columnName: "a", columnType: INT, columnSize: 8},
				{columnName: "column2", columnType: FLOAT, columnSize: 8},
				{columnName: "ThirdColumn", columnType: CHAR, columnSize: 111},
			},
		}, Table{
			Name: "MyTable2024",
			Columns: []Column{
				{columnName: "a", columnType: INT, columnSize: 8},
				{columnName: "column2", columnType: FLOAT, columnSize: 8},
				{columnName: "ThirdColumn", columnType: CHAR, columnSize: 111},
			},
		}},
		{"Table equality test 2", Table{
			Name: "someTable@54",
			Columns: []Column{
				{columnName: "first", columnType: BOOL, columnSize: 1},
				{columnName: "2", columnType: FLOAT, columnSize: 8},
				{columnName: "someColumn", columnType: CHAR, columnSize: 23},
			},
		}, Table{
			Name: "someTable@54",
			Columns: []Column{
				{columnName: "first", columnType: BOOL, columnSize: 1},
				{columnName: "2", columnType: FLOAT, columnSize: 8},
				{columnName: "someColumn", columnType: CHAR, columnSize: 23},
			},
		}},
		{"Table equality test 3", Table{
			Name: "Table3",
			Columns: []Column{
				{columnName: "testing", columnType: FLOAT, columnSize: 8},
				{columnName: "Uncreat1v3", columnType: FLOAT, columnSize: 8},
			},
		}, Table{
			Name: "Table3",
			Columns: []Column{
				{columnName: "testing", columnType: FLOAT, columnSize: 8},
				{columnName: "Uncreat1v3", columnType: FLOAT, columnSize: 8},
			},
		}},
	}
	// The execution loop
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := tt.input.toBytes()
			tt.want.GenerateFields()

			newtable := fromBytes(buf)
			newtable.GenerateFields()
			require.Equal(t, newtable, tt.want)
		})
	}
}
