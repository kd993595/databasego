package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTable(t *testing.T) {
	mytable := Table{
		Name: "MyTable2024",
		Columns: []Column{
			{columnName: "a", columnType: INT, columnSize: 8},
			{columnName: "column2", columnType: FLOAT, columnSize: 8},
			{columnName: "ThirdColumn", columnType: CHAR, columnSize: 111},
		},
	}
	buf := mytable.toBytes()
	t.Logf("buf size: %d", len(buf))

	newtable := fromBytes(buf)
	require.Equal(t, mytable, newtable)
}
