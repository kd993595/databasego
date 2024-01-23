package internal

//https://pkg.go.dev/database/sql#Rows
//https://notes.eatonphil.com/database-basics-a-database-sql-driver.html
//https://vyskocilm.github.io/blog/implement-sql-database-driver-in-100-lines-of-go/
import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
)

// Implements sql driver interface for opening database and returning connection to database
type Driver struct {
}

func init() {
	sql.Register("fusedb", &Driver{})
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	//fmt.Println(name)
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		err = os.Mkdir(name, 0755) //https://stackoverflow.com/questions/14249467/os-mkdir-and-os-mkdirall-permissions
		if err != nil {
			return nil, err
		}

		return &Conn{CreateNewDatabase(name)}, nil
	}

	tempdb, err := OpenExistingDatabase(name)
	if err != nil {
		return nil, err
	}
	return &Conn{tempdb}, nil
}

// Connection to the database
type Conn struct {
	db *Backend
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("Prepare not implemented")
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, errors.New("Begin not implemented")
}

func (c *Conn) Close() error {
	fmt.Println("closing database")
	return nil
}

func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		// TODO: support parameterization
		panic("Parameterization not supported")
	}

	ast, err := Parse(query) //check if query for tablename is too long must be less than 16bits
	if err != nil {
		return nil, fmt.Errorf("error while parsing: %s", err)
	}

	// NOTE: ignorning all but the first statement
	stmt := ast.Type
	switch stmt {
	case Create:
		err := c.db.CreateTable(ast)
		return nil, err

	default:
		return nil, errors.ErrUnsupported
	}

	//want to return rows struct
	return nil, nil
}

/*
Result from sql select statement in driver query
*/
type Rows struct {
	columns []string //should be result column holding name and type
	index   uint64
	rows    [][]interface{}
}

func (r *Rows) Columns() []string {
	return nil
}

func (r *Rows) Close() error {
	r.index = uint64(len(r.rows))
	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	if r.index >= uint64(len(r.rows)) {
		return io.EOF
	}
	//not implemented

	return nil
}
