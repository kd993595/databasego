package main

import (
	"database/sql"
	"fmt"

	_ "github.com/kd993595/fusedb/internal"
)

// https://github.com/krasun/fbptree

func main() {
	// List registered drivers, now with dummy
	fmt.Printf("Drivers=%#v\n", sql.Drivers())

	db, err := sql.Open("fusedb", "example")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fmt.Println("ping 1")
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	fmt.Println("ping 2")
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	// _, err = db.Query("CREATE TABLE 'MyTable10' (column1 int Primary Key, column2 char(20),column30 bool, column400 float)")
	// if err != nil {
	// 	panic(err)
	// }

	// rows, err := db.Query("SELECT name, age FROM users;")
	// if err != nil {
	// 	panic(err)
	// }
	// _ = rows

	// for rows.Next() {
	// 	err := rows.Scan()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

}
