package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/kd993595/fusedb"
)

// https://github.com/krasun/fbptree

func main() {
	// List registered drivers, now with dummy
	fmt.Printf("Drivers=%#v\n", sql.Drivers())

	db, err := sql.Open("fusedb", "example")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("ping 1")
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Query("CREATE TABLE 'MyTable10' (column1 int Primary Key, name char(10),column30 bool, column400 float)")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Query("INSERT INTO 'MyTable10' (column1,name,column30,column400) VALUES ('1','somecharss', 'true','1.23')")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Query("INSERT INTO 'MyTable10' (column1,name,column30,column400) VALUES ('2','10letters', 'false','4.69')")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Query("INSERT INTO 'MyTable10' (column1,name,column30,column400) VALUES ('3','Kevin', 't','.567')")
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("SELECT * FROM 'MyTable10'")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var i int
		var n string
		var b bool
		var f float64
		err := rows.Scan(&i, &n, &b, &f)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("(%d) (%s) (%t) (%f)\n", i, n, b, f)
	}

}
