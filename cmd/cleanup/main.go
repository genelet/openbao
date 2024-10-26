package main

import (
	"fmt"
	"os"

	"github.com/openbao/openbao/physical/tdengine"
)

func main() {
	dbname := "openbao"
	if len(os.Args) > 1 {
		dbname = os.Args[1]
	}
	fmt.Printf("clean up database, %s, at vm0:6030\n", dbname)
	td, err := tdengine.NewTDEngineBackend(map[string]string{
		"connection_url": "root:taosdata@tcp(vm0:6030)/" + dbname,
		"database":       dbname,
	}, nil)
	if err != nil {
		panic(err)
	}
	err = td.DropAllTables()
	if err != nil {
		panic(err)
	}
}
