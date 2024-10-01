package main

import (
	"github.com/openbao/openbao/physical/tdengine"
)

func main() {
	td, err := tdengine.NewTDEngineBackend(map[string]string{
		"connection_url": "root:taosdata@tcp(vm0:6030)/openbao",
		"database":       "openbao",
	}, nil)
	if err != nil {
		panic(err)
	}
	err = td.DropAllTables()
	if err != nil {
		panic(err)
	}
}
