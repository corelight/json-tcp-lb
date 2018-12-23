// +build prof

package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
)

func init() {
	go func() {
		log.Println("Listening on localhost:6060 for pprof")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}
