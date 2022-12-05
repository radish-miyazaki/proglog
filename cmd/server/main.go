package main

import (
	"log"

	"github.com/radish-miyazaki/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":5000")
	log.Fatal(srv.ListenAndServe())
}
