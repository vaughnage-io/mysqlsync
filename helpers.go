package main

import (
	"fmt"
	"log"
	"os"
)

// Table1 : Struct for the MySQL table
type Table1 struct {
	Tk         string `json: tk`
	Name       string `json:"name"`
	Department string `json:"department"`
}

// CreateMySQLDSN : String manipulation to formulate the MySQL DSN
func CreateMySQLDSN(user, pass, host, port, db string) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, db)
	return dsn
}

// GetHostName : Retrieves the hostname of the server the code is being run on
func GetHostName() string {
	name, err := os.Hostname()
	if err != nil {
		log.Printf("ERROR: Unable to retrieve hostname, defaulting to 'Unknown'")
		name = "unknown"
	}
	return name
}
