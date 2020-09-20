# mysqlsync



## Introduction
mysqlsync is a program to sync rows greater than a specific date from a source table to a destination MySQL table. It uses Viper to pull in declare the source and destination configuration and also gomail to send an email notification once the program has run.

## Prerequisites
- The program requires that you have a 'date' column in the source table.

## Usage
- Update the config-template.yml and save as config.yml in the same folder as the application
- Update the struct(s) in the helpers.go and the SQL queries in mysqlsync.go

## Build

go build *.go -o mysqlsync

## Known Issues and Limitations
- For simple use cases manually updating the structs and queries is fine, but this won't scale for more complicated use cases.
- Most of the code is not DRY and should be refactored in to functions.