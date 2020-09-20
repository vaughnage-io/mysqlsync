# mysqlsync



## Introduction
mysqlsync is a simple program to sync rows greater than a specific date from a source table to a destination MySQL table. It uses Viper to pull in configuration and gomail to send an email notification.

## Prerequisites
- The program requires that you have a 'date' column in the source table.

## Usage
- Update the config-template.yml and save as config.yml in the same folder as the application

## Build

go build *.go -o mysqlsync