package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
	"gopkg.in/gomail.v2"
)

func init() {
	// Read in the ./config.yml file
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("FATAL: %s\n", err)
	}

}

func main() {
	// Configure Logging
	f, err := os.OpenFile("mysqlsync.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()
	logger := log.New(f, "prefix", log.LstdFlags)

	log.Println("INFO: Starting MySQL sync, hang on to your helmets...")
	start := time.Now()

	// Source MySQL Variables
	suser := viper.GetString("source.username")
	spass := viper.GetString("source.password")
	shost := viper.GetString("source.host")
	sport := viper.GetString("source.port")
	sdb := viper.GetString("source.db")
	stbl1 := viper.GetString("source.tbl1")
	sdsn := CreateMySQLDSN(suser, spass, shost, sport, sdb)

	// Destination MySQL Variables
	duser := viper.GetString("destination.username")
	dpass := viper.GetString("destination.password")
	dhost := viper.GetString("destination.host")
	dport := viper.GetString("destination.port")
	ddb := viper.GetString("destination.db")
	dtbl1 := viper.GetString("destination.tbl1")
	ddsn := CreateMySQLDSN(duser, dpass, dhost, dport, ddb)

	// Create source db connection and test connectivity
	sourcedb, err := sql.Open("mysql", sdsn)
	if err != nil {
		logger.Printf("FATAL: Unable to create SQL connection, failed with error: %s\n", err)
		log.Fatalf("FATAL: Unable to create SQL connection, failed with error: %s\n", err)
	}
	sourcedb.SetConnMaxLifetime(time.Minute * 3)
	sourcedb.SetMaxOpenConns(10)
	sourcedb.SetMaxIdleConns(10)

	defer sourcedb.Close()
	err = sourcedb.Ping()
	if err != nil {
		logger.Printf("FATAL: %s/%s: MySQL connectivity test failed with error: %s\n", shost, sport, err)
		log.Fatalf("FATAL: %s/%s: MySQL connectivity test failed with error: %s\n", shost, sport, err)

	} else {
		logger.Printf("INFO: %s/%s: MySQL connectivity test passed\n", shost, sport)
		log.Printf("INFO: %s/%s: MySQL connectivity test passed\n", shost, sport)
	}
	// Create destination db connection and test connectivity
	destdb, err := sql.Open("mysql", ddsn)
	if err != nil {
		logger.Printf("FATAL: Unable to create SQL connection, failed with error: %s\n", err)
		log.Fatalf("FATAL: Unable to create SQL connection, failed with error: %s\n", err)
	}
	destdb.SetConnMaxLifetime(time.Minute * 3)
	destdb.SetMaxOpenConns(10)
	destdb.SetMaxIdleConns(10)
	defer destdb.Close()
	err = destdb.Ping()
	if err != nil {
		logger.Printf("FATAL: %s/%s: MySQL connectivity test failed with error: %s\n", dhost, dport, err)
		log.Fatalf("FATAL: %s/%s: MySQL connectivity test failed with error: %s\n", dhost, dport, err)
	} else {
		logger.Printf("INFO: %s/%s: MySQL connectivity test passed\n", dhost, dport)
		log.Printf("INFO: %s/%s: MySQL connectivity test passed\n", dhost, dport)
	}

	// Sync Data
	var lastsync string
	err = destdb.QueryRow(fmt.Sprintf("SELECT date FROM %s.%s ORDER BY tk DESC LIMIT 1", ddb, dtbl1)).Scan(&lastsync)
	if err != nil {
		if err == sql.ErrNoRows {
			lastsync = "2020-09-12 00:00:00"
			logger.Printf("INFO: %s:%s/%s: No date found, defaulting to '%s' as last sync date\n", dhost, dport, dtbl1, lastsync)
			log.Printf("INFO: %s:%s/%s: No date found, defaulting to '%s' as last sync date\n", dhost, dport, dtbl1, lastsync)
		} else {
			logger.Printf("FATAL: %s:%s/%s: Failed retrieving last sync date with error: %s\n", dhost, dport, dtbl1, err)
			log.Fatalf("FATAL: %s:%s/%s: Failed retrieving last sync date with error: %s\n", dhost, dport, dtbl1, err)
		}
	} else {
		logger.Printf("INFO: %s:%s/%s: Last sync date is '%s'\n", dhost, dport, dtbl1, lastsync)
		log.Printf("INFO: %s:%s/%s: Last sync date is '%s'\n", dhost, dport, dtbl1, lastsync)
	}
	var rowcount int
	row := sourcedb.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE date > '%s'", sdb, stbl1, lastsync))
	err = row.Scan(&rowcount)
	if err != nil {
		logger.Printf("FATAL: %s:%s/%s: Failed counting rows to be synced with error: %s\n", shost, sport, stbl1, err)
		log.Fatalf("FATAL: %s:%s/%s: Failed counting rows to be synced with error: %s\n", shost, sport, stbl1, err)
	} else {
		logger.Printf("INFO: %s:%s/%s: Counted %v rows, starting sync...\n", shost, sport, stbl1, rowcount)
		log.Printf("INFO: %s:%s/%s: Counted %v rows, starting sync...\n", shost, sport, stbl1, rowcount)
	}
	results, err := sourcedb.Query(fmt.Sprintf("SELECT tk, name, department FROM %s.%s WHERE date > '%s'", sdb, stbl1, lastsync))
	if err != nil {
		logger.Printf("FATAL: %s:%s/%s: Failed to retrieve rows with error: %s\n", shost, sport, stbl1, err)
		log.Fatalf("FATAL: %s:%s/%s: Failed to retrieve rows with error: %s\n", shost, sport, stbl1, err)
	}
	r := Table1{}
	for results.Next() {
		err = results.Scan(&r.Tk, &r.Name, &r.Department)
		if err != nil {
			logger.Printf("FATAL: %s:%s/%s: Failed to retrieve rows from source MySQL server with error: %s\n", shost, sport, stbl1, err)
			log.Fatalf("FATAL: %s:%s/%s: Failed to retrieve rows from source MySQL server with error: %s\n", shost, sport, stbl1, err)
		} else {
			stmtIns, err := destdb.Prepare(fmt.Sprintf("INSERT INTO %s (tk, name, department) VALUES (?, ?, ?)", dtbl1))
			if err != nil {
				logger.Printf("FATAL: %s:%s:%s: Failed to prepare destination MySQL server with error: %s\n", dhost, dport, stbl1, err)
				log.Fatalf("FATAL: %s:%s:%s: Failed to prepare destination MySQL server with error: %s\n", dhost, dport, stbl1, err)
			}
			_, err = stmtIns.Exec(r.Tk, r.Name, r.Department)
			if err != nil {
				logger.Printf("Failed to insert rows in destination MySQL server %s:%s with error: %s\n", dhost, dport, err)
				log.Fatalf("Failed to insert rows in destination MySQL server %s:%s with error: %s\n", dhost, dport, err)
				defer stmtIns.Close()
			}
		}

	}
	logger.Printf("INFO: %s:%s/%s -> %s:%s/%s : Inserted %v new rows since last sync\n", shost, sport, stbl1, dhost, dport, dtbl1, rowcount)
	log.Printf("INFO: %s:%s/%s -> %s:%s/%s : Inserted %v new rows since last sync\n", shost, sport, stbl1, dhost, dport, dtbl1, rowcount)

	elapsed := time.Since(start)

	// Send Notification Email
	h := GetHostName()
	m := gomail.NewMessage()
	m.SetHeader("From", viper.GetString("mail.from"))
	addresses := viper.GetStringSlice("mail.to")
	m.SetHeader("To", addresses...)
	m.SetHeader("Subject", viper.GetString("mail.subject"))
	m.SetBody("text/plain", fmt.Sprintf("MySQL sync completed successfully in %s on server %s.\n%v rows synced from %s:%s/%s to %s:%s/%s.\n%v rows synced from %s:%s/%s to %s:%s/%s.\nLog file located at %s:/home/tungsten/mysqlsync.log\nTo change the email settings please edit %s:/home/tungsten/config.yml", elapsed, h, pervmcount, shost, sport, spervmtbl, dhost, dport, dpervmtbl, rowcount, shost, sport, stbl1, dhost, dport, dtbl1, h, h))
	d := gomail.NewDialer(viper.GetString("mail.host"), viper.GetInt("mail.port"), viper.GetString("mail.user"), viper.GetString("mail.password"))
	if err := d.DialAndSend(m); err != nil {
		logger.Printf("ERROR: Unable to send email with error : %s", err)
		log.Printf("ERROR: Unable to send email with error : %s", err)
	} else {
		logger.Printf("INFO: Notification email sent to %v", addresses)
		log.Printf("INFO: Notification email sent to %v", addresses)
	}
	logger.Printf("INFO: MySQL sync completed successfully in %s", elapsed)
	log.Printf("INFO: MySQL sync completed successfully in %s", elapsed)
}
