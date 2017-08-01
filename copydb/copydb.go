package copydb

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type CopydbFerry struct {
	ferry *ghostferry.Ferry
}

func NewFerry(config *ghostferry.Config) *CopydbFerry {
	ferry := &ghostferry.Ferry{
		Config: config,
	}

	return &CopydbFerry{ferry: ferry}
}

func (this *CopydbFerry) Initialize() error {
	return this.ferry.Initialize()
}

func (this *CopydbFerry) Start() error {
	err := this.ferry.Start()
	if err != nil {
		return err
	}

	// We need to create the same table/schemas on the target database
	// as the ones we are copying.
	for tableName := range this.ferry.Tables {
		t := strings.Split(tableName, ".")
		if _, exists := this.ferry.ApplicableDatabases[t[0]]; !exists {
			continue
		}

		err := this.createDatabaseIfExistsOnTarget(t[0])
		if err != nil {
			logrus.WithError(err).WithField("database", t[0]).Error("cannot create database, this may leave the target database in an insane state")
			return err
		}

		err = this.createTableOnTarget(t[0], t[1])
		if err != nil {
			logrus.WithError(err).WithField("table", tableName).Error("cannot create table, this may leave the target database in an insane state")
			return err
		}
	}
	return nil
}

func (this *CopydbFerry) createDatabaseIfExistsOnTarget(database string) error {
	createDatabaseQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database)
	logrus.Infof("TARGET: %s", createDatabaseQuery)
	_, err := this.ferry.TargetDB.Exec(createDatabaseQuery)
	return err
}

func (this *CopydbFerry) createTableOnTarget(database, table string) error {
	var tableNameAgain, createTableQuery string

	r := this.ferry.SourceDB.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s.%s", database, table))
	err := r.Scan(&tableNameAgain, &createTableQuery)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error":    err,
			"database": database,
			"table":    table,
		}).Error("unable to show table on source")
		return err
	}

	// Somehow, when you issue a CREATE TABLE db.tbl ... query on a connection
	// that does not have a database selected (such as the connections held
	// by ghostferry.Ferry), either the Go MySQL driver or MySQL itself will
	// throw an error saying that a database is not selected.
	//
	// This means we have to, unfortunately, reconnect again with a database
	// selected to create a table on the target.
	//
	// This maybe related to the fact that I'm using the siddontang driver as
	// opposed to the "official" MySQL driver to save on the number of
	// dependencies I have.
	//
	// TODO: investigate this issue and see if we can switch back to using
	//       the ferry DB connection.
	dsn := fmt.Sprintf(
		"%s:%s@%s:%d?%s",
		this.ferry.TargetUser,
		this.ferry.TargetPass,
		this.ferry.TargetHost,
		this.ferry.TargetPort,
		database,
	)
	target, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer target.Close()

	_, err = target.Exec(createTableQuery)
	return err
}

func (this *CopydbFerry) Run() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		this.ferry.Run()
	}()
	this.ferry.WaitUntilRowCopyIsComplete()

	// TODO: set source to be readonly if specified via flag or script
	// TODO: ensure that all transactions are flushed (how?)

	this.ferry.FlushBinlogAndStopStreaming()

	// TODO: call external cutover scripts if applicable.
	// TODO: set target to be writable (if applicable).
	wg.Wait()

	// TODO: handle signals to shutdown the control server.
	this.ferry.WaitForControlServer()
}
