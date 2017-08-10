package copydb

import (
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

	tables := make([]string, 0)

	// We need to create the same table/schemas on the target database
	// as the ones we are copying.
	for tableName := range this.ferry.Tables {
		tables = append(tables, tableName)
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

	this.ferry.Verifier = &ghostferry.ChecksumTableVerifier{
		TablesToCheck: tables,
	}
	return nil
}

func (this *CopydbFerry) createDatabaseIfExistsOnTarget(database string) error {
	createDatabaseQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database)
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

	createTableQueryReplaced := strings.Replace(
		createTableQuery,
		fmt.Sprintf("CREATE TABLE `%s`", tableNameAgain),
		fmt.Sprintf("CREATE TABLE `%s`.`%s`", database, tableNameAgain),
		1,
	)

	if createTableQueryReplaced == createTableQuery {
		return fmt.Errorf("no effect on replacing the create table <table> with create table <db>.<table> query on query: %s", createTableQuery)
	}

	_, err = this.ferry.TargetDB.Exec(createTableQueryReplaced)
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
