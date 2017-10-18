package copydb

import (
	"fmt"
	"strings"
	"sync"

	"github.com/Shopify/ghostferry"
	"github.com/sirupsen/logrus"
)

type CopydbFerry struct {
	ferry         *ghostferry.Ferry
	controlServer *ghostferry.ControlServer
	config        *Config
}

func NewFerry(config *Config) *CopydbFerry {
	ferry := &ghostferry.Ferry{
		Config: &config.Config,
	}

	controlServer := &ghostferry.ControlServer{
		F:       ferry,
		Addr:    config.ServerBindAddr,
		Basedir: config.WebBasedir,
	}

	return &CopydbFerry{
		ferry:         ferry,
		controlServer: controlServer,
		config:        config,
	}
}

func (this *CopydbFerry) Initialize() error {
	err := this.ferry.Initialize()
	if err != nil {
		return err
	}

	return this.controlServer.Initialize()
}

func (this *CopydbFerry) Start() error {
	err := this.ferry.Start()
	if err != nil {
		return err
	}

	this.ferry.Verifier = &ghostferry.ChecksumTableVerifier{
		TablesToCheck: this.ferry.Tables.AsSlice(),
	}
	return nil
}

func (this *CopydbFerry) CreateDatabasesAndTables() error {
	// We need to create the same table/schemas on the target database
	// as the ones we are copying.
	for tableName := range this.ferry.Tables {
		t := strings.Split(tableName, ".")
		if _, exists := this.config.ApplicableDatabases[t[0]]; !exists {
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

func (this *CopydbFerry) Run() {
	serverWG := &sync.WaitGroup{}
	serverWG.Add(1)
	go this.controlServer.Run(serverWG)

	copyWG := &sync.WaitGroup{}
	copyWG.Add(1)
	go func() {
		defer copyWG.Done()
		this.ferry.Run()
	}()

	// If AutomaticCutover == false, it will pause below the following line
	this.ferry.WaitUntilRowCopyIsComplete()

	// This waits until we're pretty close in the binlog before making the
	// source readonly. This is to avoid excessive downtime caused by the
	// binlog streamer catching up.
	this.ferry.WaitUntilBinlogStreamerCatchesUp()

	// This is when the source database should be set as read only, whether it
	// is done in application level or the database level.
	// Must ensure that all transactions are flushed to the binlog before
	// proceeding.
	this.ferry.FlushBinlogAndStopStreaming()

	// After waiting for the binlog streamer to stop, the source and the target
	// should be identical.
	copyWG.Wait()

	// This is where you cutover from using the source database to
	// using the target database.

	// Work is done, the process will run the web server until killed.
	serverWG.Wait()
}

func (this *CopydbFerry) createDatabaseIfExistsOnTarget(database string) error {
	createDatabaseQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database)
	_, err := this.ferry.TargetDB.Exec(createDatabaseQuery)
	return err
}

func (this *CopydbFerry) createTableOnTarget(database, table string) error {
	var tableNameAgain, createTableQuery string

	r := this.ferry.SourceDB.QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", database, table))
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
