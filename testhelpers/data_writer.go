package testhelpers

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync"

	sq "github.com/Masterminds/squirrel"
)

var dataletters = []rune("abcdefghijklmnopqrstuvwxyz")

func randData() string {
	b := make([]rune, 32)
	for i := range b {
		b[i] = dataletters[rand.Intn(len(dataletters))]
	}
	return string(b)
}

func SeedInitialData(db *sql.DB, dbname, tablename string, numberOfRows, tenants int) error {
	var query string
	var err error

	query = "CREATE DATABASE IF NOT EXISTS %s"
	query = fmt.Sprintf(query, dbname)
	_, err = db.Exec(query)
	if err != nil {
		return err
	}

	query = "CREATE TABLE %s.%s (id bigint(20) not null auto_increment, tenant_id bigint(20), data TEXT, primary key(id))"
	query = fmt.Sprintf(query, dbname, tablename)

	_, err = db.Exec(query)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for i := 0; i < numberOfRows; i++ {
		query = "INSERT INTO %s.%s (id, tenant_id, data) VALUES (?, ?, ?)"
		query = fmt.Sprintf(query, dbname, tablename)

		_, err = tx.Exec(query, nil, i%tenants+1, randData())
		if err != nil {
			return err
		}

	}

	return tx.Commit()
}

type DataWriter interface {
	Run()
	Stop()
	Wait()
	SetDB(*sql.DB)
}

type MixedActionDataWriter struct {
	ProbabilityOfInsert float32
	ProbabilityOfUpdate float32
	ProbabilityOfDelete float32

	ModifyIdRange [2]int64

	NumberOfWriters int
	Db              *sql.DB
	Tables          []string
	TenantValues    []int

	wg     *sync.WaitGroup
	doneCh chan struct{}
}

func (this *MixedActionDataWriter) Run() {
	this.wg = &sync.WaitGroup{}
	this.doneCh = make(chan struct{})

	this.wg.Add(this.NumberOfWriters)
	for i := 0; i < this.NumberOfWriters; i++ {
		go this.WriteData(i)
	}

	this.Wait()
}

func (this *MixedActionDataWriter) Wait() {
	this.wg.Wait()
}

func (this *MixedActionDataWriter) Stop() {
	close(this.doneCh)
}

func (this *MixedActionDataWriter) SetDB(db *sql.DB) {
	this.Db = db
}

func (this *MixedActionDataWriter) WriteData(i int) {
	defer this.wg.Done()

	insertProbability := [2]float32{0, this.ProbabilityOfInsert}
	updateProbability := [2]float32{insertProbability[1], insertProbability[1] + this.ProbabilityOfUpdate}
	deleteProbability := [2]float32{updateProbability[1], updateProbability[1] + this.ProbabilityOfDelete}

	for {
		select {
		case _, _ = <-this.doneCh:
			return
		default:
		}

		r := rand.Float32()
		var err error
		if r >= insertProbability[0] && r < insertProbability[1] {
			err = this.InsertData()
		} else if r >= updateProbability[0] && r < updateProbability[1] {
			err = this.UpdateData()
		} else if r >= deleteProbability[0] && r < deleteProbability[1] {
			err = this.DeleteData()
		} else {
			// skip
		}

		if err != nil {
			panic(err)
		}
	}
}

func (this *MixedActionDataWriter) InsertData() error {
	table := this.pickRandomTable()

	colvals := make(map[string]interface{})
	colvals["id"] = nil
	colvals["data"] = randData()

	if this.TenantValues != nil {
		colvals["tenant_id"] = this.TenantValues[rand.Intn(len(this.TenantValues))]
	}

	sql, args, err := sq.Insert(table).SetMap(colvals).ToSql()
	if err != nil {
		return err
	}

	_, err = this.Db.Exec(sql, args...)
	return err
}

func (this *MixedActionDataWriter) UpdateData() error {
	table := this.pickRandomTable()

	id, err := this.getIdFromModifyIdRange(table)
	if err != nil {
		return err
	}

	colvals := make(map[string]interface{})
	colvals["data"] = randData()

	sql, args, err := sq.Update(table).
		SetMap(colvals).
		Where(sq.GtOrEq{"id": id}).
		Limit(1).ToSql()

	if err != nil {
		return err
	}

	_, err = this.Db.Exec(sql, args...)
	return err
}

func (this *MixedActionDataWriter) DeleteData() error {
	table := this.pickRandomTable()

	id, err := this.getIdFromModifyIdRange(table)
	if err != nil {
		return err
	}

	sql, args, err := sq.Delete(table).
		Where(sq.GtOrEq{"id": id}).
		Limit(1).ToSql()
	if err != nil {
		return err
	}

	_, err = this.Db.Exec(sql, args...)
	return err
}

func (this *MixedActionDataWriter) getIdFromModifyIdRange(table string) (int64, error) {
	var idRange [2]int64
	if this.ModifyIdRange[0] == 0 && this.ModifyIdRange[1] == 0 {
		row := this.Db.QueryRow(fmt.Sprintf("SELECT MIN(id), MAX(id) FROM %s", table))
		err := row.Scan(&idRange[0], &idRange[1])
		if err != nil {
			return 0, nil
		}
	} else {
		idRange = this.ModifyIdRange
	}

	return idRange[0] + rand.Int63n(idRange[1]-idRange[0]), nil
}

func (this *MixedActionDataWriter) pickRandomTable() string {
	tableIndex := rand.Intn(len(this.Tables))
	return this.Tables[tableIndex]
}
