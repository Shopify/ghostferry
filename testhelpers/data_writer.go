package testhelpers

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync"

	sq "github.com/Masterminds/squirrel"
)

var dataletters = []rune("abcdefghijklmnopqrstuvwxyz")

func RandData() string {
	b := make([]rune, 32)
	for i := range b {
		b[i] = dataletters[rand.Intn(len(dataletters))]
	}
	return string(b) + "👻"
}

// RandByteData returns a slice of random bytes
func RandByteData() []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = byte(rand.Intn(0x100))
	}
	return b
}

// RandUTF8MB4Data returns a UTF-8 string with valid codepoints up to U+10FFFF
func RandUTF8MB4Data() string {
	b := make([]rune, 32)
	for i := range b {
		b[i] = randUnicodeRune()
	}
	return string(b)
}

// RandLatin1Data returns a UTF-8 string with valid codepoints for a latin1 charset
func RandLatin1Data() string {
	b := make([]rune, 32)
	for i := range b {
		b[i] = rune(rand.Intn(95) + 160)
	}
	return string(b)
}

func randUnicodeRune() rune {
	// The maximum unicode code point is 10FFFF
	r := rune(rand.Intn(0x10FFFF + 1))
	// U+D800 through U+DFFF are prohibited
	if r >= 0xD800 && r <= 0xDFFF {
		r += (0xDFFF - 0xD800)
	}
	return r
}

func SeedInitialData(db *sql.DB, dbname, tablename string, numberOfRows int) {
	var query string
	var err error

	query = "CREATE DATABASE IF NOT EXISTS %s"
	query = fmt.Sprintf(query, dbname)
	_, err = db.Exec(query)
	PanicIfError(err)

	query = "CREATE TABLE %s.%s (id bigint(20) not null auto_increment, data TEXT, primary key(id))"
	query = fmt.Sprintf(query, dbname, tablename)

	_, err = db.Exec(query)
	PanicIfError(err)

	tx, err := db.Begin()
	PanicIfError(err)

	for i := 0; i < numberOfRows; i++ {
		query = "INSERT INTO %s.%s (id, data) VALUES (?, ?)"
		query = fmt.Sprintf(query, dbname, tablename)

		_, err = tx.Exec(query, nil, RandData())
		PanicIfError(err)
	}

	PanicIfError(tx.Commit())
}

func AddTenantID(db *sql.DB, dbName, tableName string, numberOfTenants int) {
	query := "ALTER TABLE %s.%s ADD tenant_id bigint(20)"
	query = fmt.Sprintf(query, dbName, tableName)
	_, err := db.Exec(query)
	PanicIfError(err)

	query = "UPDATE %s.%s SET tenant_id = id %% ?"
	query = fmt.Sprintf(query, dbName, tableName)
	_, err = db.Exec(query, numberOfTenants)
	PanicIfError(err)
}

func DeleteTestDBs(db *sql.DB) {
	for _, dbname := range ApplicableTestDbs {
		_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbname))
		PanicIfError(err)
	}
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

	ExtraInsertData func(string, map[string]interface{})
	ExtraUpdateData func(string, map[string]interface{})

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
	colvals["data"] = RandData()

	if this.ExtraInsertData != nil {
		this.ExtraInsertData(table, colvals)
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
	colvals["data"] = RandData()

	if this.ExtraUpdateData != nil {
		this.ExtraUpdateData(table, colvals)
	}

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
