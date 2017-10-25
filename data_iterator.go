package ghostferry

import (
	"container/ring"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

type PKPositionLog struct {
	Position uint64
	At       time.Time
}

type DataIteratorState struct {
	// We need to lock these maps as Go does not support concurrent access to
	// maps.
	// Maybe able to convert to sync.Map in Go1.9
	targetPrimaryKeys         map[string]uint64
	lastSuccessfulPrimaryKeys map[string]uint64
	completedTables           map[string]bool
	copySpeedLog              *ring.Ring

	targetPkMutex     *sync.RWMutex
	successfulPkMutex *sync.RWMutex
	tablesMutex       *sync.RWMutex
}

func newDataIteratorState(tableIteratorCount int) *DataIteratorState {
	// We want to make sure that the ring buffer is not filled with
	// only the timestamp from the last iteration.
	//
	// Having it some multiple times of the number of table iterators
	// _should_ allow for this.
	speedLog := ring.New(tableIteratorCount * 5)
	speedLog.Value = PKPositionLog{
		Position: 0,
		At:       time.Now(),
	}

	return &DataIteratorState{
		targetPrimaryKeys:         make(map[string]uint64),
		lastSuccessfulPrimaryKeys: make(map[string]uint64),
		completedTables:           make(map[string]bool),
		copySpeedLog:              speedLog,
		targetPkMutex:             &sync.RWMutex{},
		successfulPkMutex:         &sync.RWMutex{},
		tablesMutex:               &sync.RWMutex{},
	}
}

func (this *DataIteratorState) UpdateTargetPK(table string, pk uint64) {
	this.targetPkMutex.Lock()
	defer this.targetPkMutex.Unlock()

	this.targetPrimaryKeys[table] = pk
}

func (this *DataIteratorState) UpdateLastSuccessfulPK(table string, pk uint64) {
	this.successfulPkMutex.Lock()
	defer this.successfulPkMutex.Unlock()

	deltaPK := pk - this.lastSuccessfulPrimaryKeys[table]
	this.lastSuccessfulPrimaryKeys[table] = pk

	currentTotalPK := this.copySpeedLog.Value.(PKPositionLog).Position
	this.copySpeedLog = this.copySpeedLog.Next()
	this.copySpeedLog.Value = PKPositionLog{
		Position: currentTotalPK + deltaPK,
		At:       time.Now(),
	}
}

func (this *DataIteratorState) MarkTableAsCompleted(table string) {
	this.tablesMutex.Lock()
	defer this.tablesMutex.Unlock()

	this.completedTables[table] = true
}

func (this *DataIteratorState) TargetPrimaryKeys() map[string]uint64 {
	this.targetPkMutex.RLock()
	defer this.targetPkMutex.RUnlock()

	m := make(map[string]uint64)
	for k, v := range this.targetPrimaryKeys {
		m[k] = v
	}

	return m
}

func (this *DataIteratorState) LastSuccessfulPrimaryKeys() map[string]uint64 {
	this.successfulPkMutex.RLock()
	defer this.successfulPkMutex.RUnlock()

	m := make(map[string]uint64)
	for k, v := range this.lastSuccessfulPrimaryKeys {
		m[k] = v
	}

	return m
}

func (this *DataIteratorState) CompletedTables() map[string]bool {
	this.tablesMutex.RLock()
	defer this.tablesMutex.RUnlock()

	m := make(map[string]bool)
	for k, v := range this.completedTables {
		m[k] = v
	}

	return m
}

func (this *DataIteratorState) EstimatedPKProcessedPerSecond() float64 {
	this.successfulPkMutex.RLock()
	defer this.successfulPkMutex.RUnlock()

	if this.copySpeedLog.Value.(PKPositionLog).Position == 0 {
		return 0.0
	}

	earliest := this.copySpeedLog
	for earliest.Prev() != nil && earliest.Prev() != this.copySpeedLog && earliest.Prev().Value.(PKPositionLog).Position != 0 {
		earliest = earliest.Prev()
	}

	currentValue := this.copySpeedLog.Value.(PKPositionLog)
	earliestValue := earliest.Value.(PKPositionLog)
	deltaPK := currentValue.Position - earliestValue.Position
	deltaT := currentValue.At.Sub(earliestValue.At).Seconds()

	return float64(deltaPK) / deltaT
}

type DataIterator struct {
	Db           *sql.DB
	Config       *Config
	ErrorHandler ErrorHandler
	Throttler    *Throttler

	Tables []*schema.Table
	Filter CopyFilter

	CurrentState *DataIteratorState

	tableCh        chan *schema.Table
	eventListeners []func([]DMLEvent) error
	doneListeners  []func() error

	logger *logrus.Entry
}

func (this *DataIterator) Initialize() error {
	this.tableCh = make(chan *schema.Table)
	this.logger = logrus.WithField("tag", "data_iterator")

	this.CurrentState = newDataIteratorState(this.Config.NumberOfTableIterators)

	return nil
}

func (this *DataIterator) Run(doneWg *sync.WaitGroup) {
	defer func() {
		this.logger.Info("data iterator done")
		doneWg.Done()
	}()

	this.logger.WithField("tablesCount", len(this.Tables)).Info("starting data iterator run")

	tablesWithData, emptyTables, err := MaxPrimaryKeys(this.Db, this.Tables, this.logger)
	if err != nil {
		this.ErrorHandler.Fatal("data_iterator", err)
		return
	}

	for _, table := range emptyTables {
		this.CurrentState.MarkTableAsCompleted(table.String())
	}

	for table, maxPk := range tablesWithData {
		this.CurrentState.UpdateTargetPK(table.String(), maxPk)
	}

	wg := &sync.WaitGroup{}
	wg.Add(this.Config.NumberOfTableIterators)

	for id := 0; id < this.Config.NumberOfTableIterators; id++ {
		go this.runTableIterator(uint32(id), wg)
	}

	for table, _ := range tablesWithData {
		this.tableCh <- table
	}

	this.logger.Info("done queueing tables to be iterated, closing table channel")

	close(this.tableCh)

	wg.Wait()

	for _, listener := range this.doneListeners {
		listener()
	}
}

func (this *DataIterator) AddEventListener(listener func([]DMLEvent) error) {
	this.eventListeners = append(this.eventListeners, listener)
}

func (this *DataIterator) AddDoneListener(listener func() error) {
	this.doneListeners = append(this.doneListeners, listener)
}

func (this *DataIterator) runTableIterator(id uint32, doneWg *sync.WaitGroup) {
	defer func() {
		this.logger.Infof("table iterator %d done", id)
		doneWg.Done()
	}()

	this.logger.Infof("starting table iterator instance %d", id)

	for {
		table, ok := <-this.tableCh
		if !ok {
			break
		}

		err := this.iterateTable(table)
		if err != nil {
			this.logger.WithFields(logrus.Fields{
				"error": err,
				"id":    id,
				"table": table.String(),
			}).Error("failed to iterate table")
			this.ErrorHandler.Fatal("table_iterator", err)
			return
		}
	}
}

func (this *DataIterator) iterateTable(table *schema.Table) error {
	logger := this.logger.WithField("table", table.String())
	logger.Info("starting to copy table")

	var lastSuccessfulPrimaryKey uint64 = 0
	maxPrimaryKey := this.CurrentState.TargetPrimaryKeys()[table.String()]

	for lastSuccessfulPrimaryKey < maxPrimaryKey {
		var tx *sql.Tx
		var rowEvents []DMLEvent
		var pkpos uint64

		err := WithRetries(this.Config.MaxIterationReadRetries, 0, logger, "fetch rows", func() (err error) {
			this.Throttler.ThrottleIfNecessary()

			// We need to lock SELECT until we apply the updates (done in the
			// listeners). We need a transaction that is open all the way until
			// the update is applied on the target database. This is why we
			// open the transaction outside of the fetchRowsInBatch method.
			//
			// We also need to make sure that the transactions are always rolled
			// back at the end of this iteration of primary key.
			tx, err = this.Db.Begin()
			if err != nil {
				return err
			}

			rowEvents, pkpos, err = this.fetchRowsInBatch(tx, table, table.GetPKColumn(0), lastSuccessfulPrimaryKey)
			if err == nil {
				return nil
			}

			tx.Rollback()
			return err
		})

		if err != nil {
			return err
		}

		if len(rowEvents) == 0 {
			tx.Rollback()
			logger.Info("did not reach max primary key, but the table is completed as there are no more rows")
			break
		}

		for _, listener := range this.eventListeners {
			err = listener(rowEvents)
			if err != nil {
				tx.Rollback()
				logger.WithError(err).Error("failed to process events with listeners")
				return err
			}
		}

		tx.Rollback()

		if pkpos <= lastSuccessfulPrimaryKey {
			err = fmt.Errorf("new pkpos %d <= lastSuccessfulPk %d", pkpos, lastSuccessfulPrimaryKey)
			logger.WithError(err).Error("last successful pk position did not advance")
			return err
		}

		lastSuccessfulPrimaryKey = pkpos
		// The way we save the LastSuccessfulPK is probably incorrect if we
		// want to ensure that when we crash, we have a "correct" view of
		// the LastSuccessfulPK.
		// However, it's uncertain if it is even theoretically possible to
		// save the "correct" value.
		// TODO: investigate this if we want to ensure that on error, we have
		//       the "correct" last successful PK and other values.
		logger.Debugf("updated last successful PK to %d", pkpos)
		this.CurrentState.UpdateLastSuccessfulPK(table.String(), pkpos)
	}

	logger.Info("table copy completed")
	this.CurrentState.MarkTableAsCompleted(table.String())
	return nil
}

func (this *DataIterator) fetchRowsInBatch(tx *sql.Tx, table *schema.Table, pkColumn *schema.TableColumn, lastSuccessfulPk uint64) (events []DMLEvent, pkpos uint64, err error) {
	logger := this.logger.WithFields(logrus.Fields{
		"table": table.String(),
	})

	// This query must be a prepared query. If it is not, querying will use
	// MySQL's plain text interface, which will scan all values into []uint8
	// if we give it []interface{}.
	pkName := quoteField(pkColumn.Name)
	selectBuilder := sq.Select("*").
		From(QuotedTableName(table)).
		Where(sq.Gt{pkName: lastSuccessfulPk}).
		Limit(this.Config.IterateChunksize).
		OrderBy(pkName).
		Suffix("FOR UPDATE")

	if this.Filter != nil {
		var cond sq.Sqlizer
		if cond, err = this.Filter.ConstrainSelect(table, lastSuccessfulPk, this.Config.IterateChunksize); err != nil {
			logger.WithError(err).Error("failed to apply filter for select")
			return
		}
		selectBuilder = selectBuilder.Where(cond)
	}

	query, args, err := selectBuilder.ToSql()
	if err != nil {
		logger.WithError(err).Error("failed to build chunking sql")
		return
	}

	logger = logger.WithFields(logrus.Fields{
		"sql":  query,
		"args": args,
	})

	stmt, err := tx.Prepare(query)
	if err != nil {
		logger.WithError(err).Error("failed to prepare query")
		return
	}

	rows, err := stmt.Query(args...)
	if err != nil {
		logger.WithError(err).Error("failed to query database")
		return
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		logger.WithError(err).Error("failed to get columns")
		return
	}

	var pkIndex int
	for idx, c := range columns {
		if c == pkColumn.Name {
			pkIndex = idx
			break
		}
	}

	var ev DMLEvent
	var values []interface{}
	events = make([]DMLEvent, 0)

	for rows.Next() {
		values, err = ScanGenericRow(rows, len(columns))
		if err != nil {
			logger.WithError(err).Error("failed to scan row")
			return
		}

		ev, err = NewExistingRowEvent(table, values)
		if err != nil {
			logger.WithError(err).Error("failed to create row event")
			return
		}

		events = append(events, ev)

		// Since it is possible to have many different types of integers in
		// MySQL, we try to parse it all into uint64.
		if valueByteSlice, ok := values[pkIndex].([]byte); ok {
			valueString := string(valueByteSlice)
			pkpos, err = strconv.ParseUint(valueString, 10, 64)
			if err != nil {
				logger.WithError(err).Error("failed to convert string primary key value to uint64")
				return
			}
		} else {
			signedPkPos := reflect.ValueOf(values[pkIndex]).Int()
			if signedPkPos < 0 {
				err = fmt.Errorf("primary key %s had value %d in table %s", pkName, signedPkPos, QuotedTableName(table))
				logger.WithError(err).Error("failed to update primary key position")
				return
			}
			pkpos = uint64(signedPkPos)
		}
	}

	logger.Debugf("found %d rows", len(events))

	err = rows.Err()
	return
}

func ScanGenericRow(rows *sql.Rows, columnCount int) ([]interface{}, error) {
	values := make([]interface{}, columnCount)
	valuePtrs := make([]interface{}, columnCount)

	for i, _ := range values {
		valuePtrs[i] = &values[i]
	}

	err := rows.Scan(valuePtrs...)
	return values, err
}
