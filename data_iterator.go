package ghostferry

import (
	"database/sql"
	"fmt"
	"reflect"
	"sync"

	sq "github.com/Masterminds/squirrel"
	"github.com/siddontang/go-mysql/schema"
	"github.com/sirupsen/logrus"
)

func NoSelectFilter(s sq.SelectBuilder) sq.SelectBuilder {
	return s
}

type DataIteratorState struct {
	// We need to lock these maps as Go does not support concurrent access to
	// maps.
	// Maybe able to convert to sync.Map in Go1.9
	targetPrimaryKeys         map[string]int64
	lastSuccessfulPrimaryKeys map[string]int64
	completedTables           map[string]bool

	targetPkMutex     *sync.RWMutex
	successfulPkMutex *sync.RWMutex
	tablesMutex       *sync.RWMutex
}

func newDataIteratorState() *DataIteratorState {
	return &DataIteratorState{
		targetPrimaryKeys:         make(map[string]int64),
		lastSuccessfulPrimaryKeys: make(map[string]int64),
		completedTables:           make(map[string]bool),
		targetPkMutex:             &sync.RWMutex{},
		successfulPkMutex:         &sync.RWMutex{},
		tablesMutex:               &sync.RWMutex{},
	}
}

func (this *DataIteratorState) UpdateTargetPK(table string, pk int64) {
	this.targetPkMutex.Lock()
	defer this.targetPkMutex.Unlock()

	this.targetPrimaryKeys[table] = pk
}

func (this *DataIteratorState) UpdateLastSuccessfulPK(table string, pk int64) {
	this.successfulPkMutex.Lock()
	defer this.successfulPkMutex.Unlock()

	this.lastSuccessfulPrimaryKeys[table] = pk
}

func (this *DataIteratorState) MarkTableAsCompleted(table string) {
	this.tablesMutex.Lock()
	defer this.tablesMutex.Unlock()

	this.completedTables[table] = true
}

func (this *DataIteratorState) TargetPrimaryKeys() map[string]int64 {
	this.targetPkMutex.RLock()
	defer this.targetPkMutex.RUnlock()

	m := make(map[string]int64)
	for k, v := range this.targetPrimaryKeys {
		m[k] = v
	}

	return m
}

func (this *DataIteratorState) LastSuccessfulPrimaryKeys() map[string]int64 {
	this.successfulPkMutex.RLock()
	defer this.successfulPkMutex.RUnlock()

	m := make(map[string]int64)
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

type DataIterator struct {
	Db           *sql.DB
	Config       *Config
	ErrorHandler *ErrorHandler
	Throttler    *Throttler

	Tables       []*schema.Table
	SelectFilter func(sq.SelectBuilder) sq.SelectBuilder

	CurrentState *DataIteratorState

	tableCh        chan *schema.Table
	eventListeners []func([]DMLEvent) error
	doneListeners  []func() error

	logger *logrus.Entry
	wg     *sync.WaitGroup
}

func (this *DataIterator) Initialize() error {
	if this.SelectFilter == nil {
		this.SelectFilter = NoSelectFilter
	}

	this.tableCh = make(chan *schema.Table)
	this.logger = logrus.WithField("tag", "data_iterator")
	this.wg = &sync.WaitGroup{}

	this.CurrentState = newDataIteratorState()

	return nil
}

func (this *DataIterator) Run(wg *sync.WaitGroup) {
	defer func() {
		this.logger.Info("data iterator done")
		wg.Done()
	}()

	this.logger.WithField("tablesCount", len(this.Tables)).Info("starting data iterator run")

	tablesWithData, err := this.determineMinMaxPKsForAllTables()
	if err != nil {
		this.ErrorHandler.Fatal("data_iterator", err)
		return
	}

	this.wg.Add(this.Config.NumberOfTableIterators)
	for i := 0; i < this.Config.NumberOfTableIterators; i++ {
		go this.runTableIterator(uint32(i))
	}

	for _, table := range tablesWithData {
		this.tableCh <- table
	}

	this.logger.Info("done queueing tables to be iterated, closing table channel")

	close(this.tableCh)

	this.wg.Wait()

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

func (this *DataIterator) determineMinMaxPKsForAllTables() ([]*schema.Table, error) {
	tablesWithData := make([]*schema.Table, 0, len(this.Tables))
	for _, table := range this.Tables {
		logger := this.logger.WithField("table", table.String())

		rows, err := this.Db.Query(fmt.Sprintf("SELECT 1 FROM %s", quotedTableName(table)))
		if err != nil {
			logger.WithError(err).Error("failed to see if rows exist in table")
			return tablesWithData, err
		}

		if !rows.Next() {
			rows.Close()
			logger.Warn("no data in this table, skipping")
			this.CurrentState.MarkTableAsCompleted(table.String())
			continue
		}

		rows.Close()

		primaryKeyColumn := table.GetPKColumn(0)
		pkName := quoteField(primaryKeyColumn.Name)
		logger.Infof("getting min/max for primary key %s", pkName)
		query, args, err := sq.Select(fmt.Sprintf("MIN(%s), MAX(%s)", pkName, pkName)).From(quotedTableName(table)).ToSql()
		if err != nil {
			logger.WithError(err).Errorf("failed to build query to get min/max primary key %s", pkName)
			return tablesWithData, err
		}

		row := this.Db.QueryRow(query, args...)

		var minPrimaryKey, maxPrimaryKey int64
		err = row.Scan(&minPrimaryKey, &maxPrimaryKey)
		if err != nil {
			logger.WithError(err).Errorf("failed to get min/max primary key %s", primaryKeyColumn.Name)
			return tablesWithData, err
		}

		logger.Infof("min/max for %s: %d %d (%s)", pkName, minPrimaryKey, maxPrimaryKey, primaryKeyColumn.RawType)
		this.CurrentState.UpdateTargetPK(table.String(), maxPrimaryKey)

		// The last successful primary key is not the minPrimaryKey, as that
		// would indicate the row associated with minPrimaryKey has been
		// copied. Thus, we subtract one from the minPrimaryKey.
		lastSuccessfulPrimaryKey := minPrimaryKey - 1
		this.CurrentState.UpdateLastSuccessfulPK(table.String(), lastSuccessfulPrimaryKey)

		tablesWithData = append(tablesWithData, table)
	}

	return tablesWithData, nil
}

func (this *DataIterator) runTableIterator(i uint32) {
	defer func() {
		this.logger.Infof("table iterator %d done", i)
		this.wg.Done()
	}()

	this.logger.Infof("starting table iterator instance %d", i)

	for {
		table, ok := <-this.tableCh
		if !ok {
			break
		}

		err := this.iterateTable(table)
		if err != nil {
			this.logger.WithFields(logrus.Fields{
				"error": err,
				"i":     i,
				"table": table.String(),
			}).Error("failed to iterate table")
			this.ErrorHandler.Fatal("table_iterator", err)
			return
		}
	}
}

func (this *DataIterator) iterateTable(table *schema.Table) error {
	logger := this.logger.WithField("table", table.String())
	logger.Info("starting to iterate over table")

	lastSuccessfulPrimaryKey := this.CurrentState.LastSuccessfulPrimaryKeys()[table.String()]
	maxPrimaryKey := this.CurrentState.TargetPrimaryKeys()[table.String()]

	for lastSuccessfulPrimaryKey < maxPrimaryKey {
		var tx *sql.Tx
		var err error
		var rowEvents []DMLEvent
		var pkpos int64

		for i := 0; i < this.Config.MaxIterationReadRetries; i++ {
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
				logger.WithError(err).Error("failed to start database transaction, retrying if limit is not exceeded")
				continue
			}

			rowEvents, pkpos, err = this.fetchRowsInBatch(tx, table, table.GetPKColumn(0), lastSuccessfulPrimaryKey)
			if err == nil {
				break
			}

			tx.Rollback()
			logger.WithError(err).Error("failed to fetch rows, retrying if limit not exceeded")
		}

		if err != nil {
			logger.WithError(err).Error("failed to fetch rows, retry limit exceeded")
			return err
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

func (this *DataIterator) fetchRowsInBatch(tx *sql.Tx, table *schema.Table, pkColumn *schema.TableColumn, lastSuccessfulPk int64) (events []DMLEvent, pkpos int64, err error) {
	logger := this.logger.WithFields(logrus.Fields{
		"table": table.String(),
	})

	// This query must be a prepared query. If it is not, querying will use
	// MySQL's plain text interface, which will scan all values into []uint8
	// if we give it []interface{}.
	// Right now the sq.GtOrEq forces this query to be a prepared one.
	pkName := quoteField(pkColumn.Name)
	selectBuilder := sq.Select("*").
		From(quotedTableName(table)).
		Where(sq.Gt{pkName: lastSuccessfulPk}).
		Limit(this.Config.IterateChunksize).
		OrderBy(pkName).
		Suffix("FOR UPDATE")

	selectBuilder = this.SelectFilter(selectBuilder)

	query, args, err := selectBuilder.ToSql()
	if err != nil {
		logger.WithError(err).Error("failed to build chunking sql")
		return
	}

	logger = logger.WithFields(logrus.Fields{
		"sql":  query,
		"args": args,
	})

	rows, err := tx.Query(query, args...)
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

	events = make([]DMLEvent, 0)

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i, _ := range values {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			logger.WithError(err).Error("failed to scan row")
			return
		}

		ev := &ExistingRowEvent{
			database: table.Schema,
			table:    table.Name,
			values:   values,
		}

		events = append(events, ev)

		// Since it is possible to have many different types of integers in
		// MySQL, we try to parse it all into int64.
		//
		// TODO: there is a theoretical overflow here, but I'm not sure if
		//       we will hit it during any real use case.
		if pkColumn.IsUnsigned {
			pkpos = int64(reflect.ValueOf(values[pkIndex]).Uint())
		} else {
			pkpos = reflect.ValueOf(values[pkIndex]).Int()
		}
	}

	logger.Debugf("found %d rows", len(events))

	return
}
