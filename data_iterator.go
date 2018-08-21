package ghostferry

import (
	"container/ring"
	"database/sql"
	"sync"
	"time"

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
	DB          *sql.DB
	Tables      []*schema.Table
	Concurrency int

	ErrorHandler ErrorHandler
	CursorConfig *CursorConfig

	CurrentState *DataIteratorState

	batchListeners []func(*RowBatch) error
	doneListeners  []func() error
	logger         *logrus.Entry
}

func (d *DataIterator) Initialize() error {
	d.logger = logrus.WithField("tag", "data_iterator")
	d.CurrentState = newDataIteratorState(d.Concurrency)

	return nil
}

func (d *DataIterator) Run() {
	d.logger.WithField("tablesCount", len(d.Tables)).Info("starting data iterator run")

	tablesWithData, emptyTables, err := MaxPrimaryKeys(d.DB, d.Tables, d.logger)
	if err != nil {
		d.ErrorHandler.Fatal("data_iterator", err)
	}

	for _, table := range emptyTables {
		d.CurrentState.MarkTableAsCompleted(table.String())
	}

	for table, maxPk := range tablesWithData {
		d.CurrentState.UpdateTargetPK(table.String(), maxPk)
	}

	tablesQueue := make(chan *schema.Table)
	wg := &sync.WaitGroup{}
	wg.Add(d.Concurrency)

	for i := 0; i < d.Concurrency; i++ {
		go func() {
			defer wg.Done()

			for {
				table, ok := <-tablesQueue
				if !ok {
					break
				}

				logger := d.logger.WithField("table", table.String())

				cursor := d.CursorConfig.NewCursor(table, d.CurrentState.TargetPrimaryKeys()[table.String()])
				err := cursor.Each(func(batch *RowBatch) error {
					metrics.Count("RowEvent", int64(batch.Size()), []MetricTag{
						MetricTag{"table", table.Name},
						MetricTag{"source", "table"},
					}, 1.0)

					for _, listener := range d.batchListeners {
						err := listener(batch)
						if err != nil {
							logger.WithError(err).Error("failed to process row batch with listeners")
							return err
						}
					}

					// The way we save the LastSuccessfulPK is probably incorrect if we
					// want to ensure that when we crash, we have a "correct" view of
					// the LastSuccessfulPK.
					// However, it's uncertain if it is even theoretically possible to
					// save the "correct" value.
					// TODO: investigate this if we want to ensure that on error, we have
					//       the "correct" last successful PK and other values.
					// TODO: it is also perhaps possible to save the Cursor objects
					// directly as opposed to saving a state, but that is left to
					// the future.
					lastRow := batch.Values()[len(batch.Values())-1]
					pkpos, err := lastRow.GetUint64(batch.PkIndex())
					if err != nil {
						logger.WithError(err).Error("failed to convert pk to uint64")
						return err
					}

					logger.Debugf("updated last successful PK to %d", pkpos)
					d.CurrentState.UpdateLastSuccessfulPK(table.String(), pkpos)

					return nil
				})

				if err != nil {
					logger.WithError(err).Error("failed to iterate table")
					d.ErrorHandler.Fatal("data_iterator", err)
				}

				logger.Debug("table iteration completed")
				d.CurrentState.MarkTableAsCompleted(table.String())
			}
		}()
	}

	for table, _ := range tablesWithData {
		tablesQueue <- table
	}

	d.logger.Info("done queueing tables to be iterated, closing table channel")
	close(tablesQueue)

	wg.Wait()
	for _, listener := range d.doneListeners {
		listener()
	}
}

func (d *DataIterator) AddBatchListener(listener func(*RowBatch) error) {
	d.batchListeners = append(d.batchListeners, listener)
}

func (d *DataIterator) AddDoneListener(listener func() error) {
	d.doneListeners = append(d.doneListeners, listener)
}
