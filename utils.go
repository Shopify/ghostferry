package ghostferry

import (
	"context"
	"crypto/rand"
	sqlorig "database/sql"
	"encoding/binary"
	"fmt"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"sync"
	"sync/atomic"
	"time"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

func WithRetries(maxRetries int, sleep time.Duration, logger *logrus.Entry, verb string, f func() error) (err error) {
	return WithRetriesContext(nil, maxRetries, sleep, logger, verb, f)
}

func WithRetriesContext(ctx context.Context, maxRetries int, sleep time.Duration, logger *logrus.Entry, verb string, f func() error) (err error) {
	try := 1

	if logger == nil {
		logger = logrus.NewEntry(logrus.StandardLogger())
	}

	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		err = f()
		if err == nil || err == context.Canceled {
			return err
		}

		if maxRetries != 0 && try >= maxRetries {
			break
		}

		logger.WithError(err).Errorf("failed to %s, %d of %d max retries", verb, try, maxRetries)

		try++
		time.Sleep(sleep)
	}

	logger.WithError(err).Errorf("failed to %s after %d attempts, retry limit exceeded", verb, try)

	return
}

func randomServerId() uint32 {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(err)
	}

	return binary.LittleEndian.Uint32(buf[:])
}

type AtomicBoolean int32

func (a *AtomicBoolean) Set(b bool) {
	var v int32 = 0
	if b {
		v = 1
	}

	atomic.StoreInt32((*int32)(a), v)
}

func (a *AtomicBoolean) Get() bool {
	return atomic.LoadInt32((*int32)(a)) == int32(1)
}

type WorkerPool struct {
	Concurrency int
	Process     func(int) (interface{}, error)
}

// Returns a list of results of the size same as the concurrency number.
// Returns the first error that occurs during the run. Also as soon as
// a single worker errors, all workers terminates.
func (p *WorkerPool) Run(n int) ([]interface{}, error) {
	results := make([]interface{}, p.Concurrency)
	errCh := make(chan error, p.Concurrency)
	workQueue := make(chan int)

	wg := &sync.WaitGroup{}
	wg.Add(p.Concurrency)

	for j := 0; j < p.Concurrency; j++ {
		go func(j int) {
			defer wg.Done()

			for workIndex := range workQueue {
				result, err := p.Process(workIndex)
				results[j] = result
				if err != nil {
					errCh <- err
					return
				}
			}

			errCh <- nil
		}(j)
	}

	var err error = nil
	i := 0
loop:
	for i < n {
		select {
		case workQueue <- i:
			i++
		case err = <-errCh: // abort pool if an error was discovered
			if err != nil {
				break loop
			}
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}

	close(workQueue)
	wg.Wait()
	close(errCh)

	if err != nil {
		return results, err
	}

	for e := range errCh {
		if e != nil {
			err = e
		}
	}
	return results, err
}

type StmtCache struct {
	mut        sync.RWMutex
	statements map[string]*sqlorig.Stmt
}

func NewStmtCache() *StmtCache {
	return &StmtCache{
		statements: make(map[string]*sqlorig.Stmt),
	}
}

func (c *StmtCache) StmtFor(p SqlPreparer, query string) (*sqlorig.Stmt, error) {
	stmt, exists := c.getStmt(query)
	if !exists {
		return c.newStmtFor(p, query)
	}
	return stmt, nil
}

func (c *StmtCache) newStmtFor(p SqlPreparer, query string) (*sqlorig.Stmt, error) {
	stmt, err := p.Prepare(query)
	if err != nil {
		return nil, err
	}

	c.storeStmt(query, stmt)
	return stmt, nil
}

func (c *StmtCache) storeStmt(query string, stmt *sqlorig.Stmt) {
	c.mut.Lock()
	defer c.mut.Unlock()
	c.statements[query] = stmt
}

func (c *StmtCache) getStmt(query string) (*sqlorig.Stmt, bool) {
	c.mut.RLock()
	defer c.mut.RUnlock()
	stmt, exists := c.statements[query]
	return stmt, exists
}

func ShowMasterStatusBinlogPosition(db *sql.DB) (mysql.Position, error) {
	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		return NewMysqlPosition("", 0, err)
	}
	defer rows.Close()
	var file string
	var position uint32
	var binlog_do_db, binlog_ignore_db, executed_gtid_set string
	var cols []string
	if rows.Next() {
		cols, err = rows.Columns()
		if err != nil {
			return NewMysqlPosition(file, position, err)
		}
		switch len(cols) {
		case 4:
			err = rows.Scan(&file, &position, &binlog_do_db, &binlog_ignore_db)
		default:
			err = rows.Scan(&file, &position, &binlog_do_db, &binlog_ignore_db, &executed_gtid_set)
		}
	}
	return NewMysqlPosition(file, position, err)
}

func NewMysqlPosition(file string, position uint32, err error) (mysql.Position, error) {
	switch {
	case err == sqlorig.ErrNoRows:
		return mysql.Position{}, fmt.Errorf("no results from show master status")
	case err != nil:
		return mysql.Position{}, err
	default:
		if file == "" {
			return mysql.Position{}, fmt.Errorf("show master status does not show a file")
		}

		return mysql.Position{
			Name: file,
			Pos:  position,
		}, nil
	}
}

func CheckDbIsAReplica(db *sql.DB) (bool, error) {
	row := db.QueryRow("SELECT @@read_only")
	var isReadOnly bool
	err := row.Scan(&isReadOnly)
	return isReadOnly, err
}
