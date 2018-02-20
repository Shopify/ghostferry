package ghostferry

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"

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
		err = f()
		if err == nil || err == context.Canceled {
			return err
		}

		if maxRetries != 0 && try >= maxRetries {
			break
		}

		logger.WithError(err).Errorf("failed to %s, %d of %d max retries", verb, try, maxRetries)

		try++
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleep):
			}
		} else {
			time.Sleep(sleep)
		}
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

// A worker pool that exits immediately if an error occurs
type WorkerPool struct {
	Concurrency int
	Process     func(interface{}) (interface{}, error)
	LogTag      string
}

func (c *WorkerPool) Run(allwork []interface{}) ([]interface{}, []error) {
	if c.LogTag == "" {
		c.LogTag = "worker_pool"
	}

	logger := logrus.WithField("tag", c.LogTag)
	results := make([]interface{}, c.Concurrency)
	errArray := make([]error, c.Concurrency)
	queue := make(chan interface{})

	incomplete := errors.New("incomplete")
	for j := 0; j < c.Concurrency; j++ {
		errArray[j] = incomplete
	}

	wg := &sync.WaitGroup{}
	wg.Add(c.Concurrency)
	for j := 0; j < c.Concurrency; j++ {
		go func(j int) {
			defer wg.Done()
			localLog := logger.WithField("worker_idx", j)

			for {
				work, open := <-queue
				if !open {
					localLog.Info("queue closed, exiting...")
					break
				}

				result, err := c.Process(work)
				results[j] = result
				if err != nil {
					localLog.WithError(err).Errorf("error during processing of %v", work)
					errArray[j] = err
					return
				}
			}

			errArray[j] = nil
		}(j)
	}

	i := 0
loop:
	for i < len(allwork) {
		select {
		case queue <- allwork[i]:
			logger.Debugf("queued work number %d", i)
			i++
		case <-time.After(500 * time.Millisecond):
			logger.Debugf("can't queue for 500ms, checking of errors")
		}

		for j := 0; j < c.Concurrency; j++ {
			err := errArray[j]
			if err != nil && err != incomplete {
				// Only need a debug print because .Error is called within the
				// processing goroutine.
				logger.WithError(err).Debug("error detected, stopping additional processing")
				break loop
			}
		}
	}
	close(queue)
	wg.Wait()

	return results, errArray
}
