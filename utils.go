package ghostferry

import (
	"context"
	"crypto/rand"
	"encoding/binary"
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
