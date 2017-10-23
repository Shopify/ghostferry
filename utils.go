package ghostferry

import (
	"crypto/rand"
	"encoding/binary"
	"math/big"
	"os"
	"time"

	"github.com/sirupsen/logrus"
)

func WithRetries(maxRetries int, sleep time.Duration, logger *logrus.Entry, verb string, f func() error) (err error) {
	try := 1

	for {
		err = f()
		if err == nil {
			return
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
	return binary.LittleEndian.Uint32(append(randomUint24(), pidToUint8()))
}

func randomUint24() []byte {
	num, err := rand.Int(rand.Reader, big.NewInt(1<<24))
	if err != nil {
		panic("could not generate random number")
	}

	return num.Bytes()
}

func pidToUint8() byte {
	pid := uint32(os.Getpid())

	res := byte(pid)
	res ^= byte(pid >> 8)
	res ^= byte(pid >> 16)
	res ^= byte(pid >> 24)

	return res
}
