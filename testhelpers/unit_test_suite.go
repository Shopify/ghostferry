package testhelpers

import (
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
)

func SetupTest() {
	var err error

	logrus.SetLevel(logrus.DebugLevel)

	seed := time.Now().UnixNano()
	envseed := os.Getenv("SEED")
	if envseed != "" {
		seed, err = strconv.ParseInt(envseed, 10, 64)
		PanicIfError(err)
	}

	logrus.Warnf("random seed: %d", seed)
	rand.Seed(seed)
}
