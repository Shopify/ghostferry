package ghostferry

import (
	"database/sql"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const defaultThrottleDuration = 3 * time.Second

type Throttler struct {
	Db           *sql.DB
	Config       *Config
	ErrorHandler *ErrorHandler

	CurrentVariableLoad   map[string]int64
	CurrentReplicationLag int64

	logger *logrus.Entry
	stopCh chan struct{}

	throttleMutex *sync.RWMutex
	throttleUntil time.Time
}

func (this *Throttler) Initialize() error {
	this.logger = logrus.WithField("tag", "throttler")
	this.CurrentVariableLoad = make(map[string]int64)
	this.stopCh = make(chan struct{})

	this.throttleMutex = &sync.RWMutex{}
	return nil
}

func (this *Throttler) Run(wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			err := this.collectVariableLoad()
			if err != nil {
				this.ErrorHandler.Fatal("throttler", err)
				return
			}

			this.checkVariableLoadToSetThrottle()
		case <-this.stopCh:
			return
		}
	}
}

func (this *Throttler) Stop() {
	close(this.stopCh)
}

func (this *Throttler) ThrottleIfNecessary() {
	for {
		if !this.shouldThrottle() {
			return
		}

		time.Sleep(500 * time.Millisecond)
	}
}

// Add to the throttle already set. If the throttle is not in effect,
// set it.
func (this *Throttler) AddThrottleDuration(d time.Duration) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	now := time.Now()
	if now.After(this.throttleUntil) {
		this.throttleUntil = now
	}

	this.throttleUntil = this.throttleUntil.Add(d)
	this.logger.WithField("throttleUntil", this.throttleUntil).Debug("added to throttle")
}

// Set a throttle. If a throttle is set, don't override it unless it expires
// before the desired duration is over.
func (this *Throttler) SetThrottleDuration(d time.Duration) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	desiredThrottleUntil := time.Now().Add(d)
	if desiredThrottleUntil.After(this.throttleUntil) {
		this.throttleUntil = desiredThrottleUntil
		this.logger.WithField("throttleUntil", this.throttleUntil).Debug("setting throttle")
	}
}

func (this *Throttler) Pause() {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	// 100 years of throttling should be enough, right?
	this.throttleUntil = time.Now().Add(100 * 365 * 24 * time.Hour)
}

func (this *Throttler) Unpause() {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()

	this.throttleUntil = time.Now().Add(-1 * time.Hour)
}

// Used for reporting purposes rather than actual throttling.
// Reading from this is not locked with the mutex.
func (this *Throttler) ThrottleUntil() time.Time {
	return this.throttleUntil
}

func (this *Throttler) shouldThrottle() bool {
	this.throttleMutex.RLock()
	defer this.throttleMutex.RUnlock()

	return this.throttleUntil.After(time.Now())
}

func (this *Throttler) collectVariableLoad() error {
	if len(this.Config.MaxVariableLoad) == 0 {
		return nil
	}

	// TODO: actually collect that variable loads
	return nil
}

func (this *Throttler) checkVariableLoadToSetThrottle() bool {
	for variableName, value := range this.CurrentVariableLoad {
		if value >= this.Config.MaxVariableLoad[variableName] {
			this.logger.WithFields(logrus.Fields{
				"variable":      variableName,
				"value":         value,
				"criticalValue": this.Config.MaxVariableLoad[variableName],
			}).Info("variable exceeded maximum, setting throttle")
			this.SetThrottleDuration(defaultThrottleDuration)
			return true
		}
	}

	return false
}
