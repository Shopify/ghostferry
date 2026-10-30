package ghostferry

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	// How often the copy throttle re-reads the throttler while copy batches
	// wait for admission. Matches WaitForThrottle.
	copyThrottlePollInterval = 500 * time.Millisecond

	// The first batches copied after a throttle clears use this fraction of
	// the configured batch size.
	copyThrottleRecoveryDivisor = 8
)

// copyThrottleSampler is implemented by throttlers that can tell when their
// throttling signal was last refreshed. Copy recovery only increases the copy
// load after a fresh sample was observed during the stage.
type copyThrottleSampler interface {
	recoverySample() (sequence uint64, observedAt time.Time, interval time.Duration)
}

// copyThrottlePermit admits one copy batch: the source read, the batch
// listeners and the source rollback.
type copyThrottlePermit struct {
	generation uint64
	divisor    uint64
}

// batchSize scales the configured batch size for this permit, copying at
// least one row.
func (p copyThrottlePermit) batchSize(configuredRows uint64) uint64 {
	rows := configuredRows / p.divisor
	if rows == 0 {
		return 1
	}
	return rows
}

// copyThrottleController admits the copy batches of one DataIterator.Run.
//
// While the throttler is throttled, no batch is admitted. Once the throttle
// clears, the controller admits one batch at a time with an eighth of the
// configured batch size. After every healthy recovery interval it first
// doubles the batch size until the configured size is reached, then admits
// one more concurrent batch until the configured concurrency is reached.
// Batches are admitted in FIFO order, so every table keeps making progress
// while concurrency is reduced.
//
// Admission happens before the source transaction is opened, so a waiting
// batch never holds source row locks.
type copyThrottleController struct {
	throttler      Throttler
	sampler        copyThrottleSampler
	maxConcurrency int
	interval       time.Duration
	now            func() time.Time
	logger         Logger

	mu     sync.Mutex
	cond   *sync.Cond
	closed bool

	// FIFO admission: nextTicket is handed to the next caller of acquire and
	// servingTicket is the ticket allowed to be admitted next.
	nextTicket    uint64
	servingTicket uint64
	inFlight      int

	// disabled and throttled are the throttler state at the last observation.
	disabled   bool
	throttled  bool
	recovering bool
	limit      int
	divisor    uint64

	// generation changes whenever the limits change or the stage restarts.
	// Only batches admitted in the current generation count towards growth.
	generation uint64

	// The current recovery stage starts when its first batch is admitted.
	stageStarted   bool
	stageStart     time.Time
	stageSequence  uint64
	stageSucceeded bool
}

func newCopyThrottleController(throttler Throttler, maxConcurrency int, config DataIterationThrottleRecoveryConfig) (*copyThrottleController, error) {
	if maxConcurrency <= 0 {
		return nil, fmt.Errorf("DataIterationConcurrency must be greater than zero")
	}

	interval, err := config.recoveryInterval()
	if err != nil {
		return nil, err
	}

	c := &copyThrottleController{
		throttler:      throttler,
		maxConcurrency: maxConcurrency,
		interval:       interval,
		now:            time.Now,
		logger:         LogWithField("tag", "copy_throttle"),
		limit:          maxConcurrency,
		divisor:        1,
	}
	c.sampler, _ = throttler.(copyThrottleSampler)
	c.cond = sync.NewCond(&c.mu)
	return c, nil
}

// acquire blocks until a copy batch may start. It returns context.Canceled
// once the controller is closed.
func (c *copyThrottleController) acquire() (copyThrottlePermit, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ticket := c.nextTicket
	c.nextTicket++

	var now time.Time
	for {
		if c.closed {
			return copyThrottlePermit{}, context.Canceled
		}

		now = c.now()
		if c.observeLocked(now) {
			c.cond.Broadcast()
		}

		if ticket == c.servingTicket && c.hasCapacityLocked() {
			break
		}

		c.cond.Wait()
	}

	c.servingTicket++
	c.inFlight++
	if c.recovering && !c.stageStarted {
		c.startStageLocked(now)
	}

	if c.hasCapacityLocked() {
		// The next ticket may be admitted as well.
		c.cond.Broadcast()
	}

	return copyThrottlePermit{generation: c.generation, divisor: c.divisor}, nil
}

// release returns a permit after the batch's source transaction was rolled
// back. copied reports a non-empty batch whose listeners succeeded; err
// reports a failed read or listener.
func (c *copyThrottleController) release(permit copyThrottlePermit, copied bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.inFlight--

	if c.recovering {
		if err != nil {
			// Restart the stage interval without changing the limits.
			c.generation++
			c.resetStageLocked()
		} else if copied && permit.generation == c.generation && c.stageStarted {
			c.stageSucceeded = true
		}
	}

	c.observeLocked(c.now())
	c.cond.Broadcast()
}

// run re-reads the throttler every copyThrottlePollInterval until ctx is
// done, then closes the controller.
func (c *copyThrottleController) run(ctx context.Context) {
	ticker := time.NewTicker(copyThrottlePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.mu.Lock()
			c.closed = true
			c.cond.Broadcast()
			c.mu.Unlock()
			return
		case <-ticker.C:
			c.observe(c.now())
		}
	}
}

// observe re-reads the throttler and wakes waiting batches if that changed
// the admission limits.
func (c *copyThrottleController) observe(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.observeLocked(now) {
		c.cond.Broadcast()
	}
}

func (c *copyThrottleController) hasCapacityLocked() bool {
	return !c.throttled && c.inFlight < c.limit
}

// observeLocked applies the current throttler state and advances the
// recovery stage when it is healthy. It reports whether the limits changed.
func (c *copyThrottleController) observeLocked(now time.Time) bool {
	if c.throttler.Disabled() {
		if c.disabled {
			return false
		}

		c.disabled = true
		c.throttled = false
		if c.recovering {
			c.logger.Info("throttler disabled, restoring copy concurrency and batch size")
		}
		c.setLimitsLocked(false, c.maxConcurrency, 1)
		return true
	}

	changed := c.disabled
	c.disabled = false

	if c.throttler.Throttled() {
		if !c.throttled {
			c.throttled = true
			c.logger.Info("throttled, pausing new copy batches")
			c.setLimitsLocked(true, 1, copyThrottleRecoveryDivisor)
			changed = true
		}
		return changed
	}

	if c.throttled {
		c.throttled = false
		c.logger.Infof("throttle cleared, resuming copy with %d concurrent batch of 1/%d batch size", c.limit, c.divisor)
		changed = true
	}

	if c.recovering && c.advanceLocked(now) {
		changed = true
	}

	return changed
}

// advanceLocked increases either the batch size or the concurrency by one
// step if the current stage has been healthy for the recovery interval.
func (c *copyThrottleController) advanceLocked(now time.Time) bool {
	if !c.stageStarted || !c.stageSucceeded || now.Sub(c.stageStart) < c.interval {
		return false
	}

	if c.sampler != nil {
		sequence, observedAt, sampleInterval := c.sampler.recoverySample()
		if sequence <= c.stageSequence || now.Sub(observedAt) > 2*sampleInterval {
			return false
		}
	}

	limit, divisor := c.limit, c.divisor
	if divisor > 1 {
		divisor /= 2
	} else {
		limit++
	}

	recovering := divisor > 1 || limit < c.maxConcurrency
	c.setLimitsLocked(recovering, limit, divisor)

	if recovering {
		c.logger.Infof("copy recovery: %d concurrent batch(es) of 1/%d batch size", c.limit, c.divisor)
	} else {
		c.logger.Info("copy recovery complete")
	}
	return true
}

func (c *copyThrottleController) setLimitsLocked(recovering bool, limit int, divisor uint64) {
	c.recovering = recovering
	c.limit = limit
	c.divisor = divisor
	c.generation++
	c.resetStageLocked()
}

func (c *copyThrottleController) startStageLocked(now time.Time) {
	c.stageStarted = true
	c.stageStart = now
	c.stageSucceeded = false
	if c.sampler != nil {
		c.stageSequence, _, _ = c.sampler.recoverySample()
	}
}

func (c *copyThrottleController) resetStageLocked() {
	c.stageStarted = false
	c.stageSucceeded = false
}
