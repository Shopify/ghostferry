package ghostferry

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/require"
)

const testCopyThrottleInterval = 5 * time.Second

type fakeCopyThrottleClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeCopyThrottleClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeCopyThrottleClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeSampledThrottler is a PauserThrottler that reports lag samples like
// the LagThrottler.
type fakeSampledThrottler struct {
	PauserThrottler

	mu         sync.Mutex
	sequence   uint64
	observedAt time.Time
	interval   time.Duration
}

func (t *fakeSampledThrottler) recoverySample() (uint64, time.Time, time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sequence, t.observedAt, t.interval
}

func (t *fakeSampledThrottler) sample(observedAt time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sequence++
	t.observedAt = observedAt
}

type copyRelease struct {
	copied bool
	err    error
}

type copyAdmission struct {
	contender int
	permit    copyThrottlePermit
	release   chan<- copyRelease
	released  <-chan struct{}
}

// finish releases the permit and returns once the controller processed it.
func (a copyAdmission) finish(result copyRelease) {
	a.release <- result
	<-a.released
}

func (a copyAdmission) succeed()                { a.finish(copyRelease{copied: true}) }
func (a copyAdmission) finishEmpty()            { a.finish(copyRelease{}) }
func (a copyAdmission) fail(err error)          { a.finish(copyRelease{err: err}) }
func (a copyAdmission) rows(base uint64) uint64 { return a.permit.batchSize(base) }

// copyThrottleHarness runs contenders that, like cursors, acquire a permit,
// wait for the test to finish the batch, release it and acquire again.
type copyThrottleHarness struct {
	t          *testing.T
	c          *copyThrottleController
	clock      *fakeCopyThrottleClock
	contenders int
	releases   []chan copyRelease
	admitted   chan copyAdmission
	stopped    chan error
	wg         sync.WaitGroup
}

func newCopyThrottleHarness(t *testing.T, throttler Throttler, maxConcurrency int) *copyThrottleHarness {
	c, err := newCopyThrottleController(throttler, maxConcurrency, DataIterationThrottleRecoveryConfig{RecoveryInterval: "5s"})
	require.NoError(t, err)

	clock := &fakeCopyThrottleClock{now: time.Unix(1000000, 0)}
	c.now = clock.Now
	c.observe(clock.Now())

	h := &copyThrottleHarness{
		t:        t,
		c:        c,
		clock:    clock,
		admitted: make(chan copyAdmission, 16),
		stopped:  make(chan error, 16),
	}
	t.Cleanup(h.close)
	return h
}

func (h *copyThrottleHarness) startContender() {
	id := h.contenders
	h.contenders++

	release := make(chan copyRelease)
	released := make(chan struct{}, 1)
	h.releases = append(h.releases, release)
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		for {
			permit, err := h.c.acquire()
			if err != nil {
				h.stopped <- err
				return
			}
			h.admitted <- copyAdmission{contender: id, permit: permit, release: release, released: released}
			result := <-release
			h.c.release(permit, result.copied, result.err)
			released <- struct{}{}
		}
	}()
}

// startContenders starts n contenders and waits until each holds a permit or
// a queue position, so tickets follow contender order.
func (h *copyThrottleHarness) startContenders(n int) {
	for range n {
		h.startContender()
		h.waitForContenders()
	}
}

func (h *copyThrottleHarness) state() (inFlight int, waiting int) {
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	return h.c.inFlight, int(h.c.nextTicket - h.c.servingTicket)
}

// waitForContenders waits until every contender holds a permit or waits for
// admission.
func (h *copyThrottleHarness) waitForContenders() {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		inFlight, waiting := h.state()
		if inFlight+waiting == h.contenders {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("contenders did not settle: %d in flight, %d waiting, %d contenders", inFlight, waiting, h.contenders)
		}
		runtime.Gosched()
	}
}

// requireSettled asserts exactly inFlight batches are admitted while the other
// contenders wait.
func (h *copyThrottleHarness) requireSettled(inFlight int) {
	h.t.Helper()
	h.waitForContenders()
	gotInFlight, gotWaiting := h.state()
	require.Equal(h.t, inFlight, gotInFlight, "admitted batches")
	require.Equal(h.t, h.contenders-inFlight, gotWaiting, "waiting batches")
	require.Empty(h.t, h.admitted, "unexpected admission")
}

func (h *copyThrottleHarness) admit(n int) []copyAdmission {
	h.t.Helper()
	admissions := make([]copyAdmission, 0, n)
	for range n {
		select {
		case a := <-h.admitted:
			admissions = append(admissions, a)
		case <-time.After(5 * time.Second):
			h.t.Fatalf("expected %d admissions, got %d", n, len(admissions))
		}
	}
	return admissions
}

// admitStage receives limit admissions with the expected batch size and
// asserts no further batch was admitted.
func (h *copyThrottleHarness) admitStage(limit int, rows uint64) []copyAdmission {
	h.t.Helper()
	admissions := h.admit(limit)
	for _, a := range admissions {
		require.Equal(h.t, rows, a.rows(200))
	}
	h.requireSettled(limit)
	return admissions
}

// close cancels the controller and finishes every held batch so all
// contenders exit.
func (h *copyThrottleHarness) close() {
	h.c.mu.Lock()
	h.c.closed = true
	h.c.cond.Broadcast()
	h.c.mu.Unlock()

	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()

	for {
		select {
		case <-done:
			return
		case <-h.admitted:
		default:
			for _, release := range h.releases {
				select {
				case release <- copyRelease{}:
				default:
				}
			}
			runtime.Gosched()
		}
	}
}

func TestCopyThrottleRecoversInStages(t *testing.T) {
	throttler := &PauserThrottler{}
	throttler.SetPaused(true)
	h := newCopyThrottleHarness(t, throttler, 4)
	h.startContenders(4)
	h.requireSettled(0)

	throttler.SetPaused(false)
	h.c.observe(h.clock.Now())
	held := h.admitStage(1, 25)

	// The stage interval is not over yet.
	h.clock.Advance(testCopyThrottleInterval - time.Nanosecond)
	held[0].succeed()
	held = h.admitStage(1, 25)

	// The stage is healthy. Batches admitted in the previous stage keep their
	// size, and the next one is admitted after they finish.
	h.clock.Advance(time.Nanosecond)
	h.c.observe(h.clock.Now())
	h.requireSettled(1)
	require.Equal(t, uint64(25), held[0].rows(200))
	held[0].succeed()
	held = h.admitStage(1, 50)

	for _, rows := range []uint64{100, 200} {
		h.clock.Advance(testCopyThrottleInterval)
		held[0].succeed()
		held = h.admitStage(1, rows)
	}

	// Concurrency only grows once batches are full sized.
	h.clock.Advance(testCopyThrottleInterval)
	held[0].succeed()
	held = h.admitStage(2, 200)

	h.clock.Advance(testCopyThrottleInterval)
	held[0].succeed()
	held = append(held[1:], h.admit(2)...)
	h.requireSettled(3)

	// held[0] was admitted in the previous stage; finish a current one.
	h.clock.Advance(testCopyThrottleInterval)
	held[1].succeed()
	h.admit(2)
	h.requireSettled(4)

	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	require.False(t, h.c.recovering)
	require.Equal(t, 4, h.c.limit)
	require.Equal(t, uint64(1), h.c.divisor)
}

func TestCopyThrottleThrottleStopsNewBatchesOnly(t *testing.T) {
	throttler := &PauserThrottler{}
	h := newCopyThrottleHarness(t, throttler, 4)
	h.startContenders(4)
	held := h.admitStage(4, 200)

	throttler.SetPaused(true)
	h.c.observe(h.clock.Now())

	// Running batches finish; their next batches wait.
	held[0].succeed()
	h.requireSettled(3)

	// Until the outstanding batches finish, the first recovery batch waits.
	throttler.SetPaused(false)
	h.c.observe(h.clock.Now())
	h.requireSettled(3)

	// Batches admitted before the throttle do not make the stage healthy.
	h.clock.Advance(10 * testCopyThrottleInterval)
	held[1].succeed()
	held[2].succeed()
	h.requireSettled(1)
	held[3].succeed()
	held = h.admitStage(1, 25)

	// A throttle and clear between batches restarts the stage.
	throttler.SetPaused(true)
	h.c.observe(h.clock.Now())
	throttler.SetPaused(false)
	h.c.observe(h.clock.Now())
	h.clock.Advance(testCopyThrottleInterval)
	held[0].succeed()
	held = h.admitStage(1, 25)

	h.clock.Advance(testCopyThrottleInterval)
	held[0].succeed()
	held = h.admitStage(1, 50)

	// A later throttle restarts recovery from the first stage.
	throttler.SetPaused(true)
	h.c.observe(h.clock.Now())
	h.clock.Advance(testCopyThrottleInterval)
	held[0].succeed()
	h.requireSettled(0)
	throttler.SetPaused(false)
	h.c.observe(h.clock.Now())
	h.admitStage(1, 25)
}

func TestCopyThrottleGrowthRequiresCompletedBatch(t *testing.T) {
	throttler := &PauserThrottler{}
	throttler.SetPaused(true)
	h := newCopyThrottleHarness(t, throttler, 4)
	h.startContenders(2)
	throttler.SetPaused(false)
	h.c.observe(h.clock.Now())
	held := h.admitStage(1, 25)

	// Time passes without a completed batch.
	h.clock.Advance(10 * testCopyThrottleInterval)
	h.c.observe(h.clock.Now())
	h.requireSettled(1)

	// An empty terminal fetch only releases capacity.
	held[0].finishEmpty()
	held = h.admitStage(1, 25)

	// A copied batch completes the healthy stage.
	held[0].succeed()
	h.admitStage(1, 50)
}

func TestCopyThrottleFailedBatchRestartsStage(t *testing.T) {
	throttler := &PauserThrottler{}
	throttler.SetPaused(true)
	h := newCopyThrottleHarness(t, throttler, 4)
	h.startContenders(2)
	throttler.SetPaused(false)
	h.c.observe(h.clock.Now())
	held := h.admitStage(1, 25)

	h.clock.Advance(testCopyThrottleInterval)
	held[0].fail(errors.New("fetch failed"))
	held = h.admitStage(1, 25)

	// The stage restarted with the batch admitted after the failure.
	h.clock.Advance(testCopyThrottleInterval - time.Nanosecond)
	held[0].succeed()
	held = h.admitStage(1, 25)

	h.clock.Advance(time.Nanosecond)
	h.c.observe(h.clock.Now())
	held[0].succeed()
	h.admitStage(1, 50)
}

func TestCopyThrottleEmptyFetchKeepsStageProgress(t *testing.T) {
	throttler := &PauserThrottler{}
	throttler.SetPaused(true)
	h := newCopyThrottleHarness(t, throttler, 4)
	h.startContenders(2)
	throttler.SetPaused(false)
	h.c.observe(h.clock.Now())
	held := h.admitStage(1, 25)

	held[0].succeed()
	held = h.admitStage(1, 25)

	h.clock.Advance(testCopyThrottleInterval)
	held[0].finishEmpty()
	h.admitStage(1, 50)
}

func TestCopyThrottleGrowthRequiresFreshLagSample(t *testing.T) {
	throttler := &fakeSampledThrottler{interval: time.Second}
	throttler.SetPaused(true)
	h := newCopyThrottleHarness(t, throttler, 4)
	throttler.sample(h.clock.Now())
	h.startContenders(2)
	throttler.SetPaused(false)
	h.c.observe(h.clock.Now())
	held := h.admitStage(1, 25)

	// No sample newer than the one at the start of the stage, as when the
	// lag query returns NULL or no rows.
	h.clock.Advance(testCopyThrottleInterval)
	held[0].succeed()
	held = h.admitStage(1, 25)

	// A new sample that is older than two lag update intervals.
	throttler.sample(h.clock.Now().Add(-2*time.Second - time.Nanosecond))
	h.c.observe(h.clock.Now())
	h.requireSettled(1)

	throttler.sample(h.clock.Now().Add(-2 * time.Second))
	h.c.observe(h.clock.Now())
	held[0].succeed()
	held = h.admitStage(1, 50)

	// The next stage needs a sample taken after it started.
	h.clock.Advance(testCopyThrottleInterval)
	held[0].succeed()
	held = h.admitStage(1, 50)
	throttler.sample(h.clock.Now())
	h.c.observe(h.clock.Now())
	held[0].succeed()
	h.admitStage(1, 100)
}

func TestCopyThrottleAdmitsInFIFOOrder(t *testing.T) {
	throttler := &PauserThrottler{}
	throttler.SetPaused(true)
	h := newCopyThrottleHarness(t, throttler, 4)
	h.startContenders(4)
	throttler.SetPaused(false)
	h.c.observe(h.clock.Now())

	var order []int
	for range 8 {
		a := h.admitStage(1, 25)[0]
		order = append(order, a.contender)
		a.succeed()
	}
	require.Equal(t, []int{0, 1, 2, 3, 0, 1, 2, 3}, order)
}

func TestCopyThrottleRethrottleDuringFetchRetry(t *testing.T) {
	throttler := &PauserThrottler{}
	throttler.SetPaused(true)
	h := newCopyThrottleHarness(t, throttler, 4)
	h.startContenders(1)
	throttler.SetPaused(false)
	h.c.observe(h.clock.Now())
	held := h.admitStage(1, 25)

	// The cursor releases its permit before sleeping between read retries and
	// acquires a new one for the retry.
	throttler.SetPaused(true)
	held[0].fail(errors.New("fetch failed"))
	h.requireSettled(0)

	throttler.SetPaused(false)
	h.c.observe(h.clock.Now())
	h.admitStage(1, 25)
}

func TestCopyThrottleBatchSizeScaling(t *testing.T) {
	cases := []struct {
		divisor    uint64
		configured uint64
		rows       uint64
	}{
		{8, 1, 1},
		{8, 7, 1},
		{8, 8, 1},
		{8, 80, 10},
		{8, 104, 13},
		{8, 200, 25},
		{4, 200, 50},
		{2, 7, 3},
		{1, 1, 1},
		{1, 200, 200},
	}

	for _, tc := range cases {
		permit := copyThrottlePermit{divisor: tc.divisor}
		require.Equal(t, tc.rows, permit.batchSize(tc.configured), "1/%d of %d", tc.divisor, tc.configured)
	}
}

func TestCopyThrottleDisabledThrottlerBypassesRecovery(t *testing.T) {
	throttler := &PauserThrottler{}
	throttler.SetPaused(true)
	h := newCopyThrottleHarness(t, throttler, 4)
	h.startContenders(4)
	h.requireSettled(0)

	throttler.SetDisabled(true)
	h.c.observe(h.clock.Now())
	held := h.admitStage(4, 200)

	// Re-enabling without a throttle keeps the configured limits.
	throttler.SetPaused(false)
	throttler.SetDisabled(false)
	h.c.observe(h.clock.Now())
	for _, a := range held {
		a.succeed()
	}
	held = h.admitStage(4, 200)

	// Disabling during recovery restores the limits without waiting for the
	// stage interval.
	throttler.SetPaused(true)
	h.c.observe(h.clock.Now())
	throttler.SetPaused(false)
	for _, a := range held {
		a.succeed()
	}
	held = h.admitStage(1, 25)
	throttler.SetDisabled(true)
	h.c.observe(h.clock.Now())
	for _, a := range h.admit(3) {
		require.Equal(t, uint64(200), a.rows(200))
		held = append(held, a)
	}
	h.requireSettled(4)

	// Re-enabling while throttled starts a new recovery.
	throttler.SetPaused(true)
	throttler.SetDisabled(false)
	h.c.observe(h.clock.Now())
	h.clock.Advance(testCopyThrottleInterval)
	for _, a := range held {
		a.succeed()
	}
	h.requireSettled(0)
	throttler.SetPaused(false)
	h.c.observe(h.clock.Now())
	h.admitStage(1, 25)
}

func TestCopyThrottleCancellationReleasesWaiters(t *testing.T) {
	throttler := &PauserThrottler{}
	throttler.SetPaused(true)
	h := newCopyThrottleHarness(t, throttler, 4)
	h.startContenders(4)
	h.requireSettled(0)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		h.c.run(ctx)
	}()
	cancel()
	<-runDone

	for range 4 {
		require.Equal(t, context.Canceled, <-h.stopped)
	}
	h.wg.Wait()

	_, err := h.c.acquire()
	require.Equal(t, context.Canceled, err)

	// A cursor whose admission is canceled returns before touching the
	// source database, which is nil here.
	table := &TableSchema{
		Table:               &schema.Table{Schema: "gftest", Name: "table"},
		PaginationKeyColumn: &schema.TableColumn{Name: "id"},
	}
	batchSize := uint64(200)
	cursor := (&CursorConfig{BatchSize: &batchSize, ReadRetries: 5}).NewCursor(table, NewUint64Key(0), NewUint64Key(100))
	cursor.copyThrottle = h.c
	err = cursor.Each(func(*RowBatch) error {
		t.Fatal("batch copied after cancellation")
		return nil
	})
	require.Equal(t, context.Canceled, err)
}

func TestCopyThrottleConfigValidation(t *testing.T) {
	throttler := &PauserThrottler{}

	for _, concurrency := range []int{0, -1} {
		_, err := newCopyThrottleController(throttler, concurrency, DataIterationThrottleRecoveryConfig{})
		require.EqualError(t, err, "DataIterationConcurrency must be greater than zero")
	}

	for _, interval := range []string{"0s", "-1s", "five seconds"} {
		_, err := newCopyThrottleController(throttler, 4, DataIterationThrottleRecoveryConfig{RecoveryInterval: interval})
		require.EqualError(t, err, `DataIterationThrottleRecovery.RecoveryInterval must be a positive duration: "`+interval+`"`)
	}

	c, err := newCopyThrottleController(throttler, 4, DataIterationThrottleRecoveryConfig{})
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, c.interval)

	c, err = newCopyThrottleController(throttler, 4, DataIterationThrottleRecoveryConfig{RecoveryInterval: "1ns"})
	require.NoError(t, err)
	require.Equal(t, time.Nanosecond, c.interval)
}
