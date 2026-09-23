package test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	sqlorig "database/sql"

	"github.com/Shopify/ghostferry"
	sqlwrapper "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/Shopify/ghostferry/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Selects the destructive scenario when this binary is re-executed as a
// child. The scenario leaves Ferry goroutines running on failure and
// Ghostferry has no Ferry.Cancel, so only a separate process can be killed.
const failoverChildEnv = "GHOSTFERRY_FAILOVER_TEST_CHILD"

const (
	childScenarioSuccess       = "success"
	childScenarioFailAfterStop = "fail_after_stop"
)

// GTID-only, and needs docker/podman to stop the source; skips otherwise.
// As a parent it runs the child and restores the environment; with
// failoverChildEnv set it runs the scenario itself.
func TestMasterFailoverRecoveryReconnectsToPromotedMaster(t *testing.T) {
	if scenario := os.Getenv(failoverChildEnv); scenario != "" {
		runFailoverScenario(t, scenario)
		return
	}

	parent := newFailoverParent(t)
	output, err := parent.runChild(childScenarioSuccess)
	if err != nil {
		t.Errorf("failover child failed: %v\n%s", err, output)
	}
	// A skipped child would exit zero and prove nothing.
	assert.Contains(t, output, "--- PASS: TestMasterFailoverRecoveryReconnectsToPromotedMaster",
		"the child must have run the scenario, not skipped it")

	require.NoError(t, parent.cleanup(), "environment restoration failed")
	parent.assertEnvironmentRestored()
}

// A child that dies with the source down must not contaminate later tests.
func TestMasterFailoverRecoveryCleansUpAfterFailure(t *testing.T) {
	if os.Getenv(failoverChildEnv) != "" {
		t.Skip("the child process runs only the failover scenario")
	}

	parent := newFailoverParent(t)
	output, err := parent.runChild(childScenarioFailAfterStop)
	require.Error(t, err, "the child must fail:\n%s", output)
	assert.Contains(t, output, "--- FAIL: TestMasterFailoverRecoveryReconnectsToPromotedMaster",
		"the child must have failed inside the scenario, not skipped it")

	// Proves the child reached the stopped-source phase rather than failing
	// early for an unrelated reason.
	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.Error(t, parent.sourceDB.PingContext(pingCtx),
		"the source must still be down before restoration:\n%s", output)

	require.NoError(t, parent.cleanup(), "environment restoration failed")
	parent.assertEnvironmentRestored()

	testcase := &testhelpers.IntegrationTestCase{
		T:           t,
		SetupAction: setupSingleTableDatabase,
		Ferry:       testhelpers.NewTestFerry(),
	}
	testcase.Run()
}

type failoverParent struct {
	t    *testing.T
	tool string
	// Captured before the child stops it: restoration must not depend on the
	// container still being listable.
	sourceContainer string

	sourceDB   *sqlwrapper.DB
	targetDB   *sqlwrapper.DB
	promotedDB *sqlwrapper.DB

	cleanupOnce sync.Once
	cleanupErr  error
}

func newFailoverParent(t *testing.T) *failoverParent {
	t.Helper()

	if binlogModeFromEnv() != ghostferry.BinlogCoordinateGTID {
		t.Skip("master failover recovery is only supported in gtid mode")
	}
	tool := containerTool()
	if tool == "" {
		t.Skip("master failover test requires docker or podman to stop the source container")
	}
	name := sourceContainerName(tool)
	require.NotEmpty(t, name, "source container must exist")

	p := &failoverParent{t: t, tool: tool, sourceContainer: name}
	p.sourceDB = openControlDB(t, testhelpers.TestSourcePort)
	p.targetDB = openControlDB(t, testhelpers.TestTargetPort)
	p.promotedDB = openControlDB(t, testhelpers.TestFutureMasterPort)

	// Registered after the handles, so it runs before they are closed.
	t.Cleanup(func() {
		if err := p.cleanup(); err != nil {
			t.Errorf("environment restoration failed: %v", err)
		}
	})
	return p
}

func (p *failoverParent) runChild(scenario string) (string, error) {
	p.t.Helper()

	executable, err := os.Executable()
	require.NoError(p.t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, executable,
		"-test.run=^TestMasterFailoverRecoveryReconnectsToPromotedMaster$",
		"-test.v",
		"-test.timeout=210s",
	)
	cmd.Env = append(os.Environ(), failoverChildEnv+"="+scenario)
	cmd.WaitDelay = 5 * time.Second

	output, err := cmd.CombinedOutput()
	return string(output), err
}

// Runs both as an explicit post-child step and as a t.Cleanup.
func (p *failoverParent) cleanup() error {
	p.cleanupOnce.Do(func() { p.cleanupErr = p.restoreEnvironment() })
	return p.cleanupErr
}

func (p *failoverParent) restoreEnvironment() error {
	var errs []error

	if err := runContainerCommand(p.tool, 30*time.Second, "start", p.sourceContainer); err != nil {
		errs = append(errs, fmt.Errorf("restarting %s: %w", p.sourceContainer, err))
	}
	if err := waitUntilPingable(p.sourceDB, 180*time.Second); err != nil {
		errs = append(errs, fmt.Errorf("source did not accept connections again: %w", err))
	}

	// State dependent (mysql-3 may not be a replica at all), so their
	// failures are not cleanup failures.
	execControlIgnoringError(p.promotedDB, "STOP REPLICA")
	execControlIgnoringError(p.promotedDB, "RESET REPLICA ALL")
	// super_read_only must be cleared before read_only.
	if err := execControl(p.promotedDB, "SET GLOBAL super_read_only = OFF"); err != nil {
		errs = append(errs, err)
	}
	if err := execControl(p.promotedDB, "SET GLOBAL read_only = OFF"); err != nil {
		errs = append(errs, err)
	}

	// Source/target GTID history is deliberately left alone: other tests
	// resume from it.
	for name, db := range p.servers() {
		for _, schema := range testhelpers.ApplicableTestDbs {
			if err := execControl(db, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", schema)); err != nil {
				errs = append(errs, fmt.Errorf("dropping %s on %s: %w", schema, name, err))
			}
		}
	}

	return errors.Join(errs...)
}

func (p *failoverParent) servers() map[string]*sqlwrapper.DB {
	return map[string]*sqlwrapper.DB{
		"source":   p.sourceDB,
		"target":   p.targetDB,
		"promoted": p.promotedDB,
	}
}

func (p *failoverParent) assertEnvironmentRestored() {
	p.t.Helper()

	for name, db := range p.servers() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := db.PingContext(ctx); err != nil {
			p.t.Errorf("%s server is not reachable after cleanup: %v", name, err)
		}
		cancel()

		assert.Equal(p.t, 0, testSchemaCount(p.t, db), "%s server still has test schemas", name)
	}
}

func testSchemaCount(t *testing.T, db *sqlwrapper.DB) int {
	t.Helper()

	placeholders := make([]string, 0, len(testhelpers.ApplicableTestDbs))
	args := make([]interface{}, 0, len(testhelpers.ApplicableTestDbs))
	for _, schema := range testhelpers.ApplicableTestDbs {
		placeholders = append(placeholders, "?")
		args = append(args, schema)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name IN (%s)",
		strings.Join(placeholders, ","),
	)
	if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		t.Errorf("counting test schemas: %v", err)
		return -1
	}
	return count
}

func runFailoverScenario(t *testing.T, scenario string) {
	require.Contains(t, []string{childScenarioSuccess, childScenarioFailAfterStop}, scenario)
	require.Equal(t, ghostferry.BinlogCoordinateGTID, binlogModeFromEnv(),
		"the child must inherit gtid coordinate mode")

	tool := containerTool()
	require.NotEmpty(t, tool, "the child needs a container tool to stop the source")

	sourceDB := openControlDB(t, testhelpers.TestSourcePort)
	targetDB := openControlDB(t, testhelpers.TestTargetPort)
	promotedDB := openControlDB(t, testhelpers.TestFutureMasterPort)

	dropTestDBs(t, sourceDB)
	dropTestDBs(t, targetDB)
	dropTestDBs(t, promotedDB)

	fm := &futureMaster{t: t, db: promotedDB, sourceDB: sourceDB, containerTool: tool}

	// mysql-3 replicates the source BEFORE seeding, so it holds the real seed
	// data and later writes; a failover to it then cannot lose applied
	// transactions.
	fm.setupAsReplicaOfSource()

	testhelpers.SeedInitialData(sourceDB, "gftest", "table1", 30)
	testhelpers.SeedInitialData(targetDB, "gftest", "table1", 0)

	promoted := &ghostferry.DatabaseConfig{
		Host:      "127.0.0.1",
		Port:      uint16(testhelpers.TestFutureMasterPort),
		Net:       "tcp",
		User:      "root",
		Pass:      "",
		Collation: "utf8mb4_unicode_ci",
		Params:    map[string]string{"charset": "utf8mb4"},
	}

	ferry := testhelpers.NewTestFerry()
	ferry.Config.BinlogCoordinateMode = ghostferry.BinlogCoordinateGTID
	ferry.Config.MasterFailoverRecovery = &ghostferry.MasterFailoverRecoveryConfig{
		Resolver: ghostferry.MasterWriterResolverFunc(func(_ *ghostferry.DatabaseConfig) (*ghostferry.DatabaseConfig, error) {
			return promoted, nil
		}),
		MaxAttempts: 20,
		RetryWait:   250 * time.Millisecond,
	}
	require.NoError(t, ferry.Config.ValidateConfig())

	errHandler := &testhelpers.ErrorHandler{}
	ferry.ErrorHandler = errHandler

	require.NoError(t, ferry.Initialize())
	require.NoError(t, ferry.Start())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ferry.Run()
	}()

	// Failover during row copy is out of scope: in-flight cursors stay bound
	// to their connection for scan consistency.
	ferry.WaitUntilRowCopyIsComplete()

	// The extra rows may already have streamed; what matters is that the
	// promoted master holds them, so recovery can resume there.
	insertRows(t, sourceDB, 50)
	fm.waitUntilCaughtUpToSource()
	fm.promote()
	fm.stopSourceContainer()

	if scenario == childScenarioFailAfterStop {
		// The parent must restore everything from a live-goroutine failure.
		t.Fatal("deliberately failing after stopping the source master")
	}

	// So cutover records its stop coordinate from the new writer.
	require.NoError(t, waitUntilSourceRepointed(ferry, uint16(testhelpers.TestFutureMasterPort), errHandler, 60*time.Second))

	// Rows only the promoted master ever had must still reach the target.
	insertRows(t, promotedDB, 10)

	ferry.FlushBinlogAndStopStreaming()
	wg.Wait()
	ferry.StopTargetVerifier()

	require.NoError(t, errHandler.Err(), "ghostferry should not have fataled")

	// 30 seeded + 50 on the old source + 10 only on the promoted master.
	require.Equal(t, 90, tableRowCount(t, promotedDB, "gftest", "table1"))
	require.Equal(t, 90, tableRowCount(t, targetDB, "gftest", "table1"))
	promotedChecksum := tableChecksum(t, promotedDB, "gftest", "table1")
	targetChecksum := tableChecksum(t, targetDB, "gftest", "table1")
	require.Equal(t, promotedChecksum, targetChecksum, "promoted master and target checksums differ")

	require.Equal(t, uint16(testhelpers.TestFutureMasterPort), ferry.SourceRuntime().Config().Port,
		"the ferry source should now be the promoted master")

	// Proves candidate normalization ran against a real server.
	runtimeDB := ferry.SourceRuntime().DB()
	require.NotNil(t, runtimeDB)
	var timeZone, sqlMode string
	require.NoError(t, runtimeDB.QueryRow("SELECT @@session.time_zone, @@session.sql_mode").Scan(&timeZone, &sqlMode))
	require.Equal(t, "+00:00", timeZone)
	require.Contains(t, sqlMode, "STRICT_ALL_TABLES")
	require.Contains(t, sqlMode, "NO_BACKSLASH_ESCAPES")

	// All source work is finished.
	ferry.SourceRuntime().CloseRetired()
}

// Returns an error rather than failing from a helper, so the caller decides
// on the test goroutine.
func waitUntilSourceRepointed(ferry *testhelpers.TestFerry, wantPort uint16, errHandler *testhelpers.ErrorHandler, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := errHandler.Err(); err != nil {
			return fmt.Errorf("ghostferry fataled during failover recovery: %w", err)
		}
		if cfg := ferry.SourceRuntime().Config(); cfg != nil && cfg.Port == wantPort {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("source was not repointed to port %d within %s", wantPort, timeout)
}

func binlogModeFromEnv() ghostferry.BinlogCoordinateType {
	mode := os.Getenv("GHOSTFERRY_BINLOG_COORDINATE_MODE")
	if mode == "" {
		return ghostferry.BinlogCoordinateFilePosition
	}
	return ghostferry.BinlogCoordinateType(mode)
}

// Cleanup is registered immediately after Open so a failing Ping cannot leak
// the handle.
func openControlDB(t *testing.T, port uint64) *sqlwrapper.DB {
	t.Helper()
	dsn := fmt.Sprintf(
		"root@tcp(127.0.0.1:%d)/?charset=utf8mb4&collation=utf8mb4_unicode_ci&interpolateParams=true&timeout=5s&readTimeout=30s&writeTimeout=5s",
		port,
	)
	db, err := sqlwrapper.Open("mysql", dsn, "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	return db
}

func execControl(db *sqlwrapper.DB, query string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", query, err)
	}
	return nil
}

func execControlIgnoringError(db *sqlwrapper.DB, query string) {
	_ = execControl(db, query)
}

func waitUntilPingable(db *sqlwrapper.DB, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		lastErr = db.PingContext(ctx)
		cancel()
		if lastErr == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("not reachable within %s: %w", timeout, lastErr)
}

func runContainerCommand(tool string, timeout time.Duration, args ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, tool, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w (%s)", tool, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func dropTestDBs(t *testing.T, db *sqlwrapper.DB) {
	t.Helper()
	for _, name := range testhelpers.ApplicableTestDbs {
		require.NoError(t, execControl(db, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name)))
	}
}

func insertRows(t *testing.T, db *sqlwrapper.DB, count int) {
	t.Helper()
	for range count {
		_, err := db.Exec("INSERT INTO gftest.table1 (id, data) VALUES (?, ?)", nil, testhelpers.RandData())
		require.NoError(t, err)
	}
}

func tableRowCount(t *testing.T, db *sqlwrapper.DB, dbName, table string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", dbName, table)).Scan(&n))
	return n
}

func tableChecksum(t *testing.T, db *sqlwrapper.DB, dbName, table string) int64 {
	t.Helper()
	var name string
	var checksum sqlorig.NullInt64
	require.NoError(t, db.QueryRow(fmt.Sprintf("CHECKSUM TABLE `%s`.`%s`", dbName, table)).Scan(&name, &checksum))
	require.True(t, checksum.Valid, "CHECKSUM TABLE returned NULL for %s.%s", dbName, table)
	return checksum.Int64
}

// containerTool returns "docker" or "podman" if one is on PATH and can see the
// source container, else "".
func containerTool() string {
	for _, tool := range []string{"docker", "podman"} {
		if _, err := exec.LookPath(tool); err != nil {
			continue
		}
		if sourceContainerName(tool) != "" {
			return tool
		}
	}
	return ""
}

func sourceContainerName(tool string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, err := exec.CommandContext(ctx, tool, "ps", "-a", "--format", "{{.Names}}").Output()
	if err != nil {
		return ""
	}
	names := strings.Split(string(out), "\n")
	for _, candidate := range []string{"ghostferry_mysql-1_1", "ghostferry-mysql-1-1"} {
		for _, n := range names {
			if strings.TrimSpace(n) == candidate {
				return candidate
			}
		}
	}
	return ""
}

// futureMaster manages mysql-3 as a promotable replica of the source.
type futureMaster struct {
	t             *testing.T
	db            *sqlwrapper.DB
	sourceDB      *sqlwrapper.DB
	containerTool string
}

func (f *futureMaster) exec(db *sqlwrapper.DB, query string) {
	f.t.Helper()
	_, err := db.Exec(query)
	require.NoError(f.t, err, "query: %s", query)
}

func (f *futureMaster) execIgnore(db *sqlwrapper.DB, query string) {
	_, _ = db.Exec(query)
}

func (f *futureMaster) sourceExecutedGTIDSet() string {
	f.t.Helper()
	var gtid string
	require.NoError(f.t, f.sourceDB.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&gtid))
	return strings.Join(strings.Fields(gtid), "")
}

func (f *futureMaster) resetBinaryLogsStatement() string {
	var version string
	require.NoError(f.t, f.db.QueryRow("SELECT VERSION()").Scan(&version))
	if versionAtLeast(version, 8, 4) {
		return "RESET BINARY LOGS AND GTIDS"
	}
	return "RESET MASTER"
}

func (f *futureMaster) setupAsReplicaOfSource() {
	f.t.Helper()
	f.execIgnore(f.db, "STOP REPLICA")
	f.execIgnore(f.db, "RESET REPLICA ALL")
	f.execIgnore(f.db, "SET GLOBAL super_read_only = OFF")
	f.execIgnore(f.db, "SET GLOBAL read_only = OFF")
	dropTestDBs(f.t, f.db)

	sourceGTID := f.sourceExecutedGTIDSet()
	f.exec(f.db, f.resetBinaryLogsStatement())
	if sourceGTID != "" {
		f.exec(f.db, fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", sourceGTID))
	}
	f.exec(f.db, "CHANGE REPLICATION SOURCE TO SOURCE_HOST='mysql-1', SOURCE_PORT=3306, SOURCE_USER='root', SOURCE_AUTO_POSITION=1")
	f.exec(f.db, "START REPLICA")
	f.waitUntilReplicaRunning()
}

func (f *futureMaster) waitUntilReplicaRunning() {
	f.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		io, sql := f.replicaRunning()
		if io && sql {
			return
		}
		if time.Now().After(deadline) {
			f.t.Fatalf("future master replica did not start in time")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (f *futureMaster) replicaRunning() (ioRunning, sqlRunning bool) {
	rows, err := f.db.Query("SHOW REPLICA STATUS")
	require.NoError(f.t, err)
	defer rows.Close()

	cols, err := rows.Columns()
	require.NoError(f.t, err)
	if !rows.Next() {
		return false, false
	}
	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	require.NoError(f.t, rows.Scan(ptrs...))
	for i, c := range cols {
		s := asString(vals[i])
		if c == "Replica_IO_Running" {
			ioRunning = s == "Yes"
		}
		if c == "Replica_SQL_Running" {
			sqlRunning = s == "Yes"
		}
	}
	return ioRunning, sqlRunning
}

func (f *futureMaster) waitUntilCaughtUpToSource() {
	f.t.Helper()
	sourceGTID := f.sourceExecutedGTIDSet()
	if sourceGTID == "" {
		return
	}

	// The server waits up to 20 seconds; give the query a slightly longer
	// deadline so the client never gives up before the server answers.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	var result sqlorig.NullInt64
	require.NoError(f.t, f.db.QueryRowContext(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 20)", sourceGTID).Scan(&result))
	require.True(f.t, result.Valid, "WAIT_FOR_EXECUTED_GTID_SET returned NULL")
	require.Equal(f.t, int64(0), result.Int64, "future master did not catch up to source in time")
}

func (f *futureMaster) promote() {
	f.t.Helper()
	f.waitUntilCaughtUpToSource()
	f.exec(f.db, "STOP REPLICA")
	f.exec(f.db, "RESET REPLICA ALL")
	f.execIgnore(f.db, "SET GLOBAL super_read_only = OFF")
	f.exec(f.db, "SET GLOBAL read_only = OFF")
}

func (f *futureMaster) stopSourceContainer() {
	f.t.Helper()
	name := sourceContainerName(f.containerTool)
	require.NotEmpty(f.t, name, "source container not found")
	require.NoError(f.t, runContainerCommand(f.containerTool, 30*time.Second, "stop", name))
}

func asString(v interface{}) string {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case string:
		return t
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}

func versionAtLeast(version string, major, minor int) bool {
	var maj, min int
	if _, err := fmt.Sscanf(version, "%d.%d", &maj, &min); err != nil {
		return false
	}
	return maj > major || (maj == major && min >= minor)
}
