package test

import (
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

// The promoted-writer ("future master") MySQL. It exists in the docker-compose
// files as mysql-3 and is started by .github/workflows/start-mysql.sh. There is
// no N3_PORT helper, so hardcode it (matching the Ruby harness convention).
const futureMasterPort = 29293

// TestMasterFailoverRecoveryReconnectsToPromotedMaster drives a full source
// master failover and asserts Ghostferry reconnects to the promoted writer and
// finishes the move against it.
//
// It is GTID-only (failover recovery requires server-independent coordinates)
// and needs a container tool (docker/podman) to stop the source, so it skips
// when either precondition is absent.
func TestMasterFailoverRecoveryReconnectsToPromotedMaster(t *testing.T) {
	if binlogModeFromEnv() != ghostferry.BinlogCoordinateGTID {
		t.Skip("master failover recovery is only supported in gtid mode")
	}
	tool := containerTool(t)
	if tool == "" {
		t.Skip("master failover test requires docker or podman to stop the source container")
	}

	sourcePort := testhelpers.TestSourcePort
	targetPort := testhelpers.TestTargetPort

	sourceDB := openDB(t, sourcePort)
	defer sourceDB.Close()
	targetDB := openDB(t, targetPort)
	defer targetDB.Close()
	promotedDB := openDB(t, uint64(futureMasterPort))
	defer promotedDB.Close()

	// Clean slate on all three.
	dropTestDBs(t, sourceDB)
	dropTestDBs(t, targetDB)
	dropTestDBs(t, promotedDB)

	fm := &futureMaster{t: t, db: promotedDB, sourceDB: sourceDB, containerTool: tool}
	// Ensure the promoted server is restored even if the test fails.
	defer fm.restoreSourceContainer()
	defer fm.reset()

	// mysql-3 replicates the source BEFORE seeding, so it holds the real seed
	// data and later writes; a failover to it then cannot lose applied
	// transactions.
	fm.setupAsReplicaOfSource()

	testhelpers.SeedInitialData(sourceDB, "gftest", "table1", 30)
	testhelpers.SeedInitialData(targetDB, "gftest", "table1", 0)

	promoted := &ghostferry.DatabaseConfig{
		Host:      "127.0.0.1",
		Port:      uint16(futureMasterPort),
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

	// Wait until row copy is fully complete before touching the source, so the
	// data iterator is no longer reading it when we take it down. (Failover
	// during row copy is out of scope: in-flight cursors are bound to their
	// connection for scan consistency.)
	ferry.WaitUntilRowCopyIsComplete()

	// Now fail the source over while the binlog streamer is still live and has
	// no stop target yet:
	//   1. write extra rows to the source and wait for mysql-3 to replicate them,
	//      so they exist on the promoted master but have NOT been streamed yet;
	//   2. promote mysql-3 and stop the old source container.
	// The streamer then finds the old source gone and must recover onto mysql-3
	// to stream those rows before cutover completes.
	insertRows(t, sourceDB, 50)
	fm.waitUntilCaughtUpToSource()
	fm.promote()
	fm.stopSourceContainer()

	// Wait for the streamer to actually recover onto the promoted master before
	// driving cutover, so cutover records its stop coordinate from the new
	// writer. (In Go we control the flow directly, so we can synchronize here
	// rather than racing the cutover path against recovery.)
	waitUntilSourceRepointed(t, ferry, uint16(futureMasterPort), errHandler)

	ferry.FlushBinlogAndStopStreaming()
	wg.Wait()
	ferry.StopTargetVerifier()

	require.NoError(t, errHandler.LastError, "ghostferry should not have fataled")

	// The promoted master (mysql-3) is now the source of truth; the target must
	// be identical to it.
	assertTablesIdentical(t, promotedDB, targetDB, "gftest", "table1")

	// Recovery must actually have run and repointed the source.
	assert.NotEqual(t, uint16(sourcePort), ferry.SourceRuntime().Config().Port,
		"the ferry source should have been repointed away from the dead master")
	assert.Equal(t, uint16(futureMasterPort), ferry.SourceRuntime().Config().Port,
		"the ferry source should now be the promoted master")
}

// waitUntilSourceRepointed blocks until the ferry's source runtime has been
// swapped to the expected (promoted) port, or the ferry fataled, or a timeout.
func waitUntilSourceRepointed(t *testing.T, ferry *testhelpers.TestFerry, wantPort uint16, errHandler *testhelpers.ErrorHandler) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if errHandler.LastError != nil {
			t.Fatalf("ghostferry fataled during failover recovery: %v", errHandler.LastError)
		}
		if cfg := ferry.SourceRuntime().Config(); cfg != nil && cfg.Port == wantPort {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("source was not repointed to port %d within timeout", wantPort)
}

// ---- helpers ----

func binlogModeFromEnv() ghostferry.BinlogCoordinateType {
	mode := os.Getenv("GHOSTFERRY_BINLOG_COORDINATE_MODE")
	if mode == "" {
		return ghostferry.BinlogCoordinateFilePosition
	}
	return ghostferry.BinlogCoordinateType(mode)
}

func openDB(t *testing.T, port uint64) *sqlwrapper.DB {
	t.Helper()
	dsn := fmt.Sprintf("root@tcp(127.0.0.1:%d)/?charset=utf8mb4&collation=utf8mb4_unicode_ci&interpolateParams=true", port)
	db, err := sqlwrapper.Open("mysql", dsn, "")
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	return db
}

func dropTestDBs(t *testing.T, db *sqlwrapper.DB) {
	t.Helper()
	for _, name := range testhelpers.ApplicableTestDbs {
		_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name))
		require.NoError(t, err)
	}
}

func insertRows(t *testing.T, db *sqlwrapper.DB, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		_, err := db.Exec("INSERT INTO gftest.table1 (id, data) VALUES (?, ?)", nil, testhelpers.RandData())
		require.NoError(t, err)
	}
}

func assertTablesIdentical(t *testing.T, a, b *sqlwrapper.DB, dbName, table string) {
	t.Helper()
	countA := tableRowCount(t, a, dbName, table)
	countB := tableRowCount(t, b, dbName, table)
	assert.Greater(t, countA, 0, "promoted master should have rows")
	assert.Equal(t, countA, countB, "promoted master and target row counts differ")

	checksumA := tableChecksum(t, a, dbName, table)
	checksumB := tableChecksum(t, b, dbName, table)
	assert.Equal(t, checksumA, checksumB, "promoted master and target checksums differ")
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
	return checksum.Int64
}

// containerTool returns "docker" or "podman" if one is on PATH and can see the
// source container, else "".
func containerTool(t *testing.T) string {
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
	out, err := exec.Command(tool, "ps", "-a", "--format", "{{.Names}}").Output()
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
	var result sqlorig.NullInt64
	require.NoError(f.t, f.db.QueryRow("SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 20)", sourceGTID).Scan(&result))
	require.Equal(f.t, int64(0), result.Int64, "future master did not catch up to source in time")
}

func (f *futureMaster) promote() {
	f.t.Helper()
	f.waitUntilCaughtUpToSource()
	f.exec(f.db, "STOP REPLICA")
	f.exec(f.db, "RESET REPLICA ALL")
	f.exec(f.db, "SET GLOBAL read_only = OFF")
	f.execIgnore(f.db, "SET GLOBAL super_read_only = OFF")
}

func (f *futureMaster) stopSourceContainer() {
	f.t.Helper()
	name := sourceContainerName(f.containerTool)
	require.NotEmpty(f.t, name, "source container not found")
	require.NoError(f.t, exec.Command(f.containerTool, "stop", name).Run())
}

func (f *futureMaster) restoreSourceContainer() {
	name := sourceContainerName(f.containerTool)
	if name == "" {
		return
	}
	_ = exec.Command(f.containerTool, "start", name).Run()
	// Wait until the source accepts connections again for later tests.
	deadline := time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) {
		db, err := sqlwrapper.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/", testhelpers.TestSourcePort), "")
		if err == nil {
			pingErr := db.Ping()
			db.Close()
			if pingErr == nil {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (f *futureMaster) reset() {
	f.execIgnore(f.db, "STOP REPLICA")
	f.execIgnore(f.db, "RESET REPLICA ALL")
	f.execIgnore(f.db, "SET GLOBAL read_only = OFF")
	for _, name := range testhelpers.ApplicableTestDbs {
		f.execIgnore(f.db, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", name))
	}
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
