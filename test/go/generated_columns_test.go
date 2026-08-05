package test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/ghostferry"
	sql "github.com/Shopify/ghostferry/sqlwrapper"
	"github.com/Shopify/ghostferry/testhelpers"

	"github.com/stretchr/testify/suite"
)

// GeneratedColumnsTestSuite exercises generated-column handling end to end
// against real MySQL servers: real DDL, real binlog row images, and the
// generated SQL executed against a real target.  The hand-built TableSchema
// tests elsewhere in this package test our logic; these test our premises —
// that MySQL sets the flags, that the values survive the binlog, and that a
// real target accepts the SQL we emit.
type GeneratedColumnsTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	sourceDB *sql.DB
}

func (this *GeneratedColumnsTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()

	// The binlog streamer needs its own connection: it hands it to the
	// replication client for the lifetime of the stream.
	testFerry := testhelpers.NewTestFerry()
	sourceConfig, err := testFerry.Source.MySQLConfig()
	this.Require().Nil(err)

	this.sourceDB, err = sql.Open("mysql", sourceConfig.FormatDSN(), testFerry.Source.Marginalia)
	this.Require().Nil(err)

	_, err = this.Ferry.SourceDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", testhelpers.TestSchemaName))
	this.Require().Nil(err)
	_, err = this.Ferry.TargetDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", testhelpers.TestSchemaName))
	this.Require().Nil(err)
}

func (this *GeneratedColumnsTestSuite) TearDownTest() {
	if this.sourceDB != nil {
		this.sourceDB.Close()
	}
	this.GhostferryUnitTestSuite.TearDownTest()
}

func (this *GeneratedColumnsTestSuite) createOnBothSides(ddl string) {
	_, err := this.Ferry.SourceDB.Exec(ddl)
	this.Require().Nil(err)
	_, err = this.Ferry.TargetDB.Exec(ddl)
	this.Require().Nil(err)
}

func (this *GeneratedColumnsTestSuite) execOnBothSides(query string, args ...interface{}) {
	_, err := this.Ferry.SourceDB.Exec(query, args...)
	this.Require().Nil(err)
	_, err = this.Ferry.TargetDB.Exec(query, args...)
	this.Require().Nil(err)
}

func (this *GeneratedColumnsTestSuite) loadTable(tableName string) *ghostferry.TableSchema {
	cache := this.loadTables()
	table := cache.Get(testhelpers.TestSchemaName, tableName)
	this.Require().NotNil(table)
	return table
}

func (this *GeneratedColumnsTestSuite) loadTables() ghostferry.TableSchemaCache {
	cache, err := ghostferry.LoadTables(
		this.Ferry.SourceDB,
		&testhelpers.TestTableFilter{
			DbsFunc:    testhelpers.DbApplicabilityFilter([]string{testhelpers.TestSchemaName}),
			TablesFunc: nil,
		},
		nil, nil, nil, nil,
	)
	this.Require().Nil(err)
	return cache
}

// captureBinlogEvents streams the source binlog while mutate() runs, and
// returns the DMLEvents Ghostferry decodes from it — row images produced by
// MySQL, not by us.
func (this *GeneratedColumnsTestSuite) captureBinlogEvents(mutate func()) []ghostferry.DMLEvent {
	testFerry := testhelpers.NewTestFerry()
	streamer := &ghostferry.BinlogStreamer{
		DB:           this.sourceDB,
		DBConfig:     testFerry.Config.Source,
		MyServerId:   testFerry.Config.MyServerId,
		ErrorHandler: testFerry.ErrorHandler,
		TableSchema:  this.loadTables(),
	}

	_, err := streamer.ConnectBinlogStreamerToMysql()
	this.Require().Nil(err)

	var captured []ghostferry.DMLEvent
	streamer.AddEventListener(func(evs []ghostferry.DMLEvent) error {
		captured = append(captured, evs...)
		streamer.FlushAndStop()
		return nil
	})

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		streamer.Run()
	}()

	mutate()

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		this.Require().FailNow("timed out waiting for binlog events")
	}

	return captured
}

// applyToTarget replays events against the target exactly as BinlogWriter does:
// render each event to SQL and execute it.
func (this *GeneratedColumnsTestSuite) applyToTarget(events []ghostferry.DMLEvent) {
	for _, ev := range events {
		stmt, err := ev.AsSQLString(ev.Database(), ev.Table())
		this.Require().Nil(err, "rendering event to SQL")

		_, err = this.Ferry.TargetDB.Exec(stmt)
		this.Require().Nil(err, fmt.Sprintf("executing replayed statement on target: %s", stmt))
	}
}

func (this *GeneratedColumnsTestSuite) sourceStrings(query string) []string {
	return this.queryStrings(this.Ferry.SourceDB, query)
}

func (this *GeneratedColumnsTestSuite) targetStrings(query string) []string {
	return this.queryStrings(this.Ferry.TargetDB, query)
}

func (this *GeneratedColumnsTestSuite) queryStrings(db *sql.DB, query string) []string {
	rows, err := db.Query(query)
	this.Require().Nil(err)
	defer rows.Close()

	var out []string
	for rows.Next() {
		var v string
		this.Require().Nil(rows.Scan(&v))
		out = append(out, v)
	}
	this.Require().Nil(rows.Err())
	return out
}

// docsDDL is the content-addressed table from the PR #437 review: the primary
// key is a hash of the body, so nothing else can tell two rows apart.  The
// collation is pinned so the test does not depend on the server default.
const docsDDL = "CREATE TABLE %s.docs (" +
	"doc TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL," +
	"doc_hash BINARY(32) AS (UNHEX(SHA2(doc, 256))) STORED," +
	"PRIMARY KEY (doc_hash))"

func (this *GeneratedColumnsTestSuite) seedDocs() {
	this.createOnBothSides(fmt.Sprintf(docsDDL, testhelpers.TestSchemaName))
	// Four rows, four distinct hashes, one equivalence class under `=` on
	// `doc`: utf8mb4_unicode_ci is case-insensitive, accent-insensitive and
	// PAD SPACE.
	this.execOnBothSides(fmt.Sprintf(
		"INSERT INTO %s.docs (doc) VALUES ('cafe'), ('caf\u00e9'), ('CAFE'), ('cafe  ')",
		testhelpers.TestSchemaName,
	))
}

// assertDocsMatch asserts the target is indistinguishable from the source,
// catching over- and under-deletion in one order-independent assertion.
func (this *GeneratedColumnsTestSuite) assertDocsMatch() {
	query := fmt.Sprintf(
		"SELECT doc, HEX(doc_hash) AS h FROM %s.docs ORDER BY h",
		testhelpers.TestSchemaName,
	)
	testhelpers.AssertTwoQueriesHaveEqualResult(this.T(), this.Ferry, query, query)
}

func (this *GeneratedColumnsTestSuite) targetDocCount() int {
	return len(this.targetStrings(fmt.Sprintf("SELECT doc FROM %s.docs", testhelpers.TestSchemaName)))
}

// sourceCount guards against a vacuous pass: if a mutation silently failed on
// the source, source and target would agree on the unchanged data.
func (this *GeneratedColumnsTestSuite) sourceCount(table string) int {
	var n int
	row := this.Ferry.SourceDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", testhelpers.TestSchemaName, table))
	this.Require().Nil(row.Scan(&n))
	return n
}

// TestBinlogDeleteAffectsExactlyTheRowThatWasDeleted is the data-loss
// regression test for the WHERE clause.  SQL `=` compares under the column's
// collation, so without `doc_hash` in the predicate a one-row delete on the
// source becomes a multi-row delete on the target.
func (this *GeneratedColumnsTestSuite) TestBinlogDeleteAffectsExactlyTheRowThatWasDeleted() {
	this.seedDocs()

	events := this.captureBinlogEvents(func() {
		_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf(
			"DELETE FROM %s.docs WHERE doc_hash = UNHEX(SHA2('cafe', 256))",
			testhelpers.TestSchemaName,
		))
		this.Require().Nil(err)
	})
	this.Require().Equal(1, len(events))

	this.applyToTarget(events)

	this.Require().Equal(3, this.targetDocCount(),
		"replaying a one-row DELETE must not remove rows that merely compare equal under the collation")
	this.assertDocsMatch()
}

// TestBinlogDeleteOnCompositeKeyAffectsExactlyOneRow is the same failure in
// an ordinary-looking table.  The exposure is not limited to "the primary key
// IS a generated column": it is that after removing every generated column,
// no remaining subset forms a unique key.  Here `tenant`, `label` and
// `payload` are all in the WHERE clause and still fail to separate the rows.
func (this *GeneratedColumnsTestSuite) TestBinlogDeleteOnCompositeKeyAffectsExactlyOneRow() {
	this.createOnBothSides(fmt.Sprintf(
		"CREATE TABLE %s.composite ("+
			"label VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,"+
			"tenant INT NOT NULL,"+
			"payload INT NOT NULL,"+
			"label_hash BINARY(32) AS (UNHEX(SHA2(label, 256))) STORED,"+
			"PRIMARY KEY (label_hash),"+
			"UNIQUE KEY u (tenant, label_hash))",
		testhelpers.TestSchemaName,
	))
	this.execOnBothSides(fmt.Sprintf(
		"INSERT INTO %s.composite (label, tenant, payload) VALUES ('abc', 1, 7), ('ABC', 1, 7)",
		testhelpers.TestSchemaName,
	))

	events := this.captureBinlogEvents(func() {
		_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf(
			"DELETE FROM %s.composite WHERE label_hash = UNHEX(SHA2('abc', 256))",
			testhelpers.TestSchemaName,
		))
		this.Require().Nil(err)
	})
	this.Require().Equal(1, len(events))
	this.Require().Equal(1, this.sourceCount("composite"),
		"test is broken: the source DELETE should have removed exactly one of the two rows")

	this.applyToTarget(events)

	query := fmt.Sprintf(
		"SELECT label, tenant, payload, HEX(label_hash) AS h FROM %s.composite ORDER BY h",
		testhelpers.TestSchemaName,
	)
	testhelpers.AssertTwoQueriesHaveEqualResult(this.T(), this.Ferry, query, query)
}

// TestBinlogUpdateAffectsExactlyTheRowThatWasUpdated is the UPDATE
// counterpart: the same over-match, but rewriting every matched row to the
// same body makes the generated primary keys collide, so it fails loudly
// rather than silently.
func (this *GeneratedColumnsTestSuite) TestBinlogUpdateAffectsExactlyTheRowThatWasUpdated() {
	this.seedDocs()

	events := this.captureBinlogEvents(func() {
		_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf(
			"UPDATE %s.docs SET doc = 'greetings' WHERE doc_hash = UNHEX(SHA2('cafe', 256))",
			testhelpers.TestSchemaName,
		))
		this.Require().Nil(err)
	})
	this.Require().Equal(1, len(events))
	this.Require().Equal(
		[]string{"greetings"},
		this.sourceStrings(fmt.Sprintf("SELECT doc FROM %s.docs WHERE doc = 'greetings'", testhelpers.TestSchemaName)),
		"test is broken: the source UPDATE should have rewritten exactly one row",
	)

	this.applyToTarget(events)

	this.Require().Equal(4, this.targetDocCount())
	this.assertDocsMatch()
}

// TestStoredPaginationKeyIsUsedInBinlogWhereClause covers the operational
// consequence of dropping the key from the WHERE clause: every replayed
// statement would scan the table instead of seeking on the primary key.
func (this *GeneratedColumnsTestSuite) TestStoredPaginationKeyIsUsedInBinlogWhereClause() {
	this.seedDocs()

	table := this.loadTable("docs")
	this.Require().Equal("doc_hash", table.GetPaginationColumn().Name)
	this.Require().True(table.GetPaginationColumn().IsStored, "MySQL must report doc_hash as STORED")

	events := this.captureBinlogEvents(func() {
		_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf(
			"UPDATE %s.docs SET doc = 'greetings' WHERE doc_hash = UNHEX(SHA2('cafe', 256))",
			testhelpers.TestSchemaName,
		))
		this.Require().Nil(err)
	})
	this.Require().Equal(1, len(events))

	stmt, err := events[0].AsSQLString(events[0].Database(), events[0].Table())
	this.Require().Nil(err)

	// An exact prefix pins the whole SET clause and the start of WHERE in one
	// go; only the raw SHA-256 bytes of the key are left unpinned.
	this.Require().True(
		strings.HasPrefix(stmt,
			"UPDATE `gftest`.`docs` SET `doc`=_binary'greetings'"+
				" WHERE `doc`=_binary'cafe' AND `doc_hash`='"),
		"the pagination key must be in the WHERE clause and absent from SET; got: "+stmt,
	)
}

// itemsDDL places one VIRTUAL and one STORED generated column between
// ordinary columns, so index-space confusion shows up as a wrong value rather
// than a coincidentally correct one.
const itemsDDL = "CREATE TABLE %s.items (" +
	"id BIGINT NOT NULL AUTO_INCREMENT," +
	"body VARCHAR(64) NOT NULL," +
	"body_len BIGINT AS (CHAR_LENGTH(body)) VIRTUAL," +
	"note VARCHAR(64)," +
	"body_upper VARCHAR(64) AS (UPPER(body)) STORED," +
	"PRIMARY KEY (id))"

func (this *GeneratedColumnsTestSuite) seedItems() {
	this.createOnBothSides(fmt.Sprintf(itemsDDL, testhelpers.TestSchemaName))
}

func (this *GeneratedColumnsTestSuite) assertItemsMatch() {
	query := fmt.Sprintf(
		"SELECT id, body, body_len, note, body_upper FROM %s.items ORDER BY id",
		testhelpers.TestSchemaName,
	)
	testhelpers.AssertTwoQueriesHaveEqualResult(this.T(), this.Ferry, query, query)
}

// TestBinlogInsertReplayLetsTargetComputeGeneratedColumns: the INSERT must
// not name the generated columns, and need not — the target computes
// identical values from the columns we do send.
func (this *GeneratedColumnsTestSuite) TestBinlogInsertReplayLetsTargetComputeGeneratedColumns() {
	this.seedItems()

	events := this.captureBinlogEvents(func() {
		_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf(
			"INSERT INTO %s.items (id, body, note) VALUES (1, 'hello', 'first')",
			testhelpers.TestSchemaName,
		))
		this.Require().Nil(err)
	})
	this.Require().Equal(1, len(events))

	this.applyToTarget(events)
	this.assertItemsMatch()
}

// TestBinlogUpdateReplayMatchesOnGeneratedColumnValues: the VIRTUAL and
// STORED values read from the source's row image match what the target
// computed for itself, so predicating on them finds the row.
func (this *GeneratedColumnsTestSuite) TestBinlogUpdateReplayMatchesOnGeneratedColumnValues() {
	this.seedItems()
	this.execOnBothSides(fmt.Sprintf(
		"INSERT INTO %s.items (id, body, note) VALUES (1, 'hello', 'first'), (2, 'world', 'second')",
		testhelpers.TestSchemaName,
	))

	events := this.captureBinlogEvents(func() {
		_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf(
			"UPDATE %s.items SET body = 'goodbye', note = 'edited' WHERE id = 1",
			testhelpers.TestSchemaName,
		))
		this.Require().Nil(err)
	})
	this.Require().Equal(1, len(events))

	stmt, err := events[0].AsSQLString(events[0].Database(), events[0].Table())
	this.Require().Nil(err)
	this.Require().Contains(stmt, "`body_len`=", "VIRTUAL value from the row image belongs in the WHERE clause")
	this.Require().Contains(stmt, "`body_upper`=", "STORED value from the row image belongs in the WHERE clause")

	this.applyToTarget(events)
	this.assertItemsMatch()
}

// TestBinlogDeleteReplayMatchesOnGeneratedColumnValues is the DELETE
// counterpart: the WHERE clause includes both generated columns and must
// still remove the row on the target.
func (this *GeneratedColumnsTestSuite) TestBinlogDeleteReplayMatchesOnGeneratedColumnValues() {
	this.seedItems()
	this.execOnBothSides(fmt.Sprintf(
		"INSERT INTO %s.items (id, body, note) VALUES (1, 'hello', 'first'), (2, 'world', 'second')",
		testhelpers.TestSchemaName,
	))

	events := this.captureBinlogEvents(func() {
		_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf("DELETE FROM %s.items WHERE id = 1", testhelpers.TestSchemaName))
		this.Require().Nil(err)
	})
	this.Require().Equal(1, len(events))
	this.Require().Equal(1, this.sourceCount("items"), "test is broken: the source DELETE removed the wrong number of rows")

	this.applyToTarget(events)
	this.assertItemsMatch()

	this.Require().Equal(
		[]string{"world"},
		this.targetStrings(fmt.Sprintf("SELECT body FROM %s.items ORDER BY id", testhelpers.TestSchemaName)),
	)
}

// TestMySQLRejectsAssignmentToGeneratedColumns pins the premise the INSERT
// and SET filtering rests on: error 3105, for VIRTUAL and STORED alike.
func (this *GeneratedColumnsTestSuite) TestMySQLRejectsAssignmentToGeneratedColumns() {
	this.seedItems()

	_, err := this.Ferry.TargetDB.Exec(fmt.Sprintf(
		"INSERT INTO %s.items (id, body, body_len) VALUES (1, 'hello', 5)",
		testhelpers.TestSchemaName,
	))
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "3105", "assigning a VIRTUAL generated column must be rejected")

	_, err = this.Ferry.TargetDB.Exec(fmt.Sprintf(
		"INSERT INTO %s.items (id, body, body_upper) VALUES (1, 'hello', 'HELLO')",
		testhelpers.TestSchemaName,
	))
	this.Require().NotNil(err)
	this.Require().Contains(err.Error(), "3105", "assigning a STORED generated column must be rejected")
}

// TestUnsignedGeneratedColumnsAreNormalisedBeforeUse: go-mysql hands back an
// unsigned generated column as a negative signed integer, like any other
// unsigned column.  Skipping generated columns during normalisation — to
// match the INSERT and SET filtering — would emit `WHERE v = -1` for a column
// holding 18446744073709551615, and every replayed UPDATE and DELETE for the
// table would quietly do nothing.
func (this *GeneratedColumnsTestSuite) TestUnsignedGeneratedColumnsAreNormalisedBeforeUse() {
	this.createOnBothSides(fmt.Sprintf(
		"CREATE TABLE %s.bignum ("+
			"id BIGINT NOT NULL,"+
			"base BIGINT UNSIGNED NOT NULL,"+
			"v_copy BIGINT UNSIGNED AS (base) VIRTUAL,"+
			"s_copy BIGINT UNSIGNED AS (base) STORED,"+
			"PRIMARY KEY (id))",
		testhelpers.TestSchemaName,
	))
	// Above math.MaxInt64, so a signed reading of the binlog value is negative.
	this.execOnBothSides(fmt.Sprintf(
		"INSERT INTO %s.bignum (id, base) VALUES (1, 18446744073709551615)",
		testhelpers.TestSchemaName,
	))

	events := this.captureBinlogEvents(func() {
		_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf("DELETE FROM %s.bignum WHERE id = 1", testhelpers.TestSchemaName))
		this.Require().Nil(err)
	})
	this.Require().Equal(1, len(events))

	stmt, err := events[0].AsSQLString(events[0].Database(), events[0].Table())
	this.Require().Nil(err)

	this.Require().Equal(
		"DELETE FROM `gftest`.`bignum` WHERE `id`=1 AND `base`=18446744073709551615"+
			" AND `v_copy`=18446744073709551615 AND `s_copy`=18446744073709551615",
		stmt,
	)

	// And it has to actually match on the target, not merely look right.
	this.applyToTarget(events)
	this.Require().Empty(this.targetStrings(fmt.Sprintf("SELECT id FROM %s.bignum", testhelpers.TestSchemaName)))
}

// copyEverythingToTarget runs Ghostferry's real copy path over every loaded
// table, with the inline verifier enforcing.  Generated columns are part of
// the row fingerprint, so this checks the values the target computed, not
// just the columns we sent.
func (this *GeneratedColumnsTestSuite) copyEverythingToTarget() {
	this.Ferry.Tables = this.loadTables()
	err := this.Ferry.RunStandaloneDataCopy(this.Ferry.Tables.AsSlice())
	this.Require().Nil(err)
}

// TestCopyTableWithStoredGeneratedPrimaryKey covers the copy path for the
// docs table.  The copy path and the binlog path filter generated columns
// independently — one from query-result order, one from schema order — so
// passing on one says nothing about the other.
func (this *GeneratedColumnsTestSuite) TestCopyTableWithStoredGeneratedPrimaryKey() {
	this.createOnBothSides(fmt.Sprintf(docsDDL, testhelpers.TestSchemaName))
	_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf(
		"INSERT INTO %s.docs (doc) VALUES ('cafe'), ('caf\u00e9'), ('CAFE'), ('cafe  ')",
		testhelpers.TestSchemaName,
	))
	this.Require().Nil(err)

	this.copyEverythingToTarget()

	this.Require().Equal(4, this.targetDocCount())
	this.assertDocsMatch()
}

// TestCopyTableWithGeneratedColumnsBetweenOrdinaryOnes: with the generated
// columns mid-schema, dropping the wrong position shifts every later value by
// one instead of coincidentally landing correctly.
func (this *GeneratedColumnsTestSuite) TestCopyTableWithGeneratedColumnsBetweenOrdinaryOnes() {
	this.seedItems()
	_, err := this.Ferry.SourceDB.Exec(fmt.Sprintf(
		"INSERT INTO %s.items (id, body, note) VALUES (1, 'hello', 'first'), (2, 'world', 'second'), (3, 'third', NULL)",
		testhelpers.TestSchemaName,
	))
	this.Require().Nil(err)

	this.copyEverythingToTarget()
	this.assertItemsMatch()
}

func TestGeneratedColumnsTestSuite(t *testing.T) {
	suite.Run(t, &GeneratedColumnsTestSuite{GhostferryUnitTestSuite: &testhelpers.GhostferryUnitTestSuite{}})
}
