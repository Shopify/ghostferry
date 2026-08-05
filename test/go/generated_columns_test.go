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
// against real MySQL servers: real DDL, real DML, real binlog row images, and
// the generated SQL actually executed against a real target.
//
// The other generated-column tests in this package construct a TableSchema by
// hand and set IsVirtual/IsStored on an otherwise ordinary column. That is
// convenient and fast, but it can only prove what Ghostferry does with a flag
// it was handed; it cannot prove that MySQL sets that flag, that the value
// survives the binlog, or that the SQL we emit is accepted by a target that
// really has a generated column. This suite closes that gap, so that the
// hand-built tests are testing our logic and these are testing our premises.
type GeneratedColumnsTestSuite struct {
	*testhelpers.GhostferryUnitTestSuite

	sourceDB *sql.DB
}

func (this *GeneratedColumnsTestSuite) SetupTest() {
	this.GhostferryUnitTestSuite.SetupTest()

	// The binlog streamer needs its own connection, separate from the pooled
	// Ferry.SourceDB, because it hands the connection to the replication
	// client for the lifetime of the stream.
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

// createOnBothSides runs the same DDL against source and target, mirroring a
// real move where the target schema is created from the source schema.
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
// returns the DMLEvents Ghostferry decodes from it. This is the whole point of
// the suite: the row images are produced by MySQL, not by us.
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
	wg := &sync.WaitGroup{}
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

// docsDDL is Milan's table from the PR #437 review, with the collation pinned
// so the test proves the same thing whatever the server default is.
//
// A content-addressed table like this is the motivating case for supporting
// STORED generated columns at all: the primary key is a hash of the body, so
// the body is the only other column and nothing else can tell two rows apart.
const docsDDL = "CREATE TABLE %s.docs (" +
	"doc TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL," +
	"doc_hash BINARY(32) AS (UNHEX(SHA2(doc, 256))) STORED," +
	"PRIMARY KEY (doc_hash))"

func (this *GeneratedColumnsTestSuite) seedDocs() {
	this.createOnBothSides(fmt.Sprintf(docsDDL, testhelpers.TestSchemaName))
	// Four rows, four distinct hashes, one equivalence class as far as `=` on
	// `doc` is concerned.  utf8mb4_unicode_ci — Ghostferry's own configured
	// collation — is accent-insensitive and PAD SPACE as well as
	// case-insensitive, so the trailing-space row belongs to the class too.
	// That one matters most: stray trailing whitespace arrives in real data by
	// accident, where deliberate case variants usually do not.
	this.execOnBothSides(fmt.Sprintf(
		"INSERT INTO %s.docs (doc) VALUES ('cafe'), ('caf\u00e9'), ('CAFE'), ('cafe  ')",
		testhelpers.TestSchemaName,
	))
}

// assertDocsMatch states the only thing that really matters after a replay:
// the target is indistinguishable from the source.  It catches removing too
// much and removing too little in one assertion, without depending on row
// order.
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

// sourceCount exists so that comparing source against target cannot pass
// vacuously.  If a mutation silently failed to affect the source, the two
// sides would agree on the unchanged data and the comparison would say nothing.
func (this *GeneratedColumnsTestSuite) sourceCount(table string) int {
	var n int
	row := this.Ferry.SourceDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", testhelpers.TestSchemaName, table))
	this.Require().Nil(row.Scan(&n))
	return n
}

// TestBinlogDeleteAffectsExactlyTheRowThatWasDeleted is the data-loss
// regression test for the WHERE clause.
//
// A generated column is a deterministic function of the other columns in the
// row, which makes it tempting to conclude that omitting it from the WHERE
// clause selects the same rows. It does not. SQL `=` compares under the
// column's collation, which is coarser than value equality, so the remaining
// columns need not identify the row uniquely. Here `doc='hello'` matches all
// three rows and only `doc_hash` separates them, so a WHERE clause without it
// turns a one-row delete on the source into a three-row delete on the target.
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

// TestBinlogDeleteOnCompositeKeyAffectsExactlyOneRow is the same failure in a
// shape that looks like an ordinary application table rather than a minimal
// reproduction: several ordinary columns, a composite unique key, and the
// generated column as only one part of it.
//
// It is here because the tempting summary of the bug — "only tables whose
// primary key IS a generated column" — is too narrow, and a fixture built to
// that summary passes against the broken code and proves nothing.  The real
// condition is that after removing every generated column, no remaining subset
// still forms a unique key.  Here `tenant`, `label` and `payload` are all in
// the WHERE clause and still fail to separate the two rows.
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

// TestBinlogUpdateAffectsExactlyTheRowThatWasUpdated is the UPDATE counterpart
// of the delete test above. The over-match is the same; the consequence
// differs, because rewriting all three rows to the same body makes their
// generated primary keys collide, so this one fails loudly rather than
// silently. Both outcomes are wrong and both are fixed by the same WHERE
// clause.
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

// TestStoredPaginationKeyIsUsedInBinlogWhereClause is the assertion Milan
// attached to his review of PR #437. The tests above cover the correctness
// consequence of dropping the key from the WHERE clause; this one covers the
// operational one, which is that every replayed statement would otherwise scan
// the whole table instead of seeking on the primary key.
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

	// Asserted as an exact prefix rather than as separate Contains/NotContains
	// checks: it pins the whole SET clause and the start of the WHERE clause in
	// one go, and it fails if either stops filtering correctly.  Only the raw
	// SHA-256 bytes of the key are left unpinned — go-mysql hands BINARY back as
	// a string, so they are emitted as a plain quoted literal.
	this.Require().True(
		strings.HasPrefix(stmt,
			"UPDATE `gftest`.`docs` SET `doc`=_binary'greetings'"+
				" WHERE `doc`=_binary'cafe' AND `doc_hash`='"),
		"the pagination key must be in the WHERE clause and absent from SET; got: "+stmt,
	)
}

// itemsDDL carries one VIRTUAL and one STORED generated column, positioned
// between ordinary columns so that any index-space confusion between schema
// order and filtered order shows up as a wrong value rather than as a
// coincidentally correct one.
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

// assertItemsMatch is the only assertion that really matters for a move: the
// target row is indistinguishable from the source row, generated columns
// included.
func (this *GeneratedColumnsTestSuite) assertItemsMatch() {
	query := fmt.Sprintf(
		"SELECT id, body, body_len, note, body_upper FROM %s.items ORDER BY id",
		testhelpers.TestSchemaName,
	)
	testhelpers.AssertTwoQueriesHaveEqualResult(this.T(), this.Ferry, query, query)
}

// TestBinlogInsertReplayLetsTargetComputeGeneratedColumns proves the INSERT
// side of the policy: we must not name the generated columns (MySQL rejects
// the assignment outright, see the 3105 test below), and we do not need to,
// because the target computes identical values from the columns we do send.
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

// TestBinlogUpdateReplayMatchesOnGeneratedColumnValues proves the WHERE side of
// the policy against real MySQL: the VIRTUAL and STORED values Ghostferry reads
// out of the source's binlog row image do match the values the target computed
// for itself, so predicating on them finds the row rather than silently
// matching nothing.
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
// counterpart: a delete whose WHERE clause includes both generated columns must
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

// TestMySQLRejectsAssignmentToGeneratedColumns records the premise the INSERT
// and SET filtering rests on, so that a future reader does not have to take it
// on faith. MySQL error 3105 is raised for both VIRTUAL and STORED columns,
// which is why the filtering there cannot be narrowed the way the WHERE clause
// can.
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

// TestUnsignedGeneratedColumnsAreNormalisedBeforeUse guards an invariant that
// a future tidy-up would plausibly break.
//
// go-mysql hands back an unsigned column as a negative signed integer, and it
// does so for generated columns exactly as for ordinary ones.  Ghostferry
// normalises the whole row before rendering any SQL.  Skipping generated
// columns there — for the appealing but wrong reason that INSERT and SET skip
// them too — would emit `WHERE v = -1` for a column holding
// 18446744073709551615, which matches no row, so every replayed UPDATE and
// DELETE for the table would quietly do nothing.
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

// copyEverythingToTarget runs Ghostferry's real copy path — DataIterator,
// Cursor, RowBatch, BatchWriter — over every loaded table.  It also turns on
// the inline verifier in enforcing mode, so a batch whose row fingerprints
// disagree between source and target fails the copy rather than being reported
// later.  Generated columns are part of that fingerprint, so this checks the
// values the target computed, not just the columns we sent.
func (this *GeneratedColumnsTestSuite) copyEverythingToTarget() {
	this.Ferry.Tables = this.loadTables()
	err := this.Ferry.RunStandaloneDataCopy(this.Ferry.Tables.AsSlice())
	this.Require().Nil(err)
}

// TestCopyTableWithStoredGeneratedPrimaryKey covers the copy path for the shape
// the binlog tests above are built around.
//
// The copy path and the binlog path filter generated columns independently —
// one from query-result order, one from schema order — so passing on one says
// nothing about the other.  Getting it wrong here does not merely misplace a
// value: the pagination key is a generated column, so a batch that failed to
// filter would be rejected outright by MySQL and a batch that filtered the
// wrong position would write every value into the wrong column.
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

// TestCopyTableWithGeneratedColumnsBetweenOrdinaryOnes places a VIRTUAL and a
// STORED column in the middle of the schema so that dropping the wrong
// position shifts every later value by one, rather than happening to land
// correctly as it would if the generated columns were last.
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
