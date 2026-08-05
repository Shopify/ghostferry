require "test_helper"

# End-to-end coverage for tables carrying MySQL generated (computed) columns.
#
# The default integration table (see DbHelper#seed_random_data) already carries
# one VIRTUAL and one STORED generated column, so the whole suite exercises the
# common case: an ordinary integer primary key with generated columns alongside
# it. This file covers the shape that table cannot express -- a table whose row
# identity is carried by a generated column -- because that is where replaying
# a binlog event can affect more rows on the target than it did on the source.
class GeneratedColumnsTest < GhostferryTestCase
  # A content-addressed table: the primary key is a STORED generated column
  # derived from the only other column. This is the table reported by
  # milanatshopify on PR #437, reproduced verbatim.
  CONTENT_ADDRESSED_TABLE = "test_content_addressed"
  CONTENT_ADDRESSED_FULL_TABLE_NAME = DbHelper.full_table_name(DbHelper::DEFAULT_DB, CONTENT_ADDRESSED_TABLE)

  # Same, plus a payload column carrying the *same* value in every sibling row.
  CONTENT_ADDRESSED_PAYLOAD_TABLE = "test_content_addressed_payload"
  CONTENT_ADDRESSED_PAYLOAD_FULL_TABLE_NAME = DbHelper.full_table_name(DbHelper::DEFAULT_DB, CONTENT_ADDRESSED_PAYLOAD_TABLE)

  # A shape much closer to a real merchant table: ordinary columns, a natural
  # key, and a content hash that happens to be the primary key.
  TENANT_LABELS_TABLE = "test_tenant_labels"
  TENANT_LABELS_FULL_TABLE_NAME = DbHelper.full_table_name(DbHelper::DEFAULT_DB, TENANT_LABELS_TABLE)

  # Four values that are DISTINCT as bytes -- so SHA2 gives them four distinct
  # keys and MySQL stores four separate rows -- but EQUAL under
  # utf8mb4_unicode_ci, which is the collation Ghostferry itself connects with
  # and the server default on both test containers.
  #
  # That collation is accent-insensitive, case-insensitive and PAD SPACE, so
  # all four of these are one equivalence class. Trailing whitespace is the
  # variant most likely to occur by accident in real data; the others are here
  # to show the class is wide, not exotic.
  #
  # This is the whole point of the fixture. `WHERE label = 'cafe'` is collation
  # equality, not byte equality, so it selects all four.
  SIBLING_DOCUMENTS = ["cafe", "café", "CAFE", "cafe  "].freeze

  # The one we delete or update on the source in each test.
  CHOSEN_DOCUMENT = "cafe"

  # A value outside the equivalence class, used as a control: it must survive
  # every statement below. Without it, a test that wiped the whole table would
  # look the same as a test that over-matched within the class.
  UNRELATED_DOCUMENT = "tea"

  ###############################################################
  # Collation over-match: binlog WHERE clauses must be exact     #
  ###############################################################
  #
  # Ghostferry replays a source row change onto the target by reconstructing a
  # WHERE clause from the binlog row image. If that WHERE clause omits the
  # generated columns, the remaining predicate is only as selective as the
  # column collation allows. When the generated column is what makes the row
  # unique, omitting it turns a single-row change on the source into a
  # multi-row change on the target.
  #
  # These tests are deliberately built so that they CANNOT pass by accident:
  #
  #   * The sibling rows are indistinguishable on every non-generated column.
  #     The precise condition for exposure is that after removing every
  #     generated column, no remaining subset of columns still forms a unique
  #     key. A fixture that violates that -- one carrying an id, a timestamp or
  #     a differing payload -- produces a WHERE clause that picks out exactly
  #     one row, passes against the broken code, and proves nothing.
  #
  #   * Each test first asserts that the SOURCE changed exactly one row. Without
  #     that guard a test that accidentally issued a multi-row source statement
  #     would compare an equally-wrong source and target and report success.
  #
  #   * Nothing is asserted inside an on_status handler. Minitest::Assertion
  #     descends from Exception rather than StandardError, and the callback
  #     server only rescues StandardError, so an assertion that fails in a
  #     handler is swallowed and the test passes regardless. Observations are
  #     captured into locals and asserted after ghostferry.run returns.

  # DELETE is the silent case: over-matching removes rows from the target that
  # still exist on the source, nothing errors, and Ghostferry reports success.
  def test_binlog_delete_must_not_over_match_rows_equal_under_the_column_collation
    seed_content_addressed_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    docs_on_target_before_delete = nil
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      # Row copy has finished, so every row is already on the target and the
      # DELETE below can only reach the target through the binlog.
      docs_on_target_before_delete = content_addressed_docs(target_db)

      # Delete exactly ONE row, addressed by its primary key. Note that
      # `WHERE doc = 'cafe'` would delete all four here too -- on the source.
      source_db.query(
        "DELETE FROM #{CONTENT_ADDRESSED_FULL_TABLE_NAME} " \
        "WHERE doc_hash = UNHEX(SHA2('#{CHOSEN_DOCUMENT}', 256))"
      )
    end

    ghostferry.run
    assert_nil ghostferry.error

    expected = (SIBLING_DOCUMENTS - [CHOSEN_DOCUMENT]).sort

    assert_equal SIBLING_DOCUMENTS.sort, docs_on_target_before_delete,
      "test is broken: the row copy should have put every row on the target " \
      "before the DELETE was issued"

    assert_equal expected, content_addressed_docs(source_db),
      "test is broken: the source DELETE should have removed exactly one row"

    target_docs = content_addressed_docs(target_db)
    assert_equal expected, target_docs,
      "binlog DELETE over-matched on the target: one row was deleted on the " \
      "source, but the target went from #{SIBLING_DOCUMENTS.length} rows to " \
      "#{target_docs.length}"
  end

  # UPDATE over-matches for the same reason, but it does not corrupt quietly.
  # A replayed UPDATE assigns EVERY non-generated column its after-image value,
  # including the column feeding the generated expression, whether or not the
  # source statement touched it. So all over-matched rows are rewritten to the
  # same content, recompute the same key, and MySQL rejects the second one with
  # a duplicate-entry error. Ghostferry then aborts the move.
  #
  # Loud rather than silent, but still a failed shop move caused by a statement
  # that should only ever have touched one row, so the requirement is the same:
  # the target must match the source and the run must not error.
  def test_binlog_update_must_not_over_match_rows_equal_under_the_column_collation
    seed_content_addressed_payload_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    rows_on_target_before_update = nil
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      rows_on_target_before_update = content_addressed_payload_rows(target_db)

      source_db.query(
        "UPDATE #{CONTENT_ADDRESSED_PAYLOAD_FULL_TABLE_NAME} SET payload = 'updated' " \
        "WHERE doc_hash = UNHEX(SHA2('#{CHOSEN_DOCUMENT}', 256))"
      )
    end

    begin
      ghostferry.run
    rescue Ghostferry::ExitError
      flunk "ghostferry aborted while replaying the UPDATE, which means the " \
        "replayed statement matched more rows than the one the source " \
        "updated: #{ghostferry.error && ghostferry.error["ErrMessage"]}"
    end

    assert_nil ghostferry.error

    expected = SIBLING_DOCUMENTS.to_h { |doc| [doc, doc == CHOSEN_DOCUMENT ? "updated" : "original"] }

    assert_equal SIBLING_DOCUMENTS.to_h { |doc| [doc, "original"] }, rows_on_target_before_update,
      "test is broken: the row copy should have put every row on the target " \
      "before the UPDATE was issued"

    assert_equal expected, content_addressed_payload_rows(source_db),
      "test is broken: the source UPDATE should have changed exactly one row"

    assert_equal expected, content_addressed_payload_rows(target_db),
      "binlog UPDATE over-matched on the target"
  end

  # The two tests above use a minimal two-column table, which invites the
  # response that no real schema looks like that. This one does not: it has a
  # tenant, a human-readable label, a payload, a content hash for the primary
  # key and a natural unique key over (tenant, hash). Ordinary columns really
  # are present in the reconstructed WHERE clause, and they still do not save
  # it, because once the generated column is removed nothing that remains is
  # unique. `tenant` and `payload` are shared across the sibling rows.
  def test_binlog_delete_over_matches_even_when_ordinary_columns_are_present
    seed_tenant_labels_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      source_db.query(
        "DELETE FROM #{TENANT_LABELS_FULL_TABLE_NAME} " \
        "WHERE label_hash = UNHEX(SHA2('#{CHOSEN_DOCUMENT}', 256))"
      )
    end

    ghostferry.run
    assert_nil ghostferry.error

    expected = (SIBLING_DOCUMENTS - [CHOSEN_DOCUMENT] + [UNRELATED_DOCUMENT]).sort

    assert_equal expected, tenant_labels(source_db),
      "test is broken: the source DELETE should have removed exactly one row"

    target_labels = tenant_labels(target_db)

    # Stated separately from the equality below so that a failure distinguishes
    # "the replayed DELETE was too broad" from "it deleted everything".
    assert_includes target_labels, UNRELATED_DOCUMENT,
      "the replayed DELETE removed a row outside the collation equivalence class"

    assert_equal expected, target_labels,
      "binlog DELETE over-matched on the target despite tenant and payload " \
      "appearing in the WHERE clause: the source lost 1 row, the target lost " \
      "#{SIBLING_DOCUMENTS.length + 1 - target_labels.length}"
  end

  private

  # `doc`/`label` are TEXT so that the first fixture matches the table in the
  # review comment verbatim. The collation is stated explicitly rather than
  # inherited so the test cannot quietly stop testing anything if a server
  # default changes.
  COLLATED_TEXT = "TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL".freeze

  def seed_content_addressed_table
    create_on_both(
      "CREATE TABLE IF NOT EXISTS #{CONTENT_ADDRESSED_FULL_TABLE_NAME} (" \
        "doc #{COLLATED_TEXT}, " \
        "doc_hash BINARY(32) AS (UNHEX(SHA2(doc, 256))) STORED, " \
        "PRIMARY KEY (doc_hash))"
    )

    statement = source_db.prepare("INSERT INTO #{CONTENT_ADDRESSED_FULL_TABLE_NAME} (doc) VALUES (?)")
    SIBLING_DOCUMENTS.each { |doc| statement.execute(doc) }

    assert_equal SIBLING_DOCUMENTS.sort, content_addressed_docs(source_db),
      "fixture is broken: the sibling documents must be stored as distinct rows"
  end

  def seed_content_addressed_payload_table
    create_on_both(
      "CREATE TABLE IF NOT EXISTS #{CONTENT_ADDRESSED_PAYLOAD_FULL_TABLE_NAME} (" \
        "doc #{COLLATED_TEXT}, " \
        "payload VARCHAR(32) NOT NULL, " \
        "doc_hash BINARY(32) AS (UNHEX(SHA2(doc, 256))) STORED, " \
        "PRIMARY KEY (doc_hash))"
    )

    statement = source_db.prepare(
      "INSERT INTO #{CONTENT_ADDRESSED_PAYLOAD_FULL_TABLE_NAME} (doc, payload) VALUES (?, 'original')"
    )
    SIBLING_DOCUMENTS.each { |doc| statement.execute(doc) }
  end

  def seed_tenant_labels_table
    create_on_both(
      "CREATE TABLE IF NOT EXISTS #{TENANT_LABELS_FULL_TABLE_NAME} (" \
        "tenant VARCHAR(32) NOT NULL, " \
        "label #{COLLATED_TEXT}, " \
        "payload VARCHAR(32) NOT NULL, " \
        "label_hash BINARY(32) AS (UNHEX(SHA2(label, 256))) STORED, " \
        "PRIMARY KEY (label_hash), " \
        "UNIQUE KEY tenant_label (tenant, label_hash))"
    )

    statement = source_db.prepare(
      "INSERT INTO #{TENANT_LABELS_FULL_TABLE_NAME} (tenant, label, payload) VALUES ('acme', ?, 'same')"
    )
    (SIBLING_DOCUMENTS + [UNRELATED_DOCUMENT]).each { |label| statement.execute(label) }
  end

  def create_on_both(ddl)
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query(ddl)
    end
  end

  # Sorted in Ruby, byte-wise. Ordering in SQL by the text column would be
  # ambiguous under a case-insensitive collation, and ordering by the hash
  # would make the expected values in each test unreadable.
  def content_addressed_docs(db)
    db.query("SELECT doc FROM #{CONTENT_ADDRESSED_FULL_TABLE_NAME}").map { |row| row["doc"] }.sort
  end

  def content_addressed_payload_rows(db)
    db.query("SELECT doc, payload FROM #{CONTENT_ADDRESSED_PAYLOAD_FULL_TABLE_NAME}")
      .each_with_object({}) { |row, acc| acc[row["doc"]] = row["payload"] }
  end

  def tenant_labels(db)
    db.query("SELECT label FROM #{TENANT_LABELS_FULL_TABLE_NAME}").map { |row| row["label"] }.sort
  end
end
