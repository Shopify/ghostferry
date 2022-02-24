require "test_helper"

class DdlEventsTest < GhostferryTestCase
  DDL_GHOSTFERRY = "ddl_ghostferry"

  def test_default_event_handler
    seed_simple_database_with_single_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    ghostferry.run_with_logs()

    assert_ghostferry_completed(ghostferry, times: 1)
  end

  def test_ddl_event_handler
    seed_simple_database_with_single_table

    ghostferry = new_ghostferry(DDL_GHOSTFERRY)
    ghostferry.run_with_logs()

    assert_ghostferry_completed(ghostferry, times: 1)
  end

  def test_ddl_event_handler_with_ddl_events
    seed_simple_database_with_single_table

    table_name = full_table_name(DEFAULT_DB, DEFAULT_TABLE)

    ghostferry = new_ghostferry(DDL_GHOSTFERRY)

    ghostferry.on_status(GhostferryHelper::Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      source_db.query("INSERT INTO #{table_name} VALUES (9000, 'test')")
      source_db.query("ALTER TABLE #{table_name} ADD INDEX (data(100))")
      source_db.query("INSERT INTO #{table_name} (id, data) VALUES (9001, 'test')")
    end

    ghostferry.run_expecting_failure

    source, target = source_and_target_table_metrics
    source_count = source[DEFAULT_FULL_TABLE_NAME][:row_count]
    target_count = target[DEFAULT_FULL_TABLE_NAME][:row_count]

    refute_equal(source_count, target_count, "target should have fewer rows than source")

  end
end
