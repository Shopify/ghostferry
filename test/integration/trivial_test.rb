require "test_helper"

class TrivialIntegrationTests < GhostferryTestCase
  def test_copy_data_without_any_writes_to_source
    seed_simple_database_with_single_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    ghostferry.run

    assert_test_table_is_identical
  end

  def test_copy_data_with_writes_to_source
    seed_simple_database_with_single_table

    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    start_datawriter_with_ghostferry(datawriter, ghostferry)
    stop_datawriter_during_cutover(datawriter, ghostferry)

    ghostferry.run
    assert_test_table_is_identical
  end

  class AlterTableDataWriter
    include DbHelper

    def initialize
      @client = Mysql2::Client.new(source_db_config)
    end

    def start
      table_name = full_table_name(DEFAULT_DB, DEFAULT_TABLE)
      @client.query("INSERT INTO #{table_name} VALUES (9000, 'test')")
      @client.query("ALTER TABLE #{table_name} ADD INDEX (data(100))")
      @client.query("INSERT INTO #{table_name} (id, data) VALUES (9001, 'test')")
    end
  end

  def test_copy_data_with_alter_fails_part_way_through
    seed_simple_database_with_single_table

    datawriter = AlterTableDataWriter.new
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    ghostferry.on_status(GhostferryHelper::Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      datawriter.start
    end

    ghostferry.run_expecting_failure

    source, target = source_and_target_table_metrics
    source_count = source[DEFAULT_FULL_TABLE_NAME][:row_count]
    target_count = target[DEFAULT_FULL_TABLE_NAME][:row_count]
    
    refute_equal(source_count, target_count, "target should have fewer rows than source")
  end

  def test_logged_query_omits_columns
    seed_simple_database_with_single_table

    with_env('CI', nil) do
      ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
      ghostferry.run

      assert ghostferry.logrus_lines["cursor"].length > 0

      ghostferry.logrus_lines["cursor"].each do |line|
        if line["msg"].start_with?("found ")
          assert line["sql"].start_with?("SELECT [omitted] FROM")
        end
      end
    end
  end
end
