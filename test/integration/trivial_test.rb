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

  def test_fails_if_database_schema_is_changed_during_data_copy
    seed_simple_database_with_single_table
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    batches_written = 0
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      batches_written += 1
      if batches_written == 1
        source_db.query("ALTER TABLE #{DEFAULT_FULL_TABLE_NAME} ADD COLUMN extracolumn VARCHAR(15);")
      end
    end

    ghostferry.run_expecting_failure
  end
end
