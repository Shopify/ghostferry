require "test_helper"

class TrivialIntegrationTest < GhostferryTestCase
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
end
