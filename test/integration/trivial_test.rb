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
end
