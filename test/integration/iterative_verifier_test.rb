require "test_helper"

class IterativeVerifierTest < GhostferryTestCase
  def setup
    seed_simple_database_with_single_table
  end

  def test_iterative_verifier_succeeds_in_normal_run
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { enable_iterative_verifier: true })

    start_datawriter_with_ghostferry(datawriter, ghostferry)
    stop_datawriter_during_cutover(datawriter, ghostferry)

    verification_ran = false
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*incorrect_tables|
      verification_ran = true
      assert_equal 0, incorrect_tables.length
    end

    ghostferry.run
    assert verification_ran
    assert_test_table_is_identical
  end

  def test_iterative_verifier_fails_if_binlog_streamer_incorrectly_copies_data
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { enable_iterative_verifier: true })

    table_name = DEFAULT_FULL_TABLE_NAME

    chosen_id = 0
    verification_ran = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      result = source_db.query("SELECT id FROM #{table_name} ORDER BY id LIMIT 1")
      chosen_id = result.first["id"]

      refute chosen_id == 0
      source_db.query("UPDATE #{table_name} SET data = 'something' WHERE id = #{chosen_id}")
    end

    ghostferry.on_status(Ghostferry::Status::VERIFY_DURING_CUTOVER) do
      refute chosen_id == 0
      source_db.query("DELETE FROM #{table_name} WHERE id = #{chosen_id}")
    end

    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*incorrect_tables|
      verification_ran = true

      assert_equal ["gftest.test_table_1"], incorrect_tables
    end

    ghostferry.run
    assert verification_ran
  end
end
