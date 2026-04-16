require "test_helper"

class IterativeVerifierTest < GhostferryTestCase
  def setup
    seed_simple_database_with_single_table
  end

  def test_iterative_verifier_succeeds_in_normal_run
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Iterative" })

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

  # The iterative verifier must include generated columns in its fingerprint so
  # that divergence in computed output between source and target is detected.
  # Base data (id, data) is identical on both sides; only the STORED generated
  # column expression differs on the target, which must trigger a failure.
  def test_iterative_verifier_detects_stored_generated_column_divergence
    target_db.query(
      "ALTER TABLE #{DEFAULT_FULL_TABLE_NAME} " \
      "MODIFY summary VARCHAR(32) AS (MD5(CONCAT(data, '_differs'))) STORED"
    )

    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Iterative" })

    start_datawriter_with_ghostferry(datawriter, ghostferry)
    stop_datawriter_during_cutover(datawriter, ghostferry)

    verification_ran = false
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*incorrect_tables|
      verification_ran = true
      assert_equal ["gftest.test_table_1"], incorrect_tables
    end

    ghostferry.run
    assert verification_ran
  end

  # Same but for a VIRTUAL generated column.
  def test_iterative_verifier_detects_virtual_generated_column_divergence
    target_db.query(
      "ALTER TABLE #{DEFAULT_FULL_TABLE_NAME} " \
      "MODIFY length BIGINT(20) AS (LENGTH(data) + 1) VIRTUAL"
    )

    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Iterative" })

    start_datawriter_with_ghostferry(datawriter, ghostferry)
    stop_datawriter_during_cutover(datawriter, ghostferry)

    verification_ran = false
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*incorrect_tables|
      verification_ran = true
      assert_equal ["gftest.test_table_1"], incorrect_tables
    end

    ghostferry.run
    assert verification_ran
  end

  def test_iterative_verifier_fails_if_binlog_streamer_incorrectly_copies_data
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Iterative" })

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
