require "test_helper"

class CallbacksTest < GhostferryTestCase
  def test_progress_callback
    seed_simple_database_with_single_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline"})
    progress = []
    ghostferry.on_callback("progress") do |progress_data|
      progress << progress_data
    end

    ghostferry.run

    assert progress.length >= 1

    assert_equal "done", progress.last["CurrentState"]

    assert_equal 1111, progress.last["Tables"]["gftest.test_table_1"]["LastSuccessfulPaginationKey"]
    assert_equal 1111, progress.last["Tables"]["gftest.test_table_1"]["TargetPaginationKey"]
    assert_equal "completed", progress.last["Tables"]["gftest.test_table_1"]["CurrentAction"]

    result = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
    count = result.first["cnt"]
    assert count > 0, "There should be some rows on the target, not 0."
    assert_equal count, progress.last["Tables"]["gftest.test_table_1"]["RowsWritten"]
    assert progress.last["Tables"]["gftest.test_table_1"]["TotalRows"] > 0
    assert progress.last["Tables"]["gftest.test_table_1"]["TotalBytes"] > 0

    # data column is 32 characters so each row should be at least 32 bytes
    assert count * 32 < progress.last["Tables"]["gftest.test_table_1"]["BytesWritten"], "Each row should have more than 32 bytes"

    assert_equal 0, progress.last["ActiveDataIterators"]

    refute progress.last["LastSuccessfulBinlogPos"]["Name"].nil?
    refute progress.last["LastSuccessfulBinlogPos"]["Pos"].nil?
    assert progress.last["BinlogStreamerLag"] > 0
    assert_equal progress.last["LastSuccessfulBinlogPos"], progress.last["FinalBinlogPos"]

    assert progress.last["VerifierMessage"].include?("currentRowCount =")
    assert progress.last["VerifierMessage"].include?("currentEntryCount =")

    assert_equal false, progress.last["Throttled"]

    refute progress.last["PaginationKeysPerSecond"].nil?
    refute progress.last["ETA"].nil?
    assert progress.last["TimeTaken"] > 0
  end

  def test_progress_callback_triggered_at_beginning_of_cutover_phase
    # It's possible that during the cutover phase, a client of the Ghostferry library terminates.
    # We report progress before entering this phase.
    # This tests the case when the process exits before the last progress report.
    seed_simple_database_with_single_table
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    # Simulate a bad actor: the Ferry wrapper process fails after row copy completion.
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      ghostferry.kill_and_wait_for_exit
    end


    progress = []
    ghostferry.on_callback("progress") do |progress_data|
      progress << progress_data
    end

    ghostferry.run_expecting_failure

    assert progress.length >= 1

    assert_equal "cutover", progress.last["CurrentState"]

    assert_equal 1111, progress.last["Tables"]["gftest.test_table_1"]["LastSuccessfulPaginationKey"]
    assert_equal 1111, progress.last["Tables"]["gftest.test_table_1"]["TargetPaginationKey"]
    assert_equal "completed", progress.last["Tables"]["gftest.test_table_1"]["CurrentAction"]
    assert progress.last["Tables"]["gftest.test_table_1"]["TotalRows"] > 0
    assert progress.last["Tables"]["gftest.test_table_1"]["TotalBytes"] > 0

    result = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
    count = result.first["cnt"]
    assert count > 0, "There should be some rows on the target, not 0."
    assert_equal count, progress.last["Tables"]["gftest.test_table_1"]["RowsWritten"]

    assert_equal 0, progress.last["ActiveDataIterators"]

    # Note: we can't know the exact below positions, so we only refute they are empty
    refute progress.last["LastSuccessfulBinlogPos"]["Name"].nil?
    refute progress.last["LastSuccessfulBinlogPos"]["Pos"].nil?
    assert progress.last["BinlogStreamerLag"] > 0


    assert progress.last["VerifierMessage"].include?("currentRowCount =")
    assert progress.last["VerifierMessage"].include?("currentEntryCount =")

    assert_equal false, progress.last["Throttled"]

    refute progress.last["PaginationKeysPerSecond"].nil?
    refute progress.last["ETA"].nil?
    assert progress.last["TimeTaken"] > 0
  end

  def test_state_callback
    seed_simple_database_with_single_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    states = []
    ghostferry.on_callback("state") do |state_data|
      states << state_data
    end

    ghostferry.run

    assert states.length >= 1
    states.each do |state|
      assert_basic_fields_exist_in_dumped_state(state)
    end
  end
end
