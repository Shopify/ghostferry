require "test_helper"

class CallbacksTest < GhostferryTestCase
  def test_progress_callback
    seed_simple_database_with_single_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
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

    assert_equal 0, progress.last["ActiveDataIterators"]

    refute progress.last["LastSuccessfulBinlogPos"]["Name"].nil?
    refute progress.last["LastSuccessfulBinlogPos"]["Pos"].nil?
    assert progress.last["BinlogStreamerLag"] > 0
    assert_equal progress.last["LastSuccessfulBinlogPos"], progress.last["FinalBinlogPos"]

    assert progress.last["VerifierMessage"].start_with?("BinlogVerifyStore.currentRowCount =")

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
