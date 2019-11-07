require "test_helper"

class CallbacksTest < GhostferryTestCase
  def test_progress_callback
    seed_simple_database_with_single_table

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
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

    refute progress.last["LastSuccessfulBinlogPos"]["Name"].nil?
    refute progress.last["LastSuccessfulBinlogPos"]["Pos"].nil?
    assert progress.last["BinlogStreamerLag"] > 0
    assert_equal progress.last["LastSuccessfulBinlogPos"], progress.last["FinalBinlogPos"]

    assert_equal false, progress.last["Throttled"]

    refute progress.last["PaginationKeysPerSecond"].nil?
    refute progress.last["ETA"].nil?
    assert progress.last["TimeTaken"] > 0
  end
end
