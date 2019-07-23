require "test_helper"

require "json"

class InterruptResumeTest < GhostferryTestCase
  def setup
    seed_simple_database_with_single_table
  end

  def test_interrupt_resume_without_writes_to_source_to_check_target_state_when_interrupted
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    # Writes one batch
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      ghostferry.send_signal("TERM")
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)

    result = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
    count = result.first["cnt"]
    assert_equal 200, count

    result = target_db.query("SELECT MAX(id) AS max_id FROM #{DEFAULT_FULL_TABLE_NAME}")
    last_successful_id = result.first["max_id"]
    assert last_successful_id > 0
    assert_equal last_successful_id, dumped_state["LastSuccessfulPrimaryKeys"]["#{DEFAULT_DB}.#{DEFAULT_TABLE}"]
  end

  def test_interrupt_and_resume_without_last_known_schema_cache
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    # Writes one batch
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      ghostferry.send_signal("TERM")
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)
    dumped_state["LastKnownTableSchemaCache"] = nil

    # Resume Ghostferry with dumped state
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    ghostferry.run(dumped_state)

    assert_test_table_is_identical
  end

  def test_interrupt_resume_with_writes_to_source
    # Start a ghostferry run expecting it to be interrupted.
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    start_datawriter_with_ghostferry(datawriter, ghostferry)

    batches_written = 0
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      batches_written += 1
      if batches_written >= 2
        ghostferry.send_signal("TERM")
      end
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)

    # Resume Ghostferry with dumped state
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    # The datawriter is still writing to the database since earlier, so we need
    # to stop it during cutover.
    stop_datawriter_during_cutover(datawriter, ghostferry)

    ghostferry.run(dumped_state)

    assert_test_table_is_identical
  end

  def test_interrupt_resume_when_table_has_completed
    # Start a run of Ghostferry expecting to be interrupted
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    start_datawriter_with_ghostferry(datawriter, ghostferry)
    stop_datawriter_during_cutover(datawriter, ghostferry)

    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      ghostferry.send_signal("TERM")
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)

    # Resume ghostferry from interrupted state
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    start_datawriter_with_ghostferry(datawriter, ghostferry)
    stop_datawriter_during_cutover(datawriter, ghostferry)

    ghostferry.run(dumped_state)

    assert_test_table_is_identical
  end
end
