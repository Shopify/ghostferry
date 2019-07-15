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

  def test_interrupt_resume_inline_verifier_with_datawriter
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    start_datawriter_with_ghostferry(datawriter, ghostferry)

    batches_written = 0
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      batches_written += 1
      if batches_written >= 2
        ghostferry.term_and_wait_for_exit
      end
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)
    refute_nil dumped_state["BinlogVerifyStore"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]["test_table_1"]

    # Resume Ghostferry with dumped state
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    # The datawriter is still writing to the database since earlier, so we need
    # to stop it during cutover.
    stop_datawriter_during_cutover(datawriter, ghostferry)

    ghostferry.run(dumped_state)

    assert verification_ran
    assert_equal 0, incorrect_tables.length
    assert_test_table_is_identical
  end

  def test_interrupt_inline_verifier_will_emit_binlog_verify_store
    result = source_db.query("SELECT MAX(id) FROM #{DEFAULT_FULL_TABLE_NAME}")
    chosen_id = result.first["MAX(id)"] + 1

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    i = 0
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      sleep if i >= 1 # block the DataIterator so it doesn't race with the term_and_wait_for_exit below.
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (#{chosen_id}, 'data')")
      i += 1
    end

    ghostferry.on_status(Ghostferry::Status::AFTER_BINLOG_APPLY) do
      ghostferry.term_and_wait_for_exit
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)
    refute_nil dumped_state["BinlogVerifyStore"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]["test_table_1"]

    # FLAKY: AFTER_BINLOG_APPLY is emitted after the BinlogStreamer
    #        finishes processing all of the event listeners. The block below
    #        blocks the BinlogStreamer from processing additional rows but it
    #        does not block InlineVerifier.PeriodicallyVerifyBinlogEvents. This
    #        means the binlog event created in the code above may be verified
    #        before the TERM signal is processed. Thus, the state dump may not
    #        contain this row.
    #
    #        Fixing this is somewhat non-trivial and likely require a more
    #        extensive signal emitting system within Ghostferry.
    assert_equal 1, dumped_state["BinlogVerifyStore"]["gftest"]["test_table_1"]["#{chosen_id}"]

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    ghostferry.run(dumped_state)
    assert_test_table_is_identical
  end

  def test_interrupt_resume_inline_verifier_will_verify_entries_in_reverify_store
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    # This row would have been copied as we terminate ghostferry after 1 batch
    # is copied.
    result = source_db.query("SELECT MIN(id) FROM #{DEFAULT_FULL_TABLE_NAME}")
    chosen_id = result.first["MIN(id)"] + 1

    i = 0
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      if i == 1
        # We need to delete it the second batch because trying to delete the
        # minimum id row while in the first batch will result in a lock as the
        # DataIterator holds a FOR UPDATE lock for the minimum id row.
        source_db.query("DELETE FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = #{chosen_id}")
      end
      sleep if i > 1 # block the DataIterator so it doesn't race with the term_and_wait_for_exit below.
      i += 1
    end

    ghostferry.on_status(Ghostferry::Status::AFTER_BINLOG_APPLY) do
      ghostferry.term_and_wait_for_exit
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)
    refute_nil dumped_state["BinlogVerifyStore"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]["test_table_1"]

    # FLAKY: See explanation in test_interrupt_inline_verifier_will_emit_binlog_verify_store
    assert_equal 1, dumped_state["BinlogVerifyStore"]["gftest"]["test_table_1"]["#{chosen_id}"]

    # Corrupt the row before resuming
    target_db.query("REPLACE INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (#{chosen_id}, 'corrupted')")

    verification_ran = false
    incorrect_tables = nil
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run(dumped_state)
    assert verification_ran
    assert_equal 1, incorrect_tables.length
    assert_equal "gftest.test_table_1", incorrect_tables.first

    error_line = ghostferry.error_lines.last
    assert_equal "cutover verification failed for: gftest.test_table_1 [pks: #{chosen_id} ] ", error_line["msg"]
  end

  def test_interrupt_resume_inline_verifier_will_verify_additional_rows_changed_on_source_during_interrupt
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      ghostferry.term_and_wait_for_exit
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)
    refute_nil dumped_state["BinlogVerifyStore"]

    result = source_db.query("SELECT MIN(id) FROM #{DEFAULT_FULL_TABLE_NAME}")
    chosen_id = result.first["MIN(id)"]

    source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'data2' WHERE id = #{chosen_id}")
    target_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'corrupted' WHERE id = #{chosen_id}")

    verification_ran = false
    incorrect_tables = nil
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run(dumped_state)
    assert verification_ran
    assert_equal 1, incorrect_tables.length
    assert_equal "gftest.test_table_1", incorrect_tables.first

    error_line = ghostferry.error_lines.last
    assert_equal "cutover verification failed for: gftest.test_table_1 [pks: #{chosen_id} ] ", error_line["msg"]
  end
end
