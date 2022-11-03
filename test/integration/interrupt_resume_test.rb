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
    assert_equal last_successful_id, dumped_state["LastSuccessfulPaginationKeys"]["#{DEFAULT_DB}.#{DEFAULT_TABLE}"]
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

  def test_interrupt_ignored_when_table_has_completed
    # Start a run of Ghostferry expecting termination signal to be ignored
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    start_datawriter_with_ghostferry(datawriter, ghostferry)
    stop_datawriter_during_cutover(datawriter, ghostferry)

    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      ghostferry.send_signal("TERM")
    end

    with_env('CI', nil) do
      ghostferry.run

      assert_test_table_is_identical

      assert ghostferry.logrus_lines["ferry"].length > 0

      found_signal = false
      ghostferry.logrus_lines["ferry"].each do |line|
        if line["msg"].start_with?("Received signal: ")
          found_signal = true
          assert line["msg"].match?("Received signal: terminated during cutover. " \
                                    "Refusing to interrupt and will attempt to complete the run.")
        end
      end

      assert(found_signal, "Expected to receive a termination signal")
    end
  end

  def test_interrupt_resume_will_not_emit_binlog_position_for_inline_verifier_if_no_verification_is_used
    datawriter = new_source_datawriter
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

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
    assert_equal "", dumped_state["LastStoredBinlogPositionForInlineVerifier"]["Name"]
    assert_equal 0, dumped_state["LastStoredBinlogPositionForInlineVerifier"]["Pos"]
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
    refute_nil dumped_state["LastStoredBinlogPositionForInlineVerifier"].nil?
    refute_nil dumped_state["BinlogVerifyStore"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]
    refute_nil dumped_state["BinlogVerifyStore"]["gftest"]["test_table_1"]

    # FLAKY: AFTER_BINLOG_APPLY is emitted after the BinlogStreamer
    #        finishes processing all of the event listeners. The
    #        term_and_wait_for_exit above blocks the BinlogStreamer from
    #        processing additional rows but it does not block
    #        InlineVerifier.PeriodicallyVerifyBinlogEvents. This means the
    #        binlog event created in the code above may be verified before the
    #        TERM signal is processed. Thus, the state dump may not contain
    #        this row.
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
    assert error_line["msg"].start_with?("cutover verification failed for: gftest.test_table_1 [paginationKeys: #{chosen_id}")
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

    # This test is intended to observe the actions of a resumed InlineVerifier
    # on a binlog entry that was inserted into the source database while
    # Ghostferry is interrupted. Normally, Ghostferry would correctly apply the
    # binlog entry to the target and the InlineVerifier will verify the row
    # without errors. This means the test will not be able to observe that the
    # InlineVerifier actually tried to verify that row. To observe the action
    # of the InlineVerifier, we artificially corrupt the data on the target,
    # causing the BinlogStreamer to not able to SET data = 'data2' on the
    # target. The InlineVerifier should pick up this case if it is implemented
    # correctly.
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
    assert error_line["msg"].start_with?("cutover verification failed for: gftest.test_table_1 [paginationKeys: #{chosen_id}")
  end

  # originally taken from @kolbitsch-lastline in https://github.com/Shopify/ghostferry/pull/160
  def test_interrupt_resume_between_consecutive_rows_events
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    start_binlog_status = source_db.query('SHOW MASTER STATUS').first

    # create a series of rows-events that do not have interleaved table-map
    # events. This is the case when multiple rows are affected in a single
    # DML event.
    # Since we are racing between applying rows and sending the shutdown event,
    # we emit a whole bunch of them
    num_batches = 10
    num_values_per_batch = 200
    row_id = 0
    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      for _batch_id in 0..num_batches do
        insert_sql = "INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES "
        for value_in_batch in 0..num_values_per_batch do
          row_id += 1
          insert_sql += ", " if value_in_batch > 0
          insert_sql += "('data#{row_id}')"
        end
        source_db.query(insert_sql)
      end
    end

    ghostferry.on_status(Ghostferry::Status::AFTER_BINLOG_APPLY) do
      # while we are emitting events in the loop above, try to inject a shutdown
      # signal, hoping to interrupt between applying an INSERT and receiving the
      # next table-map event
      if row_id > 20
        ghostferry.term_and_wait_for_exit
      end
    end

    dumped_state = ghostferry.run_expecting_interrupt

    refute_nil dumped_state['LastWrittenBinlogPosition']['Name']
    refute_nil dumped_state['LastWrittenBinlogPosition']['Pos']
    refute_nil dumped_state['LastStoredBinlogPositionForInlineVerifier']['Name']
    refute_nil dumped_state['LastStoredBinlogPositionForInlineVerifier']['Pos']
    refute_nil dumped_state['LastStoredBinlogPositionForTargetVerifier']['Name']
    refute_nil dumped_state['LastStoredBinlogPositionForTargetVerifier']['Pos']

    # assert the resumable position is not the start position
    if dumped_state['LastWrittenBinlogPosition']['Name'] == start_binlog_status['File']
      refute_equal dumped_state['LastWrittenBinlogPosition']['Pos'], start_binlog_status['Position']
      refute_equal dumped_state['LastStoredBinlogPositionForInlineVerifier']['Pos'], start_binlog_status['Position']
    end

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    # if we did not resume at a proper state, this invocation of ghostferry
    # will crash, complaining that a rows-event is referring to an unknown
    # table
    ghostferry.run(dumped_state)

    assert_test_table_is_identical
  end

  def test_interrupt_resume_inline_verifier_will_verify_additional_rows_changed_on_target_during_interrupt
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      ghostferry.term_and_wait_for_exit
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)
    refute_nil dumped_state["LastStoredBinlogPositionForTargetVerifier"]

    result = target_db.query("SELECT MIN(id) FROM #{DEFAULT_FULL_TABLE_NAME}")
    chosen_id = result.first["MIN(id)"]

    target_db.query(
      "UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'corrupted' WHERE id = #{chosen_id}",
      annotations: ["not:ghostferry"]
    )

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    ghostferry.run_expecting_interrupt(dumped_state)
    refute_nil ghostferry.error

    assert ghostferry.error["ErrMessage"].include?(
      "row data with paginationKey #{chosen_id} on `gftest`.`test_table_1` has been corrupted"
    )
  end

  def test_interrupt_resume_idempotence
    ghostferry = new_ghostferry_with_interrupt_after_row_copy(MINIMAL_GHOSTFERRY)
    dumped_state = ghostferry.run_expecting_interrupt

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    ghostferry.run_with_logs(dumped_state)

    assert_test_table_is_identical

    # Logs are needed to assert how many times ghostferry successfully completed
    ghostferry.run_with_logs(dumped_state)

    assert_test_table_is_identical

    # assert ghostferry successfuly ran twice
    assert_ghostferry_completed(ghostferry, times: 2)
  end

  def test_interrupt_resume_idempotence_with_writes_to_source
    ghostferry = new_ghostferry_with_interrupt_after_row_copy(MINIMAL_GHOSTFERRY, after_batches_written: 2)

    datawriter = new_source_datawriter
    start_datawriter_with_ghostferry(datawriter, ghostferry)

    dumped_state = ghostferry.run_expecting_interrupt

    assert_basic_fields_exist_in_dumped_state(dumped_state)

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    # stop datawriter in this ghostferry instance as it will run to completion
    stop_datawriter_during_cutover(datawriter, ghostferry)
    ghostferry.run_with_logs(dumped_state)

    assert_test_table_is_identical

    ghostferry.run_with_logs(dumped_state)

    assert_test_table_is_identical
    assert_ghostferry_completed(ghostferry, times: 2)
  end

  def test_interrupt_resume_idempotence_with_multiple_interrupts
    ghostferry = new_ghostferry_with_interrupt_after_row_copy(MINIMAL_GHOSTFERRY, after_batches_written: 2)

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)

    ghostferry = new_ghostferry_with_interrupt_after_row_copy(MINIMAL_GHOSTFERRY, after_batches_written: 2)
    ghostferry.run_expecting_interrupt(dumped_state)

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    ghostferry.run_with_logs(dumped_state)

    assert_test_table_is_identical
    assert_ghostferry_completed(ghostferry, times: 1)
  end

  def test_interrupt_resume_idempotence_with_multiple_interrupts_and_writes_to_source
    ghostferry = new_ghostferry_with_interrupt_after_row_copy(MINIMAL_GHOSTFERRY, after_batches_written: 2)

    datawriter = new_source_datawriter
    start_datawriter_with_ghostferry(datawriter, ghostferry)

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)

    ghostferry = new_ghostferry_with_interrupt_after_row_copy(MINIMAL_GHOSTFERRY, after_batches_written: 2)
    ghostferry.run_expecting_interrupt(dumped_state)

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    stop_datawriter_during_cutover(datawriter, ghostferry)
    ghostferry.run_with_logs(dumped_state)

    assert_test_table_is_identical
    assert_ghostferry_completed(ghostferry, times: 1)
  end

  def test_interrupt_resume_idempotence_with_failure
    ghostferry = new_ghostferry_with_interrupt_after_row_copy(MINIMAL_GHOSTFERRY)
    dumped_state = ghostferry.run_expecting_interrupt

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    # Fail the ghostferry run instead of interrupting a second time
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      ghostferry.kill_and_wait_for_exit
    end
    ghostferry.run_expecting_failure(dumped_state)

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    ghostferry.run_with_logs(dumped_state)

    assert_test_table_is_identical

    assert_ghostferry_completed(ghostferry, times: 1)
  end

  def test_interrupt_resume_idempotence_with_failure_and_writes_to_source
    ghostferry = new_ghostferry_with_interrupt_after_row_copy(MINIMAL_GHOSTFERRY, after_batches_written: 2)

    datawriter = new_source_datawriter
    start_datawriter_with_ghostferry(datawriter, ghostferry)

    dumped_state = ghostferry.run_expecting_interrupt

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    # Fail the ghostferry run instead of interrupting a second time
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      ghostferry.kill_and_wait_for_exit
    end
    ghostferry.run_expecting_failure(dumped_state)

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    stop_datawriter_during_cutover(datawriter, ghostferry)
    ghostferry.run_with_logs(dumped_state)

    assert_test_table_is_identical

    assert_ghostferry_completed(ghostferry, times: 1)
  end

  def test_resume_from_failure_with_state_callback
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    state_from_callback = nil
    ghostferry.on_callback("state") do |state_data|
      state_from_callback = state_data
      ghostferry.kill_and_wait_for_exit
    end

    ghostferry.run_expecting_failure

    refute_nil state_from_callback
    assert_basic_fields_exist_in_dumped_state(state_from_callback)

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)
    ghostferry.run_with_logs(state_from_callback)

    assert_test_table_is_identical
    assert_ghostferry_completed(ghostferry, times: 1)
  end

  # https://github.com/Shopify/ghostferry/issues/149
  def test_issue_149_correct
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    # Writes one batch
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      ghostferry.send_signal("TERM")
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)

    last_pk = dumped_state["LastSuccessfulPaginationKeys"]["#{DEFAULT_DB}.#{DEFAULT_TABLE}"]
    assert last_pk > 200

    # We need to rewind the state backwards, and then change that row on the
    # source. We also need to block the binlog streamer to prevent writing to
    # the target until that row is copied by the BatchWriter. This ensures the
    # following behaviour is tested:
    #
    # The data copier should try to INSERT IGNORE the changed row, which should
    # have no effect on the target. It then should perform a CHECKSUM, which
    # should not fail and should add to the verify queue and keep checking until
    # cutover.  Eventually, the binlog streamer will be unblocked and then it will
    # apply the insert. The verification status should be correct.
    id_to_change = source_db.query("SELECT id FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id <= #{last_pk} ORDER BY id DESC LIMIT 1").first["id"]
    assert id_to_change > 2, "the last row of the batch should have an id of greater than 2, not #{id_to_change}"
    source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'changed' WHERE id = #{id_to_change}")

    data_changed = source_db.query("SELECT data FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = #{id_to_change}").first["data"]
    assert_equal "changed", data_changed

    dumped_state["LastSuccessfulPaginationKeys"]["#{DEFAULT_DB}.#{DEFAULT_TABLE}"] = id_to_change - 1

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    changed_row_copied = false
    start_time = Time.now

    ghostferry.on_status(Ghostferry::Status::BEFORE_BINLOG_APPLY) do
      until changed_row_copied
        raise "timedout waiting for the first batch row to be copied" if Time.now - start_time > 10
        sleep 0.1
      end
    end

    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      changed_row_copied = true
    end

    ghostferry.on_callback("error") do |err|
      raise "Ghostferry crashed with this error: #{err}"
    end

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run(dumped_state)

    assert verification_ran
    assert_equal 0, incorrect_tables.length
    assert_test_table_is_identical

    target_data = target_db.query("SELECT data FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = #{id_to_change}").first["data"]
    assert_equal "changed", target_data
  end

  # https://github.com/Shopify/ghostferry/issues/149
  def test_issue_149_corrupted
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    # Writes one batch
    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      ghostferry.send_signal("TERM")
    end

    dumped_state = ghostferry.run_expecting_interrupt
    assert_basic_fields_exist_in_dumped_state(dumped_state)

    last_pk = dumped_state["LastSuccessfulPaginationKeys"]["#{DEFAULT_DB}.#{DEFAULT_TABLE}"]
    assert last_pk > 200

    # This should be similar to test_issue_149_correct, except we force the
    # BinlogStremer to corrupt the data on the target. We need to check that
    # the InlineVerifier indeed fails this run, if a corruption happens on the
    # row this race condition exists for.
    id_to_change = source_db.query("SELECT id FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id <= #{last_pk} ORDER BY id DESC LIMIT 1").first["id"]
    assert id_to_change > 2, "the last row of the batch should have an id of greater than 2, not #{id_to_change}"
    source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'changed' WHERE id = #{id_to_change}")
    target_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'corrupted' WHERE id = #{id_to_change}")

    data_changed = source_db.query("SELECT data FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = #{id_to_change}").first["data"]
    assert_equal "changed", data_changed

    data_corrupted = target_db.query("SELECT data FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = #{id_to_change}").first["data"]
    assert_equal "corrupted", data_corrupted

    dumped_state["LastSuccessfulPaginationKeys"]["#{DEFAULT_DB}.#{DEFAULT_TABLE}"] = id_to_change - 1

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    changed_row_copied = false
    start_time = Time.now

    ghostferry.on_status(Ghostferry::Status::BEFORE_BINLOG_APPLY) do
      until changed_row_copied
        raise "timedout waiting for the first batch row to be copied" if Time.now - start_time > 10
        sleep 0.1
      end
    end

    ghostferry.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      changed_row_copied = true
    end

    ghostferry.on_callback("error") do |err|
      raise "Ghostferry crashed with this error: #{err}"
    end

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run(dumped_state)

    assert verification_ran
    assert_equal ["#{DEFAULT_DB}.#{DEFAULT_TABLE}"], incorrect_tables
    assert ghostferry.error_lines.last["msg"].start_with?("cutover verification failed for: gftest.test_table_1 [paginationKeys: #{id_to_change}")
  end
end
