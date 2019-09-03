require "test_helper"

class InlineVerifierTest < GhostferryTestCase
  INSERT_TRIGGER_NAME = "corrupting_insert_trigger"

  def teardown
    drop_triggers
  end

  def test_corrupted_insert_is_detected_inline_with_batch_writer
    seed_random_data(source_db, number_of_rows: 3)
    seed_random_data(target_db, number_of_rows: 0)

    result = source_db.query("SELECT id FROM #{DEFAULT_FULL_TABLE_NAME} ORDER BY RAND() LIMIT 1")
    corrupting_id = result.first["id"]

    enable_corrupting_insert_trigger(corrupting_id)

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    ghostferry.run_expecting_interrupt

    refute_nil ghostferry.error
    err_msg = ghostferry.error["ErrMessage"]
    assert err_msg.include?("row fingerprints for pks [#{corrupting_id}] on #{DEFAULT_DB}.#{DEFAULT_TABLE} do not match"), message: err_msg

    # Make sure it is not inserted into the target
    results = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = #{corrupting_id}")
    assert_equal 0, results.count
  end

  def test_different_compressed_data_is_detected_inline_with_batch_writer
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data BLOB, primary key(id))")
    end

    compressed_data1 = "\x08" + "\x0cabcd" + "\x01\x02" # abcdcdcd
    compressed_data2 = "\x08" + "\x0cabcd" + "\x01\x01" # abcddddd

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (1, _binary'#{compressed_data1}')")
    target_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (1, _binary'#{compressed_data2}')")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline", compressed_data: true })
    ghostferry.run_expecting_interrupt

    refute_nil ghostferry.error
    err_msg = ghostferry.error["ErrMessage"]
    assert err_msg.include?("row fingerprints for pks [1] on #{DEFAULT_DB}.#{DEFAULT_TABLE} do not match"), message: err_msg
  end

  def test_same_decompressed_data_different_compressed_test_passes_inline_verification
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data BLOB, primary key(id))")
    end

    compressed_data1 = load_fixture("urls1.snappy")
    compressed_data2 = load_fixture("urls2.snappy")

    source_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (?, ?)").execute(1, compressed_data1)
    target_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (?, ?)").execute(1, compressed_data2)

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline", compressed_data: true })
    ghostferry.run

    assert_nil ghostferry.error
  end

  def test_catches_binlog_streamer_corruption
    seed_random_data(source_db, number_of_rows: 1)
    seed_random_data(target_db, number_of_rows: 0)

    result = source_db.query("SELECT id FROM #{DEFAULT_FULL_TABLE_NAME} LIMIT 1")
    corrupting_id = result.first["id"] + 1
    enable_corrupting_insert_trigger(corrupting_id)

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (#{corrupting_id}, 'data')")
    end

    verification_ran = false
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*incorrect_tables|
      verification_ran = true
      assert_equal ["gftest.test_table_1"], incorrect_tables
    end

    ghostferry.run
    assert verification_ran
    assert_equal "cutover verification failed for: gftest.test_table_1 [pks: #{corrupting_id} ] ", ghostferry.error_lines.last["msg"]
  end

  private

  def enable_corrupting_insert_trigger(corrupting_id)
    query = [
      "CREATE TRIGGER #{INSERT_TRIGGER_NAME} BEFORE INSERT ON #{DEFAULT_TABLE}",
      "FOR EACH ROW BEGIN",
      "IF NEW.id = #{corrupting_id} THEN",
      "SET NEW.data = 'corrupted';",
      "END IF;",
      "END",
    ].join("\n")

    target_db_conn_with_db_selected.query(query)
  end

  def drop_triggers
    target_db_conn_with_db_selected.query("DROP TRIGGER IF EXISTS #{INSERT_TRIGGER_NAME}")
  end

  def target_db_conn_with_db_selected
    @target_db_conn_with_db_selected ||= begin
      conf = target_db_config
      conf[:database] = DEFAULT_DB
      Mysql2::Client.new(conf)
    end
  end
end
