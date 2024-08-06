require "test_helper"

class InlineVerifierTest < GhostferryTestCase
  INSERT_TRIGGER_NAME = "corrupting_insert_trigger"
  ASCIIDATA = "foobar"
  UTF8MB3DATA = "これは普通なストリングです"
  UTF8MB4DATA = "𠜎𠜱𠝹𠱓𠱸𠲖𠳏𠳕𠴕𠵼𠵿𠸎𠸏𠹷"
  CHARSET_TO_COLLATION = {
    "utf8mb4" => "utf8mb4_unicode_ci",
    "utf8mb3" => "utf8_unicode_ci",
  }

  def teardown
    drop_triggers
  end

  #############################
  # General Integration Tests #
  #############################

  def test_corrupted_insert_is_detected_inline_with_batch_writer
    seed_random_data(source_db, number_of_rows: 3)
    seed_random_data(target_db, number_of_rows: 0)

    result = source_db.query("SELECT id FROM #{DEFAULT_FULL_TABLE_NAME} ORDER BY RAND() LIMIT 1")
    corrupting_id = result.first["id"]

    enable_corrupting_insert_trigger(corrupting_id)

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run

    assert verification_ran
    assert_equal ["#{DEFAULT_DB}.#{DEFAULT_TABLE}"], incorrect_tables
    assert ghostferry.error_lines.last["msg"].start_with?("cutover verification failed for: gftest.test_table_1 [paginationKeys: #{corrupting_id}")
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

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run

    assert verification_ran
    assert_equal ["#{DEFAULT_DB}.#{DEFAULT_TABLE}"], incorrect_tables
    assert ghostferry.error_lines.last["msg"].start_with?("cutover verification failed for: gftest.test_table_1 [paginationKeys: 1")
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

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end
    ghostferry.run

    assert verification_ran
    assert_equal [], incorrect_tables
  end

  def test_different_data_in_ignored_column_passes_inline_verification
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data VARCHAR(255), data2 VARCHAR(255), primary key(id))")
    end

    source_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data1", "same")
    target_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data2", "same")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline", ignored_column: "data" })

    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'data3' WHERE id = 1")
    end

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run

    assert verification_ran
    assert_equal [], incorrect_tables

    rows = source_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 1, rows.count
    rows.each do |row|
      assert_equal 1, row["id"]
      assert_equal "data3", row["data"]
      assert_equal "same", row["data2"]
    end

    rows = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 1, rows.count
    rows.each do |row|
      assert_equal 1, row["id"]
      assert_equal "data2", row["data"]
      assert_equal "same", row["data2"]
    end
  end

  def test_json_data_with_float_numbers_verification_fail
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data JSON, data2 JSON, primary key(id))")
    end

    enable_corrupting_insert_trigger(2, '{\"data\": {\"float\": 100}}')

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY,  config: { verifier_type: "Inline" })

    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data, data2) VALUES ('{\"data\": {\"float\": 32.0}}', '{\"data\": {\"float\": 42.0}}')")
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data, data2) VALUES ('{\"data\": {\"float\": 25.0}}', '{\"data\": {\"float\": 35.0}}')")
    end

    verification_ran = false
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*incorrect_tables|
      verification_ran = true
      assert_equal ["gftest.test_table_1"], incorrect_tables
    end

    ghostferry.run
    assert verification_ran

    assert ghostferry.error_lines.last["msg"].start_with?("cutover verification failed for: gftest.test_table_1 [paginationKeys: 2")
  end

  def test_json_data_with_float_numbers_verification
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data JSON, data2 JSON, primary key(id))")
    end

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY,  config: { verifier_type: "Inline" })

    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data, data2) VALUES ('{\"data\": {\"float\": 32.0}}', '{\"data\": {\"float\": 42.0}}')")
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data, data2) VALUES ('{\"data\": {\"float\": 25.0}}', '{\"data\": {\"float\": 35.0}}')")
    end

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run

    assert_nil ghostferry.error
    assert verification_ran
    assert_equal [], incorrect_tables
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
    assert ghostferry.error_lines.last["msg"].start_with?("cutover verification failed for: gftest.test_table_1 [paginationKeys: #{corrupting_id}")
  end

  def test_target_corruption_is_ignored_if_skip_target_verification
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data VARCHAR(255), data2 VARCHAR(255), primary key(id))")
    end

    source_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data1", "same")
    target_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data2", "same")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline", ignored_column: "data", skip_target_verification: "true" })

    corrupting_id = 1
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      target_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'data5' WHERE id = #{corrupting_id}", exclude_marginalia: true)
    end

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run

    assert_nil ghostferry.error
    assert verification_ran
    assert_equal [], incorrect_tables
  end

  def test_target_corruption_is_detected
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data VARCHAR(255), data2 VARCHAR(255), primary key(id))")
    end

    source_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data1", "same")
    target_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data2", "same")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline", ignored_column: "data" })

    corrupting_id = 1
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      target_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'data5' WHERE id = #{corrupting_id}", exclude_marginalia: true)
    end

    ghostferry.run_expecting_interrupt
    refute_nil ghostferry.error

    err_msg = ghostferry.error["ErrMessage"]
    assert err_msg.include?("row data with paginationKey #{corrupting_id} on #{DEFAULT_FULL_TABLE_NAME} has been corrupted"), message: err_msg
  end

  def test_target_modification_with_annotation
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data VARCHAR(255), data2 VARCHAR(255), primary key(id))")
    end

    source_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data1", "same")
    target_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data2", "same")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline", ignored_column: "data" })

    corrupting_id = 1
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      target_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'data3' WHERE id = #{corrupting_id}")
    end

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run

    assert_nil ghostferry.error
    assert verification_ran
    assert_equal [], incorrect_tables
  end

  def test_target_modification_with_different_annotation
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data VARCHAR(255), data2 VARCHAR(255), primary key(id))")
    end

    source_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data1", "same")
    target_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data2", "same")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline", ignored_column: "data" })

    corrupting_id = 1
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      target_db.query(
        "UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'data3' WHERE id = #{corrupting_id}",
        annotations: ["application:not_ghostferry"]
      )
    end

    ghostferry.run_expecting_interrupt
    refute_nil ghostferry.error

    err_msg = ghostferry.error["ErrMessage"]
    assert err_msg.include?("row data with paginationKey #{corrupting_id} on #{DEFAULT_FULL_TABLE_NAME} has been corrupted"), message: err_msg
  end

  def test_target_modification_with_multiple_annotations
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data VARCHAR(255), data2 VARCHAR(255), primary key(id))")
    end

    source_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data1", "same")
    target_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data2", "same")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline", ignored_column: "data" })

    corrupting_id = 1
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      target_db.query(
        "UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'data3' WHERE id = #{corrupting_id}",
        annotations: [DEFAULT_ANNOTATION, "other:annotation"]
      )
    end

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run

    assert_nil ghostferry.error
    assert verification_ran
    assert_equal [], incorrect_tables
  end

  def test_target_modification_with_incorrect_annotation_order
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data VARCHAR(255), data2 VARCHAR(255), primary key(id))")
    end

    source_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data1", "same")
    target_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data2", "same")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline", ignored_column: "data" })

    corrupting_id = 1
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      target_db.query(
        "UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'data3' WHERE id = #{corrupting_id}",
        annotations: ["other:annotation", DEFAULT_ANNOTATION]
      )
    end

    ghostferry.run_expecting_interrupt
    refute_nil ghostferry.error

    err_msg = ghostferry.error["ErrMessage"]
    assert err_msg.include?("row data with paginationKey #{corrupting_id} on #{DEFAULT_FULL_TABLE_NAME} has been corrupted"), message: err_msg
  end

  def test_target_modification_with_annotation_at_end_of_statement
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data VARCHAR(255), data2 VARCHAR(255), primary key(id))")
    end

    source_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data1", "same")
    target_db.prepare("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data, data2) VALUES (?, ?, ?)").execute(1, "data2", "same")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline", ignored_column: "data" })

    corrupting_id = 1
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      target_db.query(
        "UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'data3' WHERE id = #{corrupting_id} /*#{DEFAULT_ANNOTATION}*/",
        annotations: []
      )
    end

    ghostferry.run_expecting_interrupt
    refute_nil ghostferry.error

    err_msg = ghostferry.error["ErrMessage"]
    assert err_msg.include?("row data with paginationKey #{corrupting_id} on #{DEFAULT_FULL_TABLE_NAME} has been corrupted"), message: err_msg
  end

  #######################
  # Special values test #
  #######################

  def test_catches_binlog_streamer_corruption_with_composite_pk
    seed_random_data_in_composite_pk_table(source_db, number_of_rows: 1)
    seed_random_data_in_composite_pk_table(target_db, number_of_rows: 0)

    result = source_db.query("SELECT id FROM #{DEFAULT_FULL_TABLE_NAME} LIMIT 1")
    corrupting_id = result.first["id"] + 1
    enable_corrupting_insert_trigger(corrupting_id)

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: {
      verifier_type: "Inline",
      cascading_pagination_column_config: {
        PerTable: {
          DEFAULT_DB => {
            DEFAULT_TABLE => "id"
          },
        },
      }.to_json,
    })

    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, id2, data) VALUES (#{corrupting_id}, 10, 'data')")
    end

    verification_ran = false
    incorrect_tables_found = false
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*incorrect_tables|
      verification_ran = true

      # Don't want to assert_equal here as it causes the ghostferry process to crash and mess up the error message
      incorrect_tables_found = ["#{DEFAULT_DB}.#{DEFAULT_TABLE}"] == incorrect_tables
    end

    ghostferry.run
    assert verification_ran
    assert incorrect_tables_found, "verification did not catch corrupted table"
    assert ghostferry.error_lines.last["msg"].start_with?("cutover verification failed for: #{DEFAULT_DB}.#{DEFAULT_TABLE} [paginationKeys: #{corrupting_id}")
  end

  def test_positive_negative_zero
    [source_db, target_db].each do |db|
      seed_random_data(db, number_of_rows: 0)
      db.query("ALTER TABLE #{DEFAULT_FULL_TABLE_NAME} MODIFY data FLOAT")
    end

    # If the data already exists on the target, Ghostferry's INSERT IGNORE will
    # not insert again. However, the verifier should run.
    # We first set the values to be different to ensure the InlineVerifier is
    # indeed running as the nominal case (comparing 0.0 and -0.0) should not
    # emit any error and thus we cannot say for certain if the InlineVerifier
    # ran or not.
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, 0.0)")
    target_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, 1.0)")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run

    assert verification_ran
    assert_equal ["#{DEFAULT_DB}.#{DEFAULT_TABLE}"], incorrect_tables
    assert ghostferry.error_lines.last["msg"].start_with?("cutover verification failed for: #{DEFAULT_DB}.#{DEFAULT_TABLE} [paginationKeys: 1")

    # Now we run the real test case.
    target_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = -0.0 WHERE id = 1")

    verification_ran = false
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*incorrect_tables|
      verification_ran = true
      assert_equal [], incorrect_tables
    end

    ghostferry.run
    assert verification_ran
  end

  def test_null_vs_null
    seed_random_data(source_db, number_of_rows: 0)
    seed_random_data(target_db, number_of_rows: 0)

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, NULL)")
    target_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, NULL)")

    verification_ran = false
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*incorrect_tables|
      verification_ran = true
      assert_equal [], incorrect_tables
    end

    ghostferry.run
    assert verification_ran
  end

  def test_null_vs_empty_string
    seed_random_data(source_db, number_of_rows: 0)
    seed_random_data(target_db, number_of_rows: 0)

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, NULL)")
    target_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, '')")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run

    assert verification_ran
    assert_equal ["#{DEFAULT_DB}.#{DEFAULT_TABLE}"], incorrect_tables
    assert ghostferry.error_lines.last["msg"].start_with?("cutover verification failed for: gftest.test_table_1 [paginationKeys: 1")
  end

  def test_null_vs_null_string
    seed_random_data(source_db, number_of_rows: 0)
    seed_random_data(target_db, number_of_rows: 0)

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, NULL)")
    target_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, 'NULL')")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run

    assert verification_ran
    assert_equal ["#{DEFAULT_DB}.#{DEFAULT_TABLE}"], incorrect_tables
    assert ghostferry.error_lines.last["msg"].start_with?("cutover verification failed for: gftest.test_table_1 [paginationKeys: 1")
  end

  def test_null_in_different_order
    seed_random_data(source_db, number_of_rows: 0)
    seed_random_data(target_db, number_of_rows: 0)

    source_db.query("ALTER TABLE #{DEFAULT_FULL_TABLE_NAME} ADD COLUMN data2 VARCHAR(255) AFTER data")
    target_db.query("ALTER TABLE #{DEFAULT_FULL_TABLE_NAME} ADD COLUMN data2 VARCHAR(255) AFTER data")

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, NULL, 'data')")
    target_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, 'data', NULL)")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    verification_ran = false
    incorrect_tables = []
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*tables|
      verification_ran = true
      incorrect_tables = tables
    end

    ghostferry.run

    assert verification_ran
    assert_equal ["#{DEFAULT_DB}.#{DEFAULT_TABLE}"], incorrect_tables
    assert ghostferry.error_lines.last["msg"].start_with?("cutover verification failed for: gftest.test_table_1 [paginationKeys: 1")
  end

  ###################
  # Collation Tests #
  ###################

  def test_ascii_data_from_utfmb3_to_utfmb4
    run_collation_test(ASCIIDATA, "utf8mb3", "utf8mb4", identical: true)
  end

  def test_ascii_data_from_utfmb4_to_utfmb3
    run_collation_test(ASCIIDATA, "utf8mb4", "utf8mb3", identical: true)
  end

  def test_utfmb3_data_from_utfmb3_to_utfmb4
    run_collation_test(UTF8MB3DATA, "utf8mb3", "utf8mb4", identical: true)
  end

  def test_utfmb3_data_from_utfmb4_to_utfmb3
    run_collation_test(UTF8MB3DATA, "utf8mb4", "utf8mb3", identical: true)
  end

  # skip on MySQL 8
  # More details at
  # https://github.com/Shopify/ghostferry/pull/328#discussion_r791197939
  def test_utfmb4_data_from_utfmb4_to_utfmb3
    run_collation_test(UTF8MB4DATA, "utf8mb4", "utf8mb3", identical: false)
  end unless ENV['MYSQL_VERSION'] == '8.0'

  private

  def run_collation_test(data, source_charset, target_charset, identical:)
    seed_random_data(source_db, number_of_rows: 0)
    seed_random_data(target_db, number_of_rows: 0)

    unsafe_source_db_config = source_db_config
    unsafe_source_db_config[:init_command] = "SET @@SESSION.sql_mode = ''"
    unsafe_source_db = Mysql2::Client.new(unsafe_source_db_config)

    unsafe_target_db_config = target_db_config
    unsafe_target_db_config[:init_command] = "SET @@SESSION.sql_mode = ''"
    unsafe_target_db = Mysql2::Client.new(unsafe_target_db_config)

    set_data_column_collation(unsafe_source_db, source_charset)
    set_data_column_collation(unsafe_target_db, target_charset)

    unsafe_source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (1, '#{data}')")

    verify_during_cutover_ran = false
    incorrect_tables = nil
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*t|
      verify_during_cutover_ran = true
      incorrect_tables = t
    end

    if identical
      ghostferry.run
      assert verify_during_cutover_ran
      assert_equal [], incorrect_tables

      rows = unsafe_target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = 1")
      assert_equal 1, rows.count
      rows.each do |row|
        assert_equal data, row["data"]
      end
    else
      ghostferry.run

      assert verify_during_cutover_ran
      assert_equal ["#{DEFAULT_DB}.#{DEFAULT_TABLE}"], incorrect_tables
      assert ghostferry.error_lines.last["msg"].start_with?("cutover verification failed for: gftest.test_table_1 [paginationKeys: 1")
    end
  end

  def set_data_column_collation(db, charset)
    db.query("ALTER TABLE #{DEFAULT_FULL_TABLE_NAME} MODIFY data VARCHAR(255) CHARACTER SET #{charset} COLLATE #{CHARSET_TO_COLLATION[charset]}")
  end

  def enable_corrupting_insert_trigger(corrupting_id, new_data = "corrupted")
    query = [
      "CREATE TRIGGER #{INSERT_TRIGGER_NAME} BEFORE INSERT ON #{DEFAULT_TABLE}",
      "FOR EACH ROW BEGIN",
      "IF NEW.id = #{corrupting_id} THEN",
      "SET NEW.data = '#{new_data}';",
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

  def seed_random_data_in_composite_pk_table(connection, database_name: DEFAULT_DB, table_name: DEFAULT_TABLE, number_of_rows: 1111)
    dbtable = full_table_name(database_name, table_name)

    connection.query("CREATE DATABASE IF NOT EXISTS #{database_name}")
    connection.query("CREATE TABLE IF NOT EXISTS #{dbtable} (id bigint(20) not null auto_increment, id2 bigint(20) not null, data TEXT, primary key(id2, id), INDEX `id` (id))")

    transaction(connection) do
      insert_statement = connection.prepare("INSERT INTO #{dbtable} (id, id2, data) VALUES (?, 10, ?)")

      number_of_rows.times do
        insert_statement.execute(nil, rand_data)
      end
    end
  end
end
