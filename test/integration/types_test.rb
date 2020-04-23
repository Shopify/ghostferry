require "test_helper"

class TypesTest < GhostferryTestCase
  JSON_OBJ = '{"data": {"quote": "\\\'", "value": [1]}}'
  EMPTY_JSON = '{}'
  JSON_ARRAY = '[\"test_data\", \"test_data_2\"]'
  JSON_NULL = 'null'
  JSON_TRUE = 'true'
  JSON_FALSE = 'false'
  JSON_NUMBER = '42'

  def test_json_colum_not_null_with_no_default_is_invalid_this_is_fine
    # See: https://bugs.mysql.com/bug.php?id=98496

    # source_db and target_db are global for the test, we don't want to activate
    # non strict mode for that and leak over to other tests.
    nonstrict_source_db = Mysql2::Client.new(source_db_config)
    nonstrict_target_db = Mysql2::Client.new(target_db_config)

    [nonstrict_source_db, nonstrict_target_db].each do |db|
      db.query("set session sql_mode=''")
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data JSON NOT NULL, primary key(id))")
    end

    nonstrict_source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id) VALUES (1)")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: { verifier_type: "Inline" })

    verification_ran = false
    incorrect_tables_found = true
    ghostferry.on_status(Ghostferry::Status::VERIFIED) do |*incorrect_tables|
      verification_ran = true

      # Don't want to assert_equal here as it causes the ghostferry process to crash and mess up the error message
      incorrect_tables_found = ["#{DEFAULT_DB}.#{DEFAULT_TABLE}"] == incorrect_tables
    end

    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      nonstrict_source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id) VALUES (2)")
      nonstrict_source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id) VALUES (3)")
      nonstrict_source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id) VALUES (4)")
    end

    timedout = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      # Need to make sure we don't flush binlogs until we affirmatively see the
      # 3 rows on the target and issue the DELETE statements
      start = Time.now

      loop do
        sleep 0.1
        res = nonstrict_target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
        if res.first["cnt"] == 4
          nonstrict_source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'true' WHERE id = 2")
          nonstrict_source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 'null' WHERE id = 3")
          nonstrict_source_db.query("DELETE FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = 4")
          break
        end

        if Time.now - start > 10
          timedout = true
          break
        end
      end
    end

    ghostferry.run

    assert verification_ran
    refute incorrect_tables_found, "data should be correctly copied but verifier says it is not"

    res = nonstrict_target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 3, res.first["cnt"]

    expected = [
      {"id"=>1, "data"=>"null"},
      {"id"=>2, "data"=>"true"},
      {"id"=>3, "data"=>"null"},
    ]

    res = nonstrict_target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME} ORDER BY id ASC")
    res.zip(expected).each do |row, expected_row|
      assert_equal expected_row, row
    end
  end

  def test_json_data_insert
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data JSON, primary key(id))")
    end

    insert_json_on_source

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      insert_json_on_source
    end

    ghostferry.run

    # We cannot use assert_test_table_is_identical because CHECKSUM TABLE
    # with a JSON column is broken on 5.7.
    # See: https://bugs.mysql.com/bug.php?id=87847
    res = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 16, res.first["cnt"]

    expected = [
      {"id"=>1, "data"=>"{\"data\": {\"quote\": \"'\", \"value\": [1]}}"},
      {"id"=>2, "data"=>"[\"test_data\", \"test_data_2\"]"},
      {"id"=>3, "data"=>"{}"},
      {"id"=>4, "data"=>nil},
      {"id"=>5, "data"=>"null"},
      {"id"=>6, "data"=>"true"},
      {"id"=>7, "data"=>"false"},
      {"id"=>8, "data"=>"42"},

      {"id"=>9, "data"=>"{\"data\": {\"quote\": \"'\", \"value\": [1]}}"},
      {"id"=>10, "data"=>"[\"test_data\", \"test_data_2\"]"},
      {"id"=>11, "data"=>"{}"},
      {"id"=>12, "data"=>nil},
      {"id"=>13, "data"=>"null"},
      {"id"=>14, "data"=>"true"},
      {"id"=>15, "data"=>"false"},
      {"id"=>16, "data"=>"42"},
    ]

    res = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME} ORDER BY id ASC")
    res.zip(expected).each do |row, expected_row|
      assert_equal expected_row, row
    end
  end

  def test_json_data_delete
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data JSON, primary key(id))")
    end

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      insert_json_on_source
    end

    timedout = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      # Need to make sure we don't flush binlogs until we affirmatively see the
      # 3 rows on the target and issue the DELETE statements
      start = Time.now

      loop do
        sleep 0.1
        res = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
        if res.first["cnt"] == 8
          1.upto(8) do |i|
            source_db.query("DELETE FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = #{i}")
          end
          break
        end

        if Time.now - start > 10
          timedout = true
          break
        end
      end
    end

    ghostferry.run
    refute timedout, "failed due to time out while waiting for the 4 insert binlogs to be written to the target"

    res = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 0, res.first["cnt"]
  end

  def test_json_data_update
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data JSON, primary key(id))")
    end

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      insert_json_on_source
    end

    timedout = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      # Need to make sure we don't flush binlogs until we affirmatively see the
      # 3 rows on the target and issue the DELETE statements
      start = Time.now

      loop do
        sleep 0.1
        res = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
        if res.first["cnt"] == 8
          source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = '#{EMPTY_JSON}' WHERE id = 1")
          source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = '#{JSON_ARRAY}' WHERE id = 2")
          source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = NULL WHERE id = 3")
          source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = '#{JSON_OBJ}' WHERE id = 4")
          source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = '#{JSON_TRUE}' WHERE id = 5")
          source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = '#{JSON_FALSE}' WHERE id = 6")
          source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = '#{JSON_NUMBER}' WHERE id = 7")
          source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = '#{JSON_NULL}' WHERE id = 8")
          break
        end

        if Time.now - start > 10
          timedout = true
          break
        end
      end
    end

    ghostferry.run
    refute timedout, "failed due to time out while waiting for the 4 insert binlogs to be written to the target"

    res = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 8, res.first["cnt"]

    expected = [
      {"id"=>1, "data"=>"{}"},
      {"id"=>2, "data"=>"[\"test_data\", \"test_data_2\"]"},
      {"id"=>3, "data"=>nil},
      {"id"=>4, "data"=>"{\"data\": {\"quote\": \"'\", \"value\": [1]}}"},
      {"id"=>5, "data"=>"true"},
      {"id"=>6, "data"=>"false"},
      {"id"=>7, "data"=>"42"},
      {"id"=>8, "data"=>"null"},
    ]

    res = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME} ORDER BY id ASC")
    res.zip(expected).each do |row, expected_row|
      assert_equal expected_row, row
    end
  end

  def test_escaped_data
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data1 TEXT, data2 VARCHAR(255), data3 BLOB, primary key(id))")
    end

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data1, data2, data3) VALUES (1, '''', '''', _binary'''')")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data1, data2, data3) VALUES (2, '''', '''', _binary'''')")
      source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data1 = 'test', data2 = 'test', data3 = _binary'test' WHERE id = 1")
      source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data1 = '''', data2 = '''', data3 = _binary'''' WHERE id = 1")
      source_db.query("DELETE FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = 2")
    end

    ghostferry.run

    assert_test_table_is_identical
    res = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 1, res.count
    res.each do |row|
      assert_equal 1, row["id"]
      assert_equal "'", row["data1"]
      assert_equal "'", row["data2"]
      assert_equal "'", row["data3"]
    end
  end

  def test_decimal
    # decimals are treated specially in binlog writing (they are inserted after
    # conversion to string), so we add this test to make sure we don't corrupt
    # data in the process
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data decimal, primary key(id))")
    end

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (1, 2)")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    row_copy_called = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      # this hook follows the design in the helper method
      # execute_copy_data_in_fixed_size_binary_column below. See detailed
      # comments there
      res = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME}")
      assert_equal 1, res.count
      res.each do |row|
        assert_equal 1, row["id"]
        assert_equal 2, row["data"]
      end

      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (3, 4)")
      source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = 5 WHERE id = 1")

      row_copy_called = true
    end

    ghostferry.run

    assert row_copy_called
    assert_test_table_is_identical
    res = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 2, res.count
    res.each do |row|
      if row["id"] == 1
        assert_equal 5, row["data"]
      else
        assert_equal 3, row["id"]
        assert_equal 4, row["data"]
      end
    end
  end

  def test_copy_data_in_fixed_size_binary_column
    # NOTE: We explicitly test with a value that is shorter than the max column
    # size - MySQL will 0-pad the value up the full length of the BINARY column,
    # but the MySQL replication binlogs will *not* contain these 0-bytes.
    #
    # As a result, the binlog writer must explicitly add then when building
    # update/delete statements, as the WHERE clause would not match existing
    # rows in the target DB
    inserted_data = "ABC"
    execute_copy_data_in_fixed_size_binary_column(
        column_size: 4,
        inserted_data: inserted_data,
        expected_inserted_data: "#{inserted_data}\x00",
        updated_data: "EFGH"
    )
  end

  def test_copy_data_in_fixed_size_binary_column__value_completely_filled
    # NOTE: This test is interesting (beyond what is covered above already),
    # because it seems the server strips the trailing 0-bytes before sending
    # them to the binlog.
    inserted_data = "ABC\x00"
    execute_copy_data_in_fixed_size_binary_column(
        column_size: 4,
        inserted_data: inserted_data,
        expected_inserted_data: inserted_data,
        updated_data: "EFGH"
    )
  end

  def test_copy_data_in_fixed_size_binary_column__length1
    # slight variation to cover the corner-case where there is no data in the
    # column at all and the entire value is 0-padded (here, only 1 byte)
    execute_copy_data_in_fixed_size_binary_column(
        column_size: 1,
        inserted_data: "",
        expected_inserted_data: "\x00",
        updated_data: "A"
    )
  end

  private

  def insert_json_on_source
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES ('#{JSON_OBJ}')")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES ('#{JSON_ARRAY}')")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES ('#{EMPTY_JSON}')")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES (NULL)")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES ('#{JSON_NULL}')")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES ('#{JSON_TRUE}')")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES ('#{JSON_FALSE}')")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (data) VALUES ('#{JSON_NUMBER}')")
  end

  def execute_copy_data_in_fixed_size_binary_column(column_size:, inserted_data:, expected_inserted_data:, updated_data:)
    # test for the BINARY columns needing 0-byte padding
    #
    # For details, see https://github.com/Shopify/ghostferry/pull/159

    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data BINARY(#{column_size}), primary key(id))")
    end

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} (id, data) VALUES (1, _binary'#{inserted_data}')")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    row_copy_called = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      # select row from the target and then make sure the data with 0 padding
      # is present. We do this to make sure there are no races in the test
      res = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME}")
      assert_equal 1, res.count
      res.each do |row|
        assert_equal 1, row["id"]
        assert_equal expected_inserted_data, row["data"]
      end

      # now that the target is guaranteed to be in the same state as the
      # source, trigger an update that will cause the binlog to stream an
      # entry that needs the 0-byte padding
      #
      # NOTE: If we use BINLOG_STREAMING_STARTED as hook instead, we race
      # with getting the update into the batch-copy instead of into the
      # streaming
      source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = _binary'#{updated_data}' WHERE id = 1")

      # NOTE: We move this flag to the end of the callback to make sure that
      # we don't confuse ourselves if the callback crashes before completing
      row_copy_called = true
    end

    ghostferry.run

    # make sure the test framework called the expected hooks above - otherwise
    # the test doesn't make much sense
    assert row_copy_called
    assert_test_table_is_identical

    # just being paranoid here: make sure the test outcome is as expected. It
    # should be, since we made sure the tables have the same checksums, but it
    # helps understand what the test code does
    res = target_db.query("SELECT * FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 1, res.count
    res.each do |row|
      assert_equal 1, row["id"]
      assert_equal updated_data, row["data"]
    end
  end
end
