require "test_helper"

class TypesTest < GhostferryTestCase
  JSON_OBJ = '{"data": {"quote": "\\\'", "value": [1]}}'
  EMPTY_JSON = '{}'
  JSON_ARRAY = '[\"test_data\", \"test_data_2\"]'

  def test_json_data_insert
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data JSON, primary key(id))")
    end

    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, '#{JSON_OBJ}')")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (2, '#{JSON_ARRAY}')")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (3, '#{EMPTY_JSON}')")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (4, NULL)")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (5, '#{JSON_OBJ}')")
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (6, NULL)")
    end

    ghostferry.run

    # We cannot use assert_test_table_is_identical because CHECKSUM TABLE
    # with a JSON column is broken on 5.7.
    # See: https://bugs.mysql.com/bug.php?id=87847
    res = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 6, res.first["cnt"]

    expected = [
      {"id"=>1, "data"=>"{\"data\": {\"quote\": \"'\", \"value\": [1]}}"},
      {"id"=>2, "data"=>"[\"test_data\", \"test_data_2\"]"},
      {"id"=>3, "data"=>"{}"},
      {"id"=>4, "data"=>nil},
      {"id"=>5, "data"=>"{\"data\": {\"quote\": \"'\", \"value\": [1]}}"},
      {"id"=>6, "data"=>nil},
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
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, '#{JSON_OBJ}')")
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (2, '#{EMPTY_JSON}')")
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (3, '#{JSON_ARRAY}')")
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (4, NULL)")
    end

    timedout = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      # Need to make sure we don't flush binlogs until we affirmatively see the
      # 3 rows on the target and issue the DELETE statements
      start = Time.now

      loop do
        sleep 0.1
        res = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
        if res.first["cnt"] == 4
          source_db.query("DELETE FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = 1")
          source_db.query("DELETE FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = 2")
          source_db.query("DELETE FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = 3")
          source_db.query("DELETE FROM #{DEFAULT_FULL_TABLE_NAME} WHERE id = 4")
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
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, '#{JSON_OBJ}')")
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (2, '#{EMPTY_JSON}')")
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (3, '#{JSON_ARRAY}')")
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (4, NULL)")
    end

    timedout = false
    ghostferry.on_status(Ghostferry::Status::ROW_COPY_COMPLETED) do
      # Need to make sure we don't flush binlogs until we affirmatively see the
      # 3 rows on the target and issue the DELETE statements
      start = Time.now

      loop do
        sleep 0.1
        res = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
        if res.first["cnt"] == 4
          source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = '#{EMPTY_JSON}' WHERE id = 1")
          source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = '#{JSON_ARRAY}' WHERE id = 2")
          source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = NULL WHERE id = 3")
          source_db.query("UPDATE #{DEFAULT_FULL_TABLE_NAME} SET data = '#{JSON_OBJ}' WHERE id = 4")
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
    assert_equal 4, res.first["cnt"]

    expected = [
      {"id"=>1, "data"=>"{}"},
      {"id"=>2, "data"=>"[\"test_data\", \"test_data_2\"]"},
      {"id"=>3, "data"=>nil},
      {"id"=>4, "data"=>"{\"data\": {\"quote\": \"'\", \"value\": [1]}}"},
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
end
