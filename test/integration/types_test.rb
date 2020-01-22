require "test_helper"

class TypesTest < GhostferryTestCase
  JSON_OBJ = '{"data": {"quote": "\\\'", "value": [1]}}'
  EMPTY_JSON = '{}'
  JSON_ARRAY = '[\"test_data\", \"test_data_2\"]'
  JSON_NULL = 'null'
  JSON_TRUE = 'true'
  JSON_FALSE = 'false'
  JSON_NUMBER = '42'

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
end
