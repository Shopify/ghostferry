require "test_helper"

class TypesTest < GhostferryTestCase
  def test_json_data
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{DEFAULT_DB}")
      db.query("CREATE TABLE IF NOT EXISTS #{DEFAULT_FULL_TABLE_NAME} (id bigint(20) not null auto_increment, data JSON, primary key(id))")
    end

    json_obj = '{"data": {"quote": "\\\'", "value": [1]}}'
    empty_json = '{}'
    json_array = '[\"test_data\", \"test_data_2\"]'
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (1, '#{json_obj}')")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (2, '#{json_array}')")
    source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (3, '#{empty_json}')")

    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY)

    ghostferry.on_status(Ghostferry::Status::BINLOG_STREAMING_STARTED) do
      source_db.query("INSERT INTO #{DEFAULT_FULL_TABLE_NAME} VALUES (4, '#{json_obj}')")
    end

    ghostferry.run

    # We cannot use assert_test_table_is_identical because CHECKSUM TABLE
    # with a JSON column is broken on 5.7.
    # See: https://bugs.mysql.com/bug.php?id=87847
    res = target_db.query("SELECT COUNT(*) AS cnt FROM #{DEFAULT_FULL_TABLE_NAME}")
    assert_equal 4, res.first["cnt"]

    expected = [
      {"id"=>1, "data"=>"{\"data\": {\"quote\": \"'\", \"value\": [1]}}"},
      {"id"=>2, "data"=>"[\"test_data\", \"test_data_2\"]"},
      {"id"=>3, "data"=>"{}"},
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
