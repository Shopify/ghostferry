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
end
