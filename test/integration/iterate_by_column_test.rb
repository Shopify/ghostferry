require "test_helper"

class IterateByColumnTest < GhostferryTestCase
  def test_specifying_pagination_column_by_per_table_succeeds
    pagination_key_column = 'some_column'
    db_name = DEFAULT_DB
    table_name = DEFAULT_TABLE

    create_tables(db_name, table_name, "(#{pagination_key_column} bigint(20), data BLOB)")
    seed_dynamically(DbHelper.full_table_name(db_name, table_name), {
      pagination_key_column => lambda {|i| i},
      data: lambda {|_| "'some data'"}
    }, amount_rows: 10)

    assert_ghostferry_run(per_table: {
      db_name => {
        table_name => pagination_key_column,
      },
    })
    assert_source_target_eql_data(DbHelper.full_table_name(db_name, table_name))
  end

  def test_specifying_pagination_column_by_fallback_column_succeeds
    pagination_key_column = 'some_column'
    db_name = DEFAULT_DB
    table_name = DEFAULT_TABLE

    create_tables(db_name, table_name, "(#{pagination_key_column} bigint(20), data BLOB)")
    seed_dynamically(DbHelper.full_table_name(db_name, table_name), {
      pagination_key_column => lambda {|i| i},
      data: lambda {|_| "'some data'"}
    }, amount_rows: 10)

    assert_ghostferry_run(fallback_column: pagination_key_column)
    assert_source_target_eql_data(DbHelper.full_table_name(db_name, table_name))
  end

  def test_specifying_pagination_column_by_per_table_and_fallback_column_succeeds
    pagination_key_column_1 = 'some_column'
    pagination_key_column_2 = 'other_column'

    db_name = DEFAULT_DB
    table_name_1 = DEFAULT_TABLE
    table_name_2 = "other_table"

    create_tables(db_name, table_name_2, "(#{pagination_key_column_2} bigint(20), data BLOB)")
    # Following creates composite index: other_id, some_column
    create_tables(db_name, table_name_1, "(#{pagination_key_column_1} bigint(20), other_id bigint(20), data BLOB, primary key(other_id, #{pagination_key_column_1}))")


    seed_dynamically(DbHelper.full_table_name(db_name, table_name_1), {
      pagination_key_column_1 => lambda {|i| i},
      other_id: lambda {|i| i},
      data: lambda {|_| "'some data'"}
    }, amount_rows: 10)

    seed_dynamically(DbHelper.full_table_name(db_name, table_name_2), {
      pagination_key_column_2 => lambda {|i| i},
      data: lambda {|_| "'some data'"}
    }, amount_rows: 10)

    assert_ghostferry_run(per_table: {
      db_name => {
        table_name_1 => pagination_key_column_1,
      },
    }, fallback_column: pagination_key_column_2)

    assert_source_target_eql_data(DbHelper.full_table_name(db_name, table_name_1))

    data_at_source_db = source_db.query("SELECT * FROM #{DbHelper.full_table_name(db_name, table_name_2)} ORDER BY other_column")
    data_at_target_db = target_db.query("SELECT * FROM #{DbHelper.full_table_name(db_name, table_name_2)} ORDER BY other_column")

    assert_equal data_at_source_db.to_a, data_at_target_db.to_a
  end

  private
  def create_tables(db_name, table_name, create_query)
    [source_db, target_db].each do |db|
      db.query("CREATE DATABASE IF NOT EXISTS #{db_name}")
      db.query("CREATE TABLE IF NOT EXISTS #{DbHelper.full_table_name(db_name, table_name)} #{create_query}")
    end
  end

  def seed_dynamically(full_table_name, columns, amount_rows:)
    (1..amount_rows).map do |i|
      column_names = columns.keys.join(",")
      column_values = columns.values.map {|l| l.(i)}.join(",")

      "(#{column_names}) VALUES (#{column_values})"
    end.each do |insert_query_portion|
      source_db.query("INSERT INTO #{full_table_name} #{insert_query_portion}")
    end
  end

  def cascadingPaginationColumnConfig(per_table: nil, fallback_column: nil)
    {
      PerTable: per_table,
      FallbackColumn: fallback_column
    }.reduce({}) do |a,(k,v)|
      if v
        a.merge!({k => v})
      end
      a
    end.to_json
  end

  def assert_source_target_eql_data(full_table_name)
    data_at_source_db = source_db.query("SELECT * FROM #{full_table_name} ORDER BY some_column")
    data_at_target_db = target_db.query("SELECT * FROM #{full_table_name} ORDER BY some_column")

    assert_equal data_at_source_db.to_a, data_at_target_db.to_a
  end

  def assert_ghostferry_run(per_table: nil, fallback_column: nil)
    ghostferry = new_ghostferry(MINIMAL_GHOSTFERRY, config: {
      cascading_pagination_column_config: cascadingPaginationColumnConfig(per_table: per_table, fallback_column: fallback_column)
    })
    ghostferry.run

    assert_nil ghostferry.error
  end
end
