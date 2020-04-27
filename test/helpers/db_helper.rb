require "logger"
require "mysql2"

module DbHelper
  ALPHANUMERICS = ("0".."9").to_a + ("a".."z").to_a + ("A".."Z").to_a
  DB_PORTS = {source: 29291, target: 29292}

  DEFAULT_ANNOTATION = "application:ghostferry"

  DEFAULT_DB = "gftest"
  DEFAULT_TABLE = "test_table_1"

  class Mysql2::Client
    alias_method :query_without_maginalia, :query
    alias_method :prepare_without_maginalia, :prepare

    def query(sql, exclude_marginalia: false, annotations: [DEFAULT_ANNOTATION])
      sql_annotations = []
      annotations.each do |annotation|
        sql_annotations << "/*#{annotation}*/"
      end
      sql = "#{sql_annotations.join(" ")} #{sql}" unless exclude_marginalia
      query_without_maginalia(sql)
    end

    def prepare(sql, exclude_marginalia: false, annotations: [DEFAULT_ANNOTATION])
      sql_annotations = []
      annotations.each do |annotation|
        sql_annotations << "/*#{annotation}*/"
      end
      sql = "#{sql_annotations.join(" ")} #{sql}" unless exclude_marginalia
      prepare_without_maginalia(sql)
    end
  end

  def self.full_table_name(db, table)
    "`#{db}`.`#{table}`"
  end

  def self.rand_data(length: 32)
    ALPHANUMERICS.sample(length).join("") + "👻⛴️"
  end

  DEFAULT_FULL_TABLE_NAME = full_table_name(DEFAULT_DB, DEFAULT_TABLE)

  def full_table_name(db, table)
    DbHelper.full_table_name(db, table)
  end

  def rand_data(length: 32)
    DbHelper.rand_data(length: length)
  end

  def default_db_config(port:)
    {
      host:      "127.0.0.1",
      port:      port,
      username:  "root",
      password:  "",
      encoding:  "utf8mb4",
      collation: "utf8mb4_unicode_ci",
    }
  end

  def transaction(connection)
    raise ArgumentError, "must pass a block" if !block_given?

    begin
      connection.query("BEGIN")
      yield
    rescue
      connection.query("ROLLBACK")
      raise
    else
      connection.query("COMMIT")
    end
  end

  def initialize_db_connections
    @connections = {}
    DB_PORTS.each do |name, port|
      @connections[name] = Mysql2::Client.new(default_db_config(port: port))
    end
  end

  def teardown_connections
    @connections.each_value do |conn|
      conn&.close
    end
  end

  def source_db
    @connections[:source]
  end

  def target_db
    @connections[:target]
  end

  def source_db_config
    default_db_config(port: DB_PORTS[:source])
  end

  def target_db_config
    default_db_config(port: DB_PORTS[:target])
  end

  # Database Seeding Methods
  ##########################
  # Each test case can choose what kind of database it wants to setup by
  # calling one of these methods.

  def reset_data
    @connections.each do |_, connection|
      connection.query("DROP DATABASE IF EXISTS `#{DEFAULT_DB}`")
    end
  end

  def seed_random_data(connection, database_name: DEFAULT_DB, table_name: DEFAULT_TABLE, number_of_rows: 1111)
    dbtable = full_table_name(database_name, table_name)

    connection.query("CREATE DATABASE IF NOT EXISTS #{database_name}")
    connection.query("CREATE TABLE IF NOT EXISTS #{dbtable} (id bigint(20) not null auto_increment, data TEXT, primary key(id))")

    return if number_of_rows == 0

    transaction(connection) do
      sqlargs = (["(?,?)"]*number_of_rows).join(", ")
      sql = "INSERT INTO #{dbtable} (id, data) VALUES #{sqlargs}"
      insert_statement = connection.prepare(sql)

      rand_rows = []
      number_of_rows.times do
        rand_rows += [nil, rand_data]
      end

      insert_statement.execute(*rand_rows)
    end
  end

  def seed_simple_database_with_single_table
    # Setup the source database with data.
    max_id = 1111
    seed_random_data(source_db, number_of_rows: max_id)

    # Create some holes in the data.
    num_holes = 140

    sqlargs = (["?"]*num_holes).join(",")
    delete_statement = source_db.prepare("DELETE FROM #{full_table_name(DEFAULT_DB, DEFAULT_TABLE)} WHERE id IN (#{sqlargs})")

    holes_ids = []
    num_holes.times do
      holes_ids << Random.rand(max_id)
    end

    delete_statement.execute(*holes_ids)

    # Setup the target database with no data but the correct schema.
    seed_random_data(target_db, number_of_rows: 0)
  end

  # Get some overall metrics like CHECKSUM, row count, sample row from tables.
  # Generally used for test validation.
  def source_and_target_table_metrics(tables: [DEFAULT_FULL_TABLE_NAME])
    source_metrics = {}
    target_metrics = {}

    tables.each do |table|
      source_metrics[table] = table_metric(source_db, table)
      target_metrics[table] = table_metric(target_db, table, sample_id: source_metrics[table][:sample_row]["id"])
    end

    [source_metrics, target_metrics]
  end

  def table_metric(conn, table, sample_id: nil)
    metrics = {}
    result = conn.query("CHECKSUM TABLE #{table}")
    metrics[:checksum] = result.first["Checksum"]

    result = conn.query("SELECT COUNT(*) AS cnt FROM #{table}")
    metrics[:row_count] = result.first["cnt"]

    if sample_id.nil?
      result = conn.query("SELECT * FROM #{table} ORDER BY RAND() LIMIT 1")
      metrics[:sample_row] = result.first
    else
      result = conn.query("SELECT * FROM #{table} WHERE id = #{sample_id} LIMIT 1")
      metrics[:sample_row] = result.first
    end

    metrics
  end
end
