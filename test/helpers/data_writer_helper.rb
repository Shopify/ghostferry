require "logger"
require "thread"

require "db_helper"
require "ghostferry_helper"
require "mysql2"

module DataWriterHelper
  def start_datawriter_with_ghostferry(dw, gf, &on_write)
    gf.on_status(GhostferryHelper::Ghostferry::Status::READY) do
      dw.start(&on_write)
    end
  end

  def stop_datawriter_during_cutover(dw, gf)
    gf.on_status(GhostferryHelper::Ghostferry::Status::ROW_COPY_COMPLETED) do
      puts "HK-DEBUG row copy is complete.. shutting down data writer"
      # At the start of the cutover phase, we have to set the database to
      # read-only. This is done by stopping the datawriter.
      dw.stop_and_join
    end
  end

  class DataWriter
    # A threaded data writer that just hammers the database with write
    # queries as much as possible.
    #
    # This is used essentially for random testing.
    def initialize(db_config,
                   tables: [DbHelper::DEFAULT_FULL_TABLE_NAME],
                   insert_probability: 0.33,
                   update_probability: 0.33,
                   delete_probability: 0.34,
                   number_of_writers: 1,
                   logger: nil
                  )
      @db_config = db_config
      @tables = tables

      @number_of_writers = number_of_writers
      @insert_probability = [0, insert_probability]
      @update_probability = [@insert_probability[1], @insert_probability[1] + update_probability]
      @delete_probability = [@update_probability[1], @update_probability[1] + delete_probability]

      @threads = []
      @started = false
      @stop_requested = false

      @logger = logger
      if @logger.nil?
        @logger = Logger.new(STDOUT)
        @logger.level = Logger::DEBUG
      end
    end

    def start(&on_write)
      raise "Cannot start DataWriter multiple times. Use a new instance instead " if @started
      @started = true
      @number_of_writers.times do |i|
        @threads << Thread.new do
          @logger.info("starting data writer thread #{i}")

          begin
            connection = Mysql2::Client.new(@db_config)

            until @stop_requested do
              write_data(connection, &on_write)
            end
          ensure
            connection.close
          end

          @logger.info("stopped data writer thread #{i}")
        end
      end
    end

    def stop_and_join
      puts "HK-DEBUG data writer shutdown request"
      @stop_requested = true
      join
    end

    def join
      @threads.each do |t|
        t.join
      end
    end

    def write_data(connection, &on_write)
      r = rand

      if r >= @insert_probability[0] && r < @insert_probability[1]
        id = insert_data(connection)
        op = "INSERT"
      elsif r >= @update_probability[0] && r < @update_probability[1]
        id = update_data(connection)
        op = "UPDATE"
      elsif r >= @delete_probability[0] && r < @delete_probability[1]
        id = delete_data(connection)
        op = "DELETE"
      end

      # @logger.debug("writing data: #{op} #{id}")
      on_write.call(op, id) unless on_write.nil?
    end

    def insert_data(connection)
      table = @tables.sample
      insert_statement = connection.prepare("INSERT INTO #{table} (id, data) VALUES (?, ?)")
      insert_statement.execute(nil, DbHelper.rand_data)
      insert_statement.close
      connection.last_id
    end

    def update_data(connection)
      table = @tables.sample
      id = random_real_id(connection, table)
      update_statement = connection.prepare("UPDATE #{table} SET data = ? WHERE id >= ? LIMIT 1")
      update_statement.execute(DbHelper.rand_data, id)
      update_statement.close
      id
    end

    def delete_data(connection)
      table = @tables.sample
      id = random_real_id(connection, table)
      delete_statement = connection.prepare("DELETE FROM #{table} WHERE id >= ? LIMIT 1")
      delete_statement.execute(id)
      delete_statement.close
      id
    end

    def random_real_id(connection, table)
      # This query is slow for large datasets.
      # For testing purposes, this should be okay.
      result = connection.query("SELECT id FROM #{table} ORDER BY RAND() LIMIT 1")
      raise "No rows in the database?" if result.first.nil?
      result.first["id"]
    end
  end
end
