require "json"
require "logger"
require "mysql2"
require "open3"
require "tqdm"
require "webrick"

module GhostferryBenchmark
  class CallbackServer
    attr_reader :progress_logs

    def initialize
      @server = WEBrick::HTTPServer.new(
        BindAddress: "127.0.0.1",
        Port: 8001
      )

      @logger = Logger.new(STDERR)
      @logger.level = Logger::INFO

      @progress_logs = []
      @thr = nil

      @server.mount_proc "/callbacks/progress" do |req, resp|
        unless req.body
          logger.warn("no data?")
        end

        progress_data_json = JSON.parse(JSON.parse(req.body)["Payload"])
        @progress_logs << progress_data_json
      end
    end

    def start
      @thr = Thread.new do
        @server.start
      ensure
        @server.shutdown
      end
    end

    def stop
      return if @thr.nil?
      @server.stop
      @thr.join
    end
  end

  module Databases
    SCHEMA_NAME = "benchmark"
    TABLE_NAME = "t"

    class << self
      def source
        @source ||= Mysql2::Client.new(
          host: "127.0.0.1",
          port: 29291,
          username: "root",
          password: ""
        )
      end

      def target
        @target ||= Mysql2::Client.new(
          host: "127.0.0.1",
          port: 29292,
          username: "root",
          password: ""
        )
      end

      def seed(nrows: 100000, ncols: 4, row_size: 25 * 4 * 1024, insert_batch_size: 4000)
        puts "seeding with rows = #{nrows}, row_size = #{row_size}"
        cols = []
        args = ["NULL"]
        datas = []
        bytes_per_col = (row_size / ncols).to_i
        ncols.times do |i|
          cols << "c#{i} longblob"
          args << "?"

          letter = ("a".."z").to_a.sample
          datas << letter * bytes_per_col
        end

        datas = datas * insert_batch_size

        source.query("DROP DATABASE IF EXISTS #{SCHEMA_NAME}")
        source.query("CREATE DATABASE #{SCHEMA_NAME}")
        source.query("CREATE TABLE #{SCHEMA_NAME}.#{TABLE_NAME} (id bigint(20) unsigned auto_increment, #{cols.join(', ')}, primary key (id))")

        args = ["(#{args.join(',')})"] * insert_batch_size
        args = args.join(",")
        stmt = source.prepare("INSERT INTO #{SCHEMA_NAME}.#{TABLE_NAME} VALUES #{args}")

        (nrows / insert_batch_size).round.to_i.times.tqdm(leave: true).each do
          stmt.execute(*datas)
        end
      end

      def wipe_target
        target.query("DROP DATABASE IF EXISTS #{SCHEMA_NAME}")
      end
    end
  end

  class << self
    def default_ghostferry_config
      {
        "Source" => {
          "Host" => "127.0.0.1",
          "Port" => 29291,
          "User" => "root",
          "Pass" => "",
          "Collation" => "utf8mb4_unicode_ci",
          "Params" => {
            "charset": "utf8mb4",
          },
        },
        "Target" => {
          "Host" => "127.0.0.1",
          "Port" => 29292,
          "User" => "root",
          "Pass" => "",
          "Collation" => "utf8mb4_unicode_ci",
          "Params" => {
            "charset" => "utf8mb4"
          },
        },

        "Databases" => {
          "Whitelist" => [Databases::SCHEMA_NAME]
        },

        "Tables" => {},

        "ProgressCallback" => {
          "URI" => "http://localhost:8001/callbacks/progress",
          "Payload" => ""
        },

        "ProgressReportFrequency" => 1500,

        "DumpStateOnSignal" => false,
        "VerifierType" => "Inline",
        "SkipTargetVerification" => true,
        "DataIterationBatchSize" => 200
      }
    end

    def run_ghostferry(ghostferry_config:, output_dir:, duration: 15)
      ghostferry_config_path = "#{output_dir}/conf.json"
      log_path = "#{output_dir}/ghostferry.log"
      progress_log_path = "#{output_dir}/progress.json.log"
      csv_path = "#{output_dir}/rows_written.csv"

      logger = Logger.new(STDERR)

      File.open(ghostferry_config_path, "w") do |f|
        f.write(JSON.pretty_generate(ghostferry_config))
      end

      logger.info("running ghostferry for output_dir = #{output_dir}")
      callback_server = CallbackServer.new
      callback_server.start

      stdin, stdouterr, wt = Open3.popen2e("ghostferry-copydb #{ghostferry_config_path}")
      stdin.close

      duration.times.tqdm.each do
        sleep 1
      end

      logger.info("stopping ghostferrying and gathering results")
      begin
        Process.kill("TERM", wt.pid)
      rescue
        puts stdouterr.read
      end
      wt.join
      callback_server.stop

      File.open(log_path, "w") do |f|
        f.write(stdouterr.read)
      end

      data = []
      File.open(csv_path, "w") do |csv_file|
        File.open(progress_log_path, "w") do |progress_file|
          callback_server.progress_logs.each do |log|
            line = [log["TimeTaken"], log["Tables"]["#{Databases::SCHEMA_NAME}.#{Databases::TABLE_NAME}"]["RowsWritten"], log["CurrentState"]]
            data << line
            csv_file.puts(line.join(","))
            progress_file.puts(JSON.generate(log))
          end
        end
      end

      w0 = data[0][1]
      t0 = data[0][0]
      w1 = nil
      t1 = nil

      num_data_in_calculation = 0
      data.each do |line|
        # In case we reach a non-copying state, we don't want to calculate the
        # speed based on a timestamp when the copy is already done.
        if line[2] == "copying"
          w1 = line[1]
          t1 = line[0]
          num_data_in_calculation += 1
        end
      end

      avg_speed = (w1 - w0) / (t1 - t0)
      logger.info("average speed for #{output_dir} (calculated with #{num_data_in_calculation} pts) = #{avg_speed.round} rows/s")
      avg_speed
    end
  end
end
