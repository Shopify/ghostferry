require "fileutils"
require "json"
require "logger"
require "open3"
require "thread"
require "tmpdir"
require "webrick"
require "cgi"

module GhostferryHelper
  GHOSTFERRY_TEMPDIR = File.join(Dir.tmpdir, "ghostferry-integration")

  def self.remove_all_binaries
    FileUtils.remove_entry(GHOSTFERRY_TEMPDIR) if Dir.exist?(GHOSTFERRY_TEMPDIR)
  end

  class GhostferryExitFailure < StandardError
  end

  class Ghostferry
    # Manages compiling, running, and communicating with Ghostferry.
    #
    #
    # To use this class:
    #
    # ghostferry = Ghostferry.new("path/to/main.go")
    # ghostferry.on_status(Ghostferry::Status::BEFORE_ROW_COPY) do
    #   # do custom work here, such as injecting data into the database
    # end
    # ghostferry.run

    # Keep these in sync with integrationferry.go
    ENV_KEY_PORT = "GHOSTFERRY_INTEGRATION_PORT"

    module Status
      # This should be in sync with integrationferry.go
      READY = "READY"
      BINLOG_STREAMING_STARTED = "BINLOG_STREAMING_STARTED"
      ROW_COPY_COMPLETED = "ROW_COPY_COMPLETED"
      VERIFY_DURING_CUTOVER = "VERIFY_DURING_CUTOVER"
      VERIFIED = "VERIFIED"
      DONE = "DONE"

      BEFORE_ROW_COPY = "BEFORE_ROW_COPY"
      AFTER_ROW_COPY = "AFTER_ROW_COPY"
      BEFORE_BINLOG_APPLY = "BEFORE_BINLOG_APPLY"
      AFTER_BINLOG_APPLY = "AFTER_BINLOG_APPLY"
    end

    attr_reader :stdout, :stderr, :logrus_lines, :exit_status, :pid, :error, :error_lines

    def initialize(main_path, config: {}, logger: nil, message_timeout: 30, port: 39393)
      @logger = logger
      if @logger.nil?
        @logger = Logger.new(STDOUT)
        @logger.level = Logger::DEBUG
      end

      @main_path = main_path
      @config = config

      @message_timeout = message_timeout

      @status_handlers = {}
      @callback_handlers = {}

      @server_thread = nil
      @server_watchdog_thread = nil
      @subprocess_thread = nil

      @server = nil
      @server_last_error = nil
      @server_port = port

      @pid = 0
      @exit_status = nil
      @stdout = []
      @stderr = []
      @logrus_lines = {} # tag => [line1, line2]
      @error_lines = [] # lines with level == error
      @error = nil

      # Setup the directory to the compiled binary under the system temporary
      # directory.
      FileUtils.mkdir_p(GHOSTFERRY_TEMPDIR, mode: 0700)

      # To guarentee that the compiled binary will have an unique name, we use
      # the full path of the file relative to the ghostferry root directory as
      # the binary name. In order to avoid having / in the binary name all / in
      # the full path is replaced with _. The .go extension is also stripped to
      # avoid confusion.
      full_path = File.absolute_path(@main_path)
      full_path = full_path.split("/ghostferry/")[-1] # Assuming that ghostferry will show up in the path as its own directory
      binary_name = File.join(File.dirname(full_path), File.basename(full_path, ".*")).gsub("/", "_")
      @compiled_binary_path = File.join(GHOSTFERRY_TEMPDIR, binary_name)
    end

    # The main method to call to run a Ghostferry subprocess.
    def run(resuming_state = nil)
      resuming_state = JSON.generate(resuming_state) if resuming_state.is_a?(Hash)

      compile_binary
      start_server
      start_ghostferry(resuming_state)
      start_server_watchdog

      @subprocess_thread.join
      @server_watchdog_thread.join
      @server_thread.join
    ensure
      kill
      raise @server_last_error unless @server_last_error.nil?
    end

    # When using this method, you need to ensure that the datawriter has been
    # stopped properly (if you're using stop_datawriter_during_cutover).
    def run_expecting_interrupt(resuming_state = nil)
      run(resuming_state)
    rescue GhostferryExitFailure
      dumped_state = @stdout.join("")
      JSON.parse(dumped_state)
    else
      raise "Ghostferry did not get interrupted"
    end

    # Same as above - ensure that the datawriter has been
    # stopped properly (if you're using stop_datawriter_during_cutover).
    def run_expecting_failure(resuming_state = nil)
      run(resuming_state)
    rescue GhostferryExitFailure
    else
      raise "Ghostferry did not fail"
    end

    def run_with_logs(resuming_state = nil)
      with_env('CI', nil) { run(resuming_state) }
    end

    ######################################################
    # Methods representing the different stages of `run` #
    ######################################################

    def compile_binary
      return if File.exist?(@compiled_binary_path)

      @logger.debug("compiling test binary to #{@compiled_binary_path}")
      rc = system(
        "go", "build",
        "-o", @compiled_binary_path,
        @main_path
      )

      raise "could not compile ghostferry" unless rc
    end

    def start_server
      @server_last_error = nil

      @last_message_time = Time.now
      @server = WEBrick::HTTPServer.new(
        BindAddress: "127.0.0.1",
        Port: @server_port,
        Logger: @logger,
        AccessLog: [],
      )

      @server.mount_proc "/" do |req, resp|
        begin
          unless req.body
            @server_last_error = ArgumentError.new("Ghostferry is improperly implemented and did not send form data")
            resp.status = 400
            @server.shutdown
          end

          query = CGI::parse(req.body)

          status = query["status"]
          data = query["data"]

          unless status
            @server_last_error = ArgumentError.new("Ghostferry is improperly implemented and did not send a status")
            resp.status = 400
            @server.shutdown
          end

          status = status.first

          @last_message_time = Time.now
          @status_handlers[status].each { |f| f.call(*data) } unless @status_handlers[status].nil?
        rescue StandardError => e
          # errors are not reported from WEBrick but the server should fail early
          # as this indicates there is likely a programming error.
          @server_last_error = e
          @server.shutdown
        end
      end

      @server.mount_proc "/callbacks/progress" do |req, resp|
        begin
          unless req.body
            @server_last_error = ArgumentError.new("Ghostferry is improperly implemented and did not send data")
            resp.status = 400
            @server.shutdown
          end

          data = JSON.parse(JSON.parse(req.body)["Payload"])
          @callback_handlers["progress"].each { |f| f.call(data) } unless @callback_handlers["progress"].nil?
        rescue StandardError
        end
      end

      @server.mount_proc "/callbacks/state" do |req, resp|
        begin
          unless req.body
            @server_last_error = ArgumentError.new("Ghostferry is improperly implemented and did not send data")
            resp.status = 400
            @server.shutdown
          end
          data = JSON.parse(JSON.parse(req.body)["Payload"])
          @callback_handlers["state"].each { |f| f.call(data) } unless @callback_handlers["state"].nil?
        rescue StandardError
        end
      end

      @server.mount_proc "/callbacks/error" do |req, resp|
        @error = JSON.parse(JSON.parse(req.body)["Payload"])
        @callback_handlers["error"].each { |f| f.call(@error) } unless @callback_handlers["error"].nil?
      end

      @server_thread = Thread.new do
        @logger.debug("starting server thread")
        @server.start
        @logger.debug("server thread stopped")
      end
    end

    def start_ghostferry(resuming_state = nil)
      @subprocess_thread = Thread.new do
        # No need to spam the logs with Ghostferry interrupted exceptions if
        # Ghostferry is supposed to be interrupted.
        Thread.current.report_on_exception = false

        environment = {
          ENV_KEY_PORT => @server_port.to_s,
        }

        # TODO: maybe in the future we'll have a better way to specify the
        # configurations.
        if @config[:verifier_type]
          environment["GHOSTFERRY_VERIFIER_TYPE"] = @config[:verifier_type]
        end

        if @config[:compressed_data]
          environment["GHOSTFERRY_DATA_COLUMN_SNAPPY"] = "1"
        end

        if @config[:ignored_column]
          environment["GHOSTFERRY_IGNORED_COLUMN"] = @config[:ignored_column]
        end

        if @config[:cascading_pagination_column_config]
          environment["GHOSTFERRY_CASCADING_PAGINATION_COLUMN_CONFIG"] = @config[:cascading_pagination_column_config]
        end

        if @config[:skip_target_verification]
          environment["GHOSTFERRY_SKIP_TARGET_VERIFICATION"] = @config[:skip_target_verification]
        end

        if @config[:marginalia]
          environment["GHOSTFERRY_MARGINALIA"] = @config[:marginalia]
        end

        @logger.debug("starting ghostferry test binary #{@compiled_binary_path}")
        Open3.popen3(environment, @compiled_binary_path) do |stdin, stdout, stderr, wait_thr|
          stdin.puts(resuming_state) unless resuming_state.nil?
          stdin.close

          @pid = wait_thr.pid

          reads = [stdout, stderr]
          until reads.empty? do
            ready_reads, _, _ = IO.select(reads)
            ready_reads.each do |reader|
              line = reader.gets
              if line.nil?
                # EOF effectively
                reads.delete(reader)
                next
              end

              line.tr!("\n", '') # remove trailing newline

              if reader == stdout
                @stdout << line
                @logger.debug("stdout: #{line}")
              elsif reader == stderr
                @stderr << line
                if json_log_line?(line)
                  logline = JSON.parse(line)
                  tag = logline["tag"]
                  tag = "_none" if tag.nil?
                  @logrus_lines[tag] ||= []
                  @logrus_lines[tag] << logline

                  if logline["level"] == "error"
                    @error_lines << logline
                  end
                end
                @logger.debug("stderr: #{line}")
              end
            end
          end

          @exit_status = wait_thr.value
          @pid = 0
        end

        @logger.debug("ghostferry test binary exitted: #{@exit_status}")
        if @exit_status.exitstatus != 0
          raise GhostferryExitFailure, "ghostferry test binary returned non-zero status: #{@exit_status}"
        end
      end
    end

    def start_server_watchdog
      # If the subprocess hangs or exits abnormally due to a bad implementation
      # (panic/other unexpected errors) we need to make sure to terminate the
      # HTTP server to free up the port.
      @server_watchdog_thread = Thread.new do
        while @subprocess_thread.alive? do
          if Time.now - @last_message_time > @message_timeout
            @server.shutdown
            raise "ghostferry did not report to the integration test server for the last #{@message_timeout}s"
          end

          sleep 1
        end

        @server.shutdown
        @logger.debug("server watchdog thread stopped")
      end

      @server_watchdog_thread.abort_on_exception = true
    end

    ###################
    # Utility methods #
    ###################

    # TODO: eventually we should merge status and callback into a callback
    # system within Ghostferry. Right now the "status" is sent by
    # integrationferry, where as callback is sent by the Ghostferry library.
    def on_status(status, &block)
      raise "must specify a block" unless block_given?
      @status_handlers[status] ||= []
      @status_handlers[status] << block
    end

    def on_callback(callback, &block)
      raise "must specify a block" unless block_given?
      @callback_handlers[callback] ||= []
      @callback_handlers[callback] << block
    end

    def send_signal(signal)
      Process.kill(signal, @pid) if @pid != 0
    end

    def term_and_wait_for_exit
      send_signal("TERM")
      @subprocess_thread.join
    end

    def kill_and_wait_for_exit
      send_signal("KILL")
      @subprocess_thread.join
    end

    def kill
      @server.shutdown unless @server.nil?
      send_signal("KILL")

      # Need to ensure the server shutdown before returning so the port gets
      # freed and can be reused.
      @server_thread.join if @server_thread
      @server_watchdog_thread.join if @server_watchdog_thread

      begin
        @subprocess_thread.join if @subprocess_thread
      rescue GhostferryExitFailure
        # ignore
      end
    end

    private

    def json_log_line?(line)
      line.start_with?("{")
    end

    def with_env(key, value)
      previous_value = ENV.delete(key)
      ENV[key] = value
      yield
    ensure
      ENV[key] = previous_value
    end
  end
end
