require "fileutils"
require "json"
require "logger"
require "open3"
require "socket"
require "thread"
require "tmpdir"

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
    ENV_KEY_SOCKET_PATH = "GHOSTFERRY_INTEGRATION_SOCKET_PATH"
    MAX_MESSAGE_SIZE = 256

    SOCKET_PATH = ENV[ENV_KEY_SOCKET_PATH] || "/tmp/ghostferry-integration.sock"

    CONTINUE = "CONTINUE"

    module Status
      # This should be in sync with integrationferry.go
      READY = "READY"
      BINLOG_STREAMING_STARTED = "BINLOG_STREAMING_STARTED"
      ROW_COPY_COMPLETED = "ROW_COPY_COMPLETED"
      DONE = "DONE"

      BEFORE_ROW_COPY = "BEFORE_ROW_COPY"
      AFTER_ROW_COPY = "AFTER_ROW_COPY"
      BEFORE_BINLOG_APPLY = "BEFORE_BINLOG_APPLY"
      AFTER_BINLOG_APPLY = "AFTER_BINLOG_APPLY"
    end

    attr_reader :stdout, :stderr, :exit_status, :pid

    def initialize(main_path, logger: nil, message_timeout: 30)
      @main_path = main_path
      @message_timeout = message_timeout
      @logger = logger
      if @logger.nil?
        @logger = Logger.new(STDOUT)
        @logger.level = Logger::DEBUG
      end

      FileUtils.mkdir_p(GHOSTFERRY_TEMPDIR, mode: 0700)

      # full name relative to the ghostferry root dir, with / replaced with _
      # and the extension stripped.
      full_path = File.absolute_path(@main_path)
      full_path = full_path.split("/ghostferry/")[-1] # Assuming that ghostferry will show up in the path as its own directory
      binary_name = File.join(File.dirname(full_path), File.basename(full_path, ".*")).gsub("/", "_")
      @compiled_binary_path = File.join(GHOSTFERRY_TEMPDIR, binary_name)

      @status_handlers = {}
      @stop_requested = false

      @server_thread = nil
      @subprocess_thread = nil

      @server = nil
      @server_started_notifier = Queue.new

      @pid = 0
      @exit_status = nil
      @stdout = []
      @stderr = []
    end

    def on_status(status, &block)
      raise "must specify a block" unless block_given?
      @status_handlers[status] ||= []
      @status_handlers[status] << block
    end

    def compile_binary
      return if File.exist?(@compiled_binary_path)

      @logger.info("compiling test binary to #{@compiled_binary_path}")
      rc = system(
        "go", "build",
        "-o", @compiled_binary_path,
        @main_path
      )

      raise "could not compile ghostferry" unless rc
    end

    def start_server
      @server_thread = Thread.new do
        @logger.info("starting integration test server")
        @server = UNIXServer.new(SOCKET_PATH)
        @server_started_notifier.push(true)

        reads = [@server]
        last_message_time = Time.now

        while (!@stop_requested && @exit_status.nil?) do
          ready = IO.select(reads, nil, nil, 0.2)

          if ready.nil?
            next if Time.now - last_message_time < @message_timeout

            raise "ghostferry did not report to the integration test server for the last #{@message_timeout}"
          end

          last_message_time = Time.now

          # Each client should send one message, expects a message back, and
          # then close the connection.
          #
          # This is done because there are many goroutines operating in
          # parallel and sending messages over a shared connection would result
          # in multiplexing issues. Using separate connections gets around this
          # problem.
          ready[0].each do |socket|
            if socket == @server
              # A new message is to be sent by a goroutine
              client = @server.accept_nonblock
              reads << client
            elsif socket.eof?
              # A message was complete
              @logger.warn("client disconnected?")
              socket.close
              reads.delete(socket)
            else
              # Receiving a message
              data = socket.read_nonblock(MAX_MESSAGE_SIZE)
              data = data.split("\0")
              @logger.debug("server received status: #{data}")

              status = data.shift

              @status_handlers[status].each { |f| f.call(*data) } unless @status_handlers[status].nil?
              begin
                socket.write(CONTINUE)
              rescue Errno::EPIPE
                # It is possible for the status handler to kill Ghostferry.
                # In such scenarios, this write may result in a broken pipe as
                # the socket is closed.
                #
                # We rescue this and move on.
                @logger.debug("can't send CONTINUE due to broken pipe")
              end

              reads.delete(socket)
            end
          end
        end

        @server.close
        @logger.info("server thread stopped")
     end
    end

    def start_ghostferry(resuming_state = nil)
      @subprocess_thread = Thread.new do
        Thread.current.report_on_exception = false

        environment = {
          ENV_KEY_SOCKET_PATH => SOCKET_PATH
        }

        @logger.info("starting ghostferry test binary #{@compiled_binary_path}")
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
                @logger.debug("stderr: #{line}")
              end
            end
          end

          @exit_status = wait_thr.value
          @pid = 0
        end

        @logger.info("ghostferry test binary exitted: #{@exit_status}")
        if @exit_status.exitstatus != 0
          raise GhostferryExitFailure, "ghostferry test binary returned non-zero status: #{@exit_status}"
        end
      end
    end

    def wait_until_server_has_started
      @server_started_notifier.pop
      @logger.info("integration test server started and listening for connection")
    end

    def wait_until_ghostferry_run_is_complete
      # Server thread should always join first because the loop within it
      # should exit if @exit_status != nil.
      @server_thread.join if @server_thread
      @subprocess_thread.join if @subprocess_thread
    end

    def send_signal(signal)
      Process.kill(signal, @pid) if @pid != 0
    end

    def kill
      @stop_requested = true
      send_signal("KILL")
      begin
        wait_until_ghostferry_run_is_complete
      rescue GhostferryExitFailure
        # ignore
      end

      File.unlink(SOCKET_PATH) if File.exist?(SOCKET_PATH)
    end

    def run(resuming_state = nil)
      resuming_state = JSON.generate(resuming_state) if resuming_state.is_a?(Hash)

      compile_binary
      start_server
      wait_until_server_has_started
      start_ghostferry(resuming_state)
      wait_until_ghostferry_run_is_complete
    ensure
      kill
    end

    # When using this method, you need to call it within the block of
    # GhostferryIntegration::TestCase#with_state_cleanup to ensure that the
    # integration server is shutdown properly.
    def run_expecting_interrupt(resuming_state = nil)
      run(resuming_state)
    rescue GhostferryExitFailure
      dumped_state = @stdout.join("")
      JSON.parse(dumped_state)
    else
      raise "Ghostferry did not get interrupted"
    end
  end
end
