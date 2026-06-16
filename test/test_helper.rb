require "stringio"
require "logger"
require "minitest"
require "minitest/autorun"
require "minitest/reporters"
require "minitest/retry"
require "minitest/fail_fast"
require "minitest/hooks/test"
require "pry-byebug" unless ENV["CI"]

GO_CODE_PATH = File.join(File.absolute_path(File.dirname(__FILE__)), "lib", "go")
FIXTURE_PATH = File.join(File.absolute_path(File.dirname(__FILE__)), "fixtures")

def add_to_load_path(path)
  $LOAD_PATH.unshift(path) unless $LOAD_PATH.include?(path)
end

test_path      = File.expand_path(File.dirname(__FILE__))
test_lib_path  = File.join(test_path, "lib")
lib_path       = File.expand_path(File.join(test_path, "..", "lib"))
helpers_path   = File.join(test_path, "helpers")

[test_path, test_lib_path, lib_path, helpers_path].each { add_to_load_path(_1) }

require "db_helper"
require "ghostferry_helper"
require "data_writer_helper"
require "blocking_gate_helper"

Minitest::Reporters.use! Minitest::Reporters::SpecReporter.new
Minitest::Retry.use!(exceptions_to_retry: [GhostferryHelper::Ghostferry::TimeoutError])

at_exit do
  GhostferryHelper.remove_all_binaries
end

class LogCapturer
  attr_reader :logger

  def initialize(level: Logger::DEBUG)
    @capture = ENV["DEBUG"] != "1"
    if @capture
      @logger_device = StringIO.new
      @logger = Logger.new(@logger_device, level: level)
    else
      @logger = Logger.new(STDOUT)
    end
  end

  def reset
    @logger_device.truncate(0) if @capture
  end

  def print_output
    if @capture
      puts "\n"
      puts "--- Start of failed test output ---"
      puts @logger_device.string
      puts "--- End of failed test output ---"
      puts "\n"
    end
  end
end

class GhostferryTestCase < Minitest::Test
  include Minitest::Hooks
  include GhostferryHelper
  include DbHelper
  include DataWriterHelper
  include BlockingGateHelper

  MINIMAL_GHOSTFERRY = "minimal_ghostferry"

  # Wall-clock budget for a single integration test, independent of the
  # Ghostferry idle-message watchdog. This catches hangs that happen *outside*
  # of an active Ghostferry#run, such as a wedged teardown or datawriter join.
  # On timeout the watchdog dumps Ruby thread backtraces, asks every tracked
  # Ghostferry subprocess for a goroutine dump (SIGQUIT), and exits the process
  # so CI fails fast with diagnostics instead of waiting for the job timeout.
  TEST_WALL_CLOCK_TIMEOUT = Integer(ENV.fetch("GHOSTFERRY_TEST_TIMEOUT", "240"))

  def new_ghostferry(filepath, config: {})
    # Transform path to something ruby understands
    path = File.join(GO_CODE_PATH, filepath, "main.go")
    g = Ghostferry.new(path, config: config, logger: @log_capturer.logger)
    @ghostferry_instances << g
    g
  end

  def new_ghostferry_with_interrupt_after_row_copy(filepath, config: {}, after_batches_written: 0)
    g = new_ghostferry(filepath, config: config)

    batches_written = 0
    g.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      batches_written += 1

      if batches_written >= after_batches_written
        g.send_signal("TERM")
      end
    end

    g
  end

  def new_source_datawriter(*args, **kwargs)
    dw = DataWriter.new(source_db_config, *args, **kwargs, logger: @log_capturer.logger)
    @datawriter_instances << dw
    dw
  end

  def load_fixture(filename)
    File.read(File.join(FIXTURE_PATH, filename))
  end

  def setup_signal_watcher
    Signal.trap("INT") { self.on_term }
    Signal.trap("TERM") { self.on_term }
  end

  ##############
  # Test Hooks #
  ##############

  def before_all
    super
    @log_capturer = LogCapturer.new
    initialize_db_connections
    setup_signal_watcher
  end

  def before_setup
    super
    reset_data

    # Any ghostferry instances created via the new_ghostferry method will be
    # pushed to here, which allows the test to kill the process after each test
    # should there be a hung process/failed test/errored test.
    @ghostferry_instances = []

    # Same thing with DataWriter as above
    @datawriter_instances = []

    start_test_watchdog
  end

  def after_teardown
    stop_test_watchdog

    @ghostferry_instances.each do |ghostferry|
      ghostferry.kill
    end

    @datawriter_instances.each do |datawriter|
      datawriter.stop_and_join
    end

    @log_capturer.print_output if self.failure
    @log_capturer.reset
    super
  end

  # Starts a background thread that fires after TEST_WALL_CLOCK_TIMEOUT and,
  # if the test has not finished, dumps diagnostics and terminates the process.
  # Covers hangs anywhere in the test lifecycle, including setup, teardown,
  # datawriter joins, and DB queries -- not just inside Ghostferry#run.
  def start_test_watchdog
    @test_watchdog_done = false
    @test_watchdog_mutex = Mutex.new
    @test_watchdog_cond = ConditionVariable.new
    test_name = "#{self.class.name}##{self.name}"

    @test_watchdog_thread = Thread.new do
      @test_watchdog_mutex.synchronize do
        unless @test_watchdog_done
          @test_watchdog_cond.wait(@test_watchdog_mutex, TEST_WALL_CLOCK_TIMEOUT)
        end
        next if @test_watchdog_done
      end

      $stderr.puts("\n=== GHOSTFERRY TEST WATCHDOG FIRED ===")
      $stderr.puts("Test #{test_name.inspect} exceeded #{TEST_WALL_CLOCK_TIMEOUT}s.")

      Array(@ghostferry_instances).each do |ghostferry|
        begin
          ghostferry.diagnose!("test_watchdog: #{test_name}")
        rescue StandardError => e
          $stderr.puts("failed to diagnose ghostferry instance: #{e.class}: #{e.message}")
        end
      end

      $stderr.puts("--- all ruby thread backtraces ---")
      Thread.list.each do |t|
        $stderr.puts("thread #{t.object_id} status=#{t.status.inspect}")
        (t.backtrace || []).each { |line| $stderr.puts("    #{line}") }
      end
      $stderr.puts("=== END GHOSTFERRY TEST WATCHDOG ===\n")
      $stderr.flush

      # Give the Go child a moment to flush its goroutine dump before we exit.
      sleep 2
      @log_capturer.print_output
      $stderr.flush
      exit!(1)
    end
  end

  def stop_test_watchdog
    return unless @test_watchdog_thread

    @test_watchdog_mutex.synchronize do
      @test_watchdog_done = true
      @test_watchdog_cond.broadcast
    end
    @test_watchdog_thread.join(5)
    @test_watchdog_thread = nil
  end

  def on_term
    @log_capturer.print_output
    exit
  end

  def after_all
    reset_data
    teardown_connections
    super
  end

  #####################
  # Assertion Helpers #
  #####################

  def assert_test_table_is_identical
    source, target = source_and_target_table_metrics

    assert_operator 0, :<, source[DEFAULT_FULL_TABLE_NAME][:row_count]
    assert_operator 0, :<, target[DEFAULT_FULL_TABLE_NAME][:row_count]

    assert_equal(
      source[DEFAULT_FULL_TABLE_NAME][:row_count],
      target[DEFAULT_FULL_TABLE_NAME][:row_count],
      "source and target row count don't match",
    )

    assert_equal(
      source[DEFAULT_FULL_TABLE_NAME][:checksum],
      target[DEFAULT_FULL_TABLE_NAME][:checksum],
      "source and target checksum don't match",
    )
  end

  def assert_uuid_table_is_identical
    source, target = source_and_target_table_metrics(tables: [UUID_FULL_TABLE_NAME])

    assert_operator 0, :<, source[UUID_FULL_TABLE_NAME][:row_count]
    assert_operator 0, :<, target[UUID_FULL_TABLE_NAME][:row_count]

    assert_equal(
      source[UUID_FULL_TABLE_NAME][:row_count],
      target[UUID_FULL_TABLE_NAME][:row_count],
      "source and target row count don't match",
    )

    assert_equal(
      source[UUID_FULL_TABLE_NAME][:checksum],
      target[UUID_FULL_TABLE_NAME][:checksum],
      "source and target checksum don't match",
    )
  end

  # Use this method to assert the validity of the structure of the dumped
  # state.
  #
  # To actually assert the validity of the data within the dumped state, you
  # have to do it manually.
  def assert_basic_fields_exist_in_dumped_state(dumped_state)
    refute dumped_state.nil?
    refute dumped_state["GhostferryVersion"].nil?
    refute dumped_state["LastKnownTableSchemaCache"].nil?
    refute dumped_state["LastSuccessfulPaginationKeys"].nil?
    refute dumped_state["CompletedTables"].nil?
    refute dumped_state["LastWrittenBinlogPosition"].nil?
  end

  def assert_ghostferry_completed(instance, times:)
    started_runs = instance.logrus_lines["ferry"].select{ |line| line["msg"].include?("hello world") }.count
    completed_runs = instance.logrus_lines["ferry"].select{ |line| line["msg"].include?("ghostferry run is complete") }.count

    assert started_runs == times
    assert completed_runs == times
  end

  def with_env(key, value)
    previous_value = ENV.delete(key)
    ENV[key] = value
    yield
  ensure
    ENV[key] = previous_value
  end
end
