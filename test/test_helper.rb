require "stringio"
require "logger"

require "pry-byebug" unless ENV["CI"]

GO_CODE_PATH = File.join(File.absolute_path(File.dirname(__FILE__)), "lib", "go")
FIXTURE_PATH = File.join(File.absolute_path(File.dirname(__FILE__)), "fixtures")

require "db_helper"
require "ghostferry_helper"
require "data_writer_helper"

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

  MINIMAL_GHOSTFERRY = "minimal_ghostferry"

  def new_ghostferry(filepath, config: {})
    # Transform path to something ruby understands
    path = File.join(GO_CODE_PATH, filepath, "main.go")
    g = Ghostferry.new(path, config: config, logger: @log_capturer.logger)
    @ghostferry_instances << g
    g
  end

  def new_ghostferry_with_interrupt_after_row_copy(filepath, config: {}, after_batches_written: 0)
    g = new_ghostferry(filepath, config)

    batches_written = 0
    g.on_status(Ghostferry::Status::AFTER_ROW_COPY) do
      batches_written += 1
      if batches_written >= after_batches_written
        g.send_signal("TERM")
      end
    end

    g
  end

  def new_source_datawriter(*args)
    dw = DataWriter.new(source_db_config, *args, logger: @log_capturer.logger)
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
  end

  def after_teardown
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

    assert source[DEFAULT_FULL_TABLE_NAME][:row_count] > 0
    assert target[DEFAULT_FULL_TABLE_NAME][:row_count] > 0

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
