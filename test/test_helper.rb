require "logger"
require "minitest"
require "minitest/autorun"
require "minitest/hooks/test"

GO_CODE_PATH = File.join(File.absolute_path(File.dirname(__FILE__)), "lib", "go")

require "db_helper"
require "ghostferry_helper"
require "data_writer_helper"

Minitest.after_run do
  GhostferryHelper.remove_all_binaries
end

class GhostferryTestCase < Minitest::Test
  include Minitest::Hooks
  include GhostferryHelper
  include DbHelper
  include DataWriterHelper

  MINIMAL_GHOSTFERRY = "integrationferry.go"

  def new_ghostferry(filename)
    # Transform path to something ruby understands
    path = File.join(GO_CODE_PATH, filename)
    g = Ghostferry.new(path, logger: @logger)
    @ghostferry_instances << g
    g
  end

  def new_source_datawriter(*args)
    dw = DataWriter.new(source_db_config, *args, logger: @logger)
    @datawriter_instances << dw
    dw
  end

  ##############
  # Test Hooks #
  ##############

  def before_all
    @logger = Logger.new(STDOUT)
    if ENV["DEBUG"] == "1"
      @logger.level = Logger::DEBUG
    else
      @logger.level = Logger::INFO
    end

    initialize_db_connections
  end

  def before_setup
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
    )

    assert_equal(
      source[DEFAULT_FULL_TABLE_NAME][:checksum],
      target[DEFAULT_FULL_TABLE_NAME][:checksum],
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
    refute dumped_state["LastSuccessfulPrimaryKeys"].nil?
    refute dumped_state["CompletedTables"].nil?
    refute dumped_state["LastWrittenBinlogPosition"].nil?
  end
end
