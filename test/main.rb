# frozen_string_literal: true
test_path        = File.expand_path(File.dirname(__FILE__))
test_lib_path    = File.join(test_path, "lib")
lib_path         = File.expand_path(File.join(test_path, "..", "lib"))
helpers_path     = File.join(test_path, "helpers")
integration_path = File.join(test_path, "integration")
test_files       = Dir.glob("#{integration_path}/**/*_test.rb")

$LOAD_PATH.unshift(test_path) unless $LOAD_PATH.include?(test_path)
$LOAD_PATH.unshift(test_lib_path) unless $LOAD_PATH.include?(test_lib_path)
$LOAD_PATH.unshift(lib_path) unless $LOAD_PATH.include?(lib_path)
$LOAD_PATH.unshift(helpers_path) unless $LOAD_PATH.include?(helpers_path)

require "ghostferry_helper"

require "minitest"
require "minitest/reporters"
require "minitest/retry"
require "minitest/fail_fast"
require "minitest/hooks/test"

Minitest::Reporters.use! Minitest::Reporters::SpecReporter.new
Minitest::Retry.use!(exceptions_to_retry: [GhostferryHelper::Ghostferry::TimeoutError])

test_files.each do |f|
  require f
end

exit_code = 1

at_exit do
  GhostferryHelper.remove_all_binaries
  exit(exit_code)
end

exit_code = Minitest.run(ARGV)
