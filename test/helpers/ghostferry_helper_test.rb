require 'minitest/autorun'
require 'ghostferry_helper'

# Unit tests for GhostferryHelper::Ghostferry that do not require a running
# database or a compiled Ghostferry binary.
class GhostferryHelperTest < Minitest::Test
  # send_signal must not raise when the child process has already exited.
  #
  # Regression: concurrent AFTER_ROW_COPY callbacks (DataIterationConcurrency
  # defaults to 4) can fire while the subprocess is shutting down. The redundant
  # Process.kill calls race against the child's exit and used to raise
  # Errno::ESRCH, failing interrupt/resume tests spuriously.
  def test_send_signal_ignores_esrch_for_already_exited_process
    # Spawn a child that exits immediately and wait for it so the OS has
    # reaped the PID. Any subsequent signal attempt against it raises ESRCH.
    pid = spawn('true')
    Process.waitpid(pid)

    gf = make_ghostferry_with_pid(pid)
    assert_silent { gf.send_signal('TERM') }
  end

  def test_send_signal_is_noop_when_pid_is_zero
    gf = make_ghostferry_with_pid(0)
    assert_silent { gf.send_signal('TERM') }
  end

  private

  def make_ghostferry_with_pid(pid)
    logger = Logger.new(IO::NULL)
    gf = GhostferryHelper::Ghostferry.new(
      'test/lib/go/minimal_ghostferry/main.go',
      logger: logger
    )
    gf.instance_variable_set(:@pid, pid)
    gf
  end
end
