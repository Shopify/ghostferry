require "thread"

module BlockingGateHelper
  # BlockingGate is a bounded synchronization primitive for integration tests
  # that need to block Ghostferry's progress at a certain point (by blocking
  # inside a status handler) until some other event releases it.
  #
  # Previously tests used a bare `sleep` with no argument inside a status
  # handler to block the DataIterator/BinlogStreamer indefinitely, relying on
  # a later status handler (e.g. AFTER_BINLOG_APPLY) to TERM the process and
  # unblock everything. When the expected releasing event did not arrive (a
  # legitimate ordering race), the handler slept forever. The Go side then hit
  # its 30s HTTP client timeout and panicked, but the Ruby WEBrick handler
  # thread stayed asleep, which blocked server shutdown and hung the whole
  # suite until the CI job-level timeout.
  #
  # BlockingGate replaces the bare `sleep` with a wait that:
  #   - returns immediately once #release is called, and
  #   - raises a clear error after `timeout` seconds instead of blocking
  #     forever, so a broken ordering assumption fails fast with a useful
  #     message rather than wedging CI.
  class BlockingGate
    Error = Class.new(StandardError)
    TimeoutError = Class.new(Error)

    def initialize(label:, timeout: 20)
      @label = label
      @timeout = timeout
      @mutex = Mutex.new
      @cond = ConditionVariable.new
      @released = false
    end

    # Block the calling thread until #release is called or `timeout` seconds
    # elapse. Raises BlockingGate::TimeoutError on timeout.
    def wait
      deadline = monotonic_now + @timeout
      @mutex.synchronize do
        until @released
          remaining = deadline - monotonic_now
          if remaining <= 0
            raise TimeoutError, "BlockingGate(#{@label}) timed out after #{@timeout}s waiting to be released"
          end
          @cond.wait(@mutex, remaining)
        end
      end
    end

    # Unblock any thread currently in #wait, and make future #wait calls
    # return immediately. Idempotent.
    def release
      @mutex.synchronize do
        @released = true
        @cond.broadcast
      end
    end

    def released?
      @mutex.synchronize { @released }
    end

    private

    def monotonic_now
      ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
    end
  end

  # Convenience factory so tests can write `gate = blocking_gate(...)`.
  def blocking_gate(label:, timeout: 20)
    BlockingGate.new(label: label, timeout: timeout)
  end
end
