class Juggler
  class Runner
    def initialize(juggler, method, concurrency, strategy)
      @juggler = juggler
      @strategy = strategy
      @concurrency = concurrency
      @queue = method.to_s

      @running = []
      @reserved = false
    end

    # We potentially need to issue a new reserve call after a job is reserved
    # (if we're not at the concurrency limit), and after a job completes
    # (unless we're already reserving)
    def reserve_if_necessary
      if @on && @connection.connected? && !@reserved && @running.size < @concurrency
        @juggler.logger.debug "#{to_s}: Reserving"
        reserve
      end

      if !@on && !@reserved && @running.empty?
        @stop_df.succeed
      end
    end

    def reserve
      @reserved = true

      reserve_call = connection.reserve(1)

      reserve_call.callback do |job|
        @reserved = false

        begin
          params = @juggler.serializer.load(job.body)
        rescue => e
          handle_exception(e, "#{to_s}: Exception unserializing #{@queue} job")
          connection.delete(job)
          reserve_if_necessary
          next
        end

        if params == "__STOP__"
          connection.delete(job)
          reserve_if_necessary
          next
        end

        if !@on
          job.release
          reserve_if_necessary
          next
        end

        job_runner = JobRunner.new(@juggler, job, params, @strategy)

        @running << job_runner

        @juggler.logger.debug {
          "#{to_s}: Executing #{@running.size} jobs"
        }

        # We may reserve after job is running (after fetching stats)
        job_runner.bind(:running) {
          reserve_if_necessary
        }

        # Also may reserve when a job is done
        job_runner.bind(:done) {
          @running.delete(job_runner)
          reserve_if_necessary
        }

        job_runner.run
      end

      reserve_call.errback do |error|
        @reserved = false

        if error == :deadline_soon
          # This doesn't necessarily mean that a job has taken too long, it is
          # quite likely that the blocking reserve is just stopping jobs from
          # being deleted
          @juggler.logger.debug "#{to_s}: Reserve terminated (deadline_soon)"

          check_all_reserved_jobs.callback {
            reserve_if_necessary
          }
        elsif error == :disconnected
          @juggler.logger.warn "#{to_s}: Reserve terminated (beanstalkd disconnected)"
        elsif error == :timed_out
          reserve_if_necessary
        else
          @juggler.logger.error "#{to_s}: Unexpected error: #{error}"
          reserve_if_necessary
        end
      end
    end

    def run
      @on = true
      @juggler.send(:add_runner, self)

      @connection = EMJack::Connection.new({
        :host => @juggler.server.host,
        :port => @juggler.server.port,
      })
      @connection.on_connect {
        @connection.watch(@queue)
        reserve_if_necessary
      }
      @connection.on_disconnect {
        @juggler.send(:disconnected)
      }
    end

    # Stopping a runner causes it to stop reserving any new jobs and closes the 
    # connection.
    def stop
      @on = false

      df = EM::DefaultDeferrable.new
      @stop_df = EM::DefaultDeferrable.new

      @stop_df.callback {
        connection.disconnect.callback {
          df.succeed
        }
      }
      df
    end

    # The number of jobs currently running.
    # This will be between 0 and @concurrency.
    def running_jobs
      @running.size
    end

    def to_s
      "Tube #{@queue}"
    end

    private

    def handle_exception(e, message)
      @juggler.logger.error "#{message}: #{e.message} (#{e.class})"
      @juggler.logger.debug e.backtrace.join("\n")
    end

    def connection
      @connection
    end

    # Iterates over all jobs reserved on this connection and fails them if
    # they're within 1s of their timeout. Returns a callback which completes
    # when all jobs have been checked
    def check_all_reserved_jobs
      dd = EM::DefaultDeferrable.new

      @running.each do |job_runner|
        job_runner.check_for_timeout
      end

      # Wait 1s before reserving or we'll just get DEALINE_SOON again
      # "If the client issues a reserve command during the safety margin,
      # <snip>, the server will respond with: DEADLINE_SOON"
      #
      # In theory, one should not need to do this since reserve will already
      # be triggered as a callback on the job that has timed out
      EM::Timer.new(1) do
        dd.succeed
      end

      dd
    end
  end
end
