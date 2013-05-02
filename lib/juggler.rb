require 'em-jack'
require 'eventmachine'
require 'uri'

class Juggler
  class JugglerInstance
    attr_writer :logger
    attr_writer :shutdown_grace_timeout
    attr_accessor :exception_handler
    attr_accessor :backoff_function
    attr_accessor :serializer
    attr_reader :server

    def initialize(options = {})
      @server = URI(options[:server] || "beanstalk://localhost:11300")
      @logger = options[:logger]
      @serializer = options[:serializer] || Marshal

      # Default exception handler
      @exception_handler = Proc.new do |e|
        logger.error "Error running job: #{e.message} (#{e.class})"
        logger.debug e.backtrace.join("\n")
      end

      # Default backoff function
      @backoff_function = Proc.new do |job_runner, job_stats|
        # 2, 3, 4, 6, 8, 11, 15, 20, ..., 72465
        delay = ([1, job_stats["delay"] * 1.3].max).ceil
        if delay > 60 * 60 * 24
          job_runner.bury
        else
          job_runner.release(delay)
        end
      end

      @runners = []
    end

    def server=(uri)
      @server = URI(uri)
    end

    # By default after receiving QUIT juggler will wait up to 2s for running
    # jobs to complete before killing them
    def shutdown_grace_timeout
      @shutdown_grace_timeout || 2
    end

    def logger
      @logger ||= begin
        require 'logger'
        logger = Logger.new(STDOUT)
        logger.level = Logger::WARN
        logger.debug("Created logger")
        logger
      end
    end

    def throw(method, params, options = {})
      # TODO: Do some checking on the method
      connection.use(method.to_s)
      connection.put(@serializer.dump(params), options)
    end

    # Strategy block: should return a deferrable object (so that juggler can
    # apply callbacks and errbacks). You should note that this deferrable may
    # be failed by juggler if the job timeout is exceeded, and therefore you
    # are responsible for cleaning up your state (for example cancelling any
    # timers which you have created)
    def juggle(method, concurrency = 1, &strategy)
      Runner.new(self, method, concurrency, strategy).tap { |r| r.run }
    end

    def stop
      @runners.each { |r| r.stop }
    end

    def running?
      @runners.any? { |r| r.running? }
    end

    # Run sync code with Juggler. The code will be run in a thread pool by
    # using EM.defer
    #
    # Important: Since the code will be ran in multiple threads you should
    # take care to close thread local resources, for example ActiveRecord
    # connections
    #
    # If the block returns without raising an exception or throwing, then the
    # job is considered to have succeeded
    #
    # The job is considered to have failed if the block returns an exception
    # or if :fail is thrown. A second argument may be passed to throw in which
    # case it is treated in exactly the same way as the argument passed to `df.fail` in the async juggle version. For example
    #
    #     juggle_sync(:foo) { |params|
    #       throw(:fail, :no_retry) if some_condition
    #     }
    #
    def juggle_sync(method, concurrency = 1, &strategy)
      defer_wrapper = lambda { |df, params|
        EM.defer {
          begin
            success = nil
            caught_response = catch(:fail) do
              strategy.call(params)
              success = true
            end

            if success
              df.succeed
            else
              df.fail(caught_response)
            end
          rescue => e
            df.fail(e)
          end
        }
      }
      Runner.new(self, method, concurrency, defer_wrapper).tap { |r| r.run }
    end

    def on_disconnect(&blk)
      @on_disconnect = blk
    end

    private

    def add_runner(runner)
      @runners << runner
    end

    def disconnected
      if @on_disconnect
        @on_disconnect.call
      else
        logger.warn "Disconnected"
      end
    end

    def connection
      @connection ||= begin
        c = EMJack::Connection.new({
          :host => server.host,
          :port => server.port
        })
        c.on_disconnect(&self.method(:disconnected))
        c
      end
    end
  end

  class << self
    def default
      @default ||= JugglerInstance.new
    end

    def method_missing(method, *args, &blk)
      default.send(method, *args, &blk)
    end
  end
end

Juggler.autoload 'Runner', 'juggler/runner'
Juggler.autoload 'JobRunner', 'juggler/job_runner'
