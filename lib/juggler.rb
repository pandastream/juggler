require 'em-jack'
require 'eventmachine'
require 'uri'

class Juggler
  class JugglerInstance
    attr_writer :logger
    attr_writer :shutdown_grace_timeout
    attr_accessor :exception_handler
    attr_accessor :backoff_function
    attr_reader :server

    def initialize(options = {})
      @server = URI(options[:server]) || URI("beanstalk://localhost:11300")
      @logger = options[:logger]

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
      connection.put(Marshal.dump(params), options)
    end

    # Strategy block: should return a deferrable object (so that juggler can 
    # apply callbacks and errbacks). You should note that this deferrable may 
    # be failed by juggler if the job timeout is exceeded, and therefore you 
    # are responsible for cleaning up your state (for example cancelling any 
    # timers which you have created)
    def juggle(method, concurrency = 1, &strategy)
      Runner.new(self, method, concurrency, strategy).run
    end

    def stop
      @runners.each { |r| r.stop }
    end

    def running?
      @runners.any? { |r| r.running? }
    end

    private

    def add_runner(runner)
      @runners << runner
    end

    def connection
      @connection ||= EMJack::Connection.new({
        :host => server.host,
        :port => server.port
      })
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

require 'juggler/runner'
require 'juggler/job_runner'
