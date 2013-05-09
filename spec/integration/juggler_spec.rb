require File.expand_path('../../spec_helper', __FILE__)

describe "@juggler" do
  include EM::SpecHelper
  
  before :each do
    # Reset state
    @juggler = Juggler::JugglerInstance.new

    # Start clean beanstalk instance for each test
    @juggler.server = "beanstalk://localhost:10001"
    @beanstalkd_pid = spawn "beanstalkd -p 10001"
    sleep 0.1
  end
  
  after :each do
    system "kill #{@beanstalkd_pid}"
  end
  
  it "should successfully excecute one job" do
    em(1) do
      params_for_jobs_received = []
      @juggler.juggle(:some_task, 1) { |df, params|
        params_for_jobs_received << params
        df.succeed_later_with(nil)
      }
      @juggler.throw(:some_task, {:some => "params"})
      
      EM.add_timer(0.1) {
        params_for_jobs_received.should == [{:some => "params"}]
        done
      }
    end
  end
  
  it "should run correct number of jobs concurrently" do
    em(1) do
      params_for_jobs_received = []
      @juggler.juggle(:some_task, 2) { |df, params|
        params_for_jobs_received << params
        df.succeed_later_with(nil, 0.2)
      }
      
      10.times { |i| @juggler.throw(:some_task, i) }
      
      EM.add_timer(0.3) {
        # After 0.3 seconds, 2 jobs should have completed, and 2 more started
        params_for_jobs_received.should == [0, 1, 2, 3]
        done
      }
    end
  end

  it "should be possible to stop a juggler instance immediately after creating a runner" do
    em(2) {
      @juggler.juggle(:some_task, 1) { |df, params|
        fail
      }
      @juggler.stop.callback {
        done
      }
    }
  end

  it "should be possible to stop a juggler instance that is idle" do
    em(2) {
      @juggler.juggle(:some_task, 1) { |df, params|
        fail
      }
      EM.add_timer(0.1) {
        @juggler.stop.callback {
          done
        }
      }
    }
  end

  it "should be possible to stop a juggler instance that is reserving and is also running a job" do
    em(2) {
      jobs_started = 0
      jobs_completed = 0
      @juggler.juggle(:some_task, 2) { |df, params|
        jobs_started += 1
        EM.add_timer(0.2) {
          jobs_completed += 1
          df.succeed
        }
      }
      @juggler.throw(:some_task, {})
      EM.add_timer(0.1) {
        @juggler.stop.callback {
          jobs_started.should == 1
          jobs_completed.should == 1
          done
        }
      }
    }
  end

  it "should be possible to stop a juggler instance that is not reserving" do
    em(2) {
      jobs_started = 0
      jobs_completed = 0
      @juggler.juggle(:some_task, 1) { |df, params|
        jobs_started += 1
        EM.add_timer(0.2) {
          jobs_completed += 1
          df.succeed
        }
      }
      @juggler.throw(:some_task, {})
      EM.add_timer(0.1) {
        @juggler.stop.callback {
          jobs_started.should == 1
          jobs_completed.should == 1
          done
        }
      }
    }
  end

  it "should be possible to stop a juggler instance that receives a job after being stopped" do
    em(2) {
      jobs_started = 0
      jobs_completed = 0
      @juggler.juggle(:some_task, 2) { |df, params|
        jobs_started += 1
        EM.add_timer(0.2) {
          jobs_completed += 1
          df.succeed
        }
      }
      @juggler.throw(:some_task, {})
      EM.add_timer(0.1) {
        @juggler.stop.callback {
          jobs_started.should == 1
          jobs_completed.should == 1
          done
        }
        EM.next_tick {
          @juggler.throw(:some_task, {})
        }
      }
    }
  end
end
