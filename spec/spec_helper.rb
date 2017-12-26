require "English"
require "redis"
require "tmpdir"
require "fileutils"

require "rubbis/server"

TEST_PORT = 6380

module AcceptanceHelpers
  def client
    Redis.new(host: "localhost", port: TEST_PORT)
  end

  def with_server
    server = nil
    server_thread = Thread.new do
      server = Rubbis::Server.new(TEST_PORT, test_db)
      server.listen
    end

    wait_for_open_port TEST_PORT

    yield
  rescue Timeout::Error
    server_thread.value unless server_thread.alive?

    raise
  ensure
    server.shutdown if server
  end

  def wait_for_open_port(port)
    time = Time.now

    sleep 0.01 while !check_port(port) && Time.now - time <= 1
    raise Timeout::Error unless check_port(port)
  end

  def check_port(port)
    `nc -z localhost #{port}`
    $CHILD_STATUS.success?
  end
end

class FakeClock
  attr_reader :t

  def initialize
    @t = 0
  end

  def sleep(t)
    @t += t
  end

  alias now t
end

RSpec.configure do |c|
  def test_db
    Dir.tmpdir + "/rubbis_test.dump"
  end

  c.before(:each) do
    FileUtils.rm_f(test_db)
  end

  c.include AcceptanceHelpers, acceptance: true
end
