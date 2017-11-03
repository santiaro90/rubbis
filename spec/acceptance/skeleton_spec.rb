require "redis"

require "rubbis/server"

TEST_PORT = 6380

describe Rubbis::Server, :acceptance do
  it "responds to ping" do
    with_server do
      c = client
      c.without_reconnect do
        expect(c.ping).to eq("PONG")
        expect(c.ping).to eq("PONG")
      end
    end
  end

  it "supports multiple clients simultaneously" do
    with_server do
      expect(client.echo("hello\nthere")).to eq("hello\nthere")
      expect(client.echo("hello\nthere")).to eq("hello\nthere")
    end
  end

  it "echoes messages" do
    with_server do
      expect(client.echo("hello\nthere")).to eq("hello\nthere")
    end
  end

  def client
    Redis.new(host: "localhost", port: TEST_PORT)
  end

  def with_server
    server_thread = Thread.new do
      server = Rubbis::Server.new(TEST_PORT)
      server.listen
    end

    wait_for_open_port TEST_PORT

    yield
  rescue Timeout::Error
    sleep 0.01
    server_thread.value unless server_thread.alive?

    raise
  ensure
    Thread.kill(server_thread) if server_thread
  end

  def wait_for_open_port(port)
    time = Time.now
    while !check_port(port) && 1 > Time.now - time
      sleep 0.01
    end

    raise Timeout::Error unless check_port(port)
  end

  def check_port(port)
    `nc -z localhost #{port}`
    $?.success?
  end
end
