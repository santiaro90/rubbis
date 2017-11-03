require "socket"

module Rubbis
  class Server
    def initialize(port)
      @port = port
    end

    def listen
      socket = TCPServer.new(port)

      loop do
        Thread.start(socket.accept) do |client|
          handle_client client
        end
      end
    ensure
      socket.close if socket
    end

    def handle_client(client)
      loop do
        header = client.gets.to_s

        return unless header.start_with?("*")

        num_args = header[1..-1].to_i

        cmd = num_args.times.map do
          len = client.gets[1..-1].to_i
          client.read(len + 2).chomp
        end

        response = case cmd.first.downcase
                   when "ping" then "+PONG\r\n"
                   when "echo" then "$#{cmd[1].length}\r\n#{cmd[1]}\r\n"
                   end

        client.write(response)
      end
    ensure
      client.close
    end

    private

      attr_reader :port
  end
end
