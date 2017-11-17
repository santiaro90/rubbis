require "socket"

require "rubbis/protocol"
require "rubbis/state"

module Rubbis
  class Server
    attr_reader :shutdown_pipe

    def initialize(port)
      @port = port
      @shutdown_pipe = IO.pipe
      @state = State.new(Clock.new)
    end

    def shutdown
      shutdown_pipe[1].close
    end

    def listen
      readable = []
      clients = {}
      server = TCPServer.new(port)

      readable << server
      readable << shutdown_pipe[0]

      running = true

      while running
        ready_to_read = IO.select(readable + clients.keys).first

        ready_to_read.each do |socket|
          case socket
          when server
            child_socket = socket.accept
            clients[child_socket] = Handler.new(child_socket)
          when shutdown_pipe[0]
            running = false
          else
            begin
              clients[socket].process!(@state)
            rescue EOFError
              clients.delete(socket)
              socket.close
            end
          end
        end
      end
    ensure
      (readable + clients.keys).each(&:close)
    end

    class Clock
      def now
        Time.now.to_f
      end

      def sleep(x)
        ::Kernel.sleep x
      end
    end

    class Handler
      attr_reader :client, :buffer

      def initialize(socket)
        @client = socket
        @buffer = ""
      end

      def process!(state)
        buffer << client.read_nonblock(1024)

        cmds, processed = Protocol.unmarshal(buffer)
        @buffer = buffer[processed..-1]

        cmds.each { |cmd| exec_command(cmd, state) }
      end

      def exec_command(cmd, state)
        response = case cmd.first.downcase
                   when "ping" then :pong
                   when "echo" then cmd[1]
                   else state.apply_command(cmd)
                   end

        client.write(Protocol.marshal(response))
      end
    end

    private

    attr_reader :port
  end
end
