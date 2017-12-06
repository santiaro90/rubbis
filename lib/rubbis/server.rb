require "socket"

require "rubbis/protocol"
require "rubbis/state"

module Rubbis
  class Server
    attr_reader :shutdown_pipe

    def initialize(port)
      @clock = Clock.new
      @port = port
      @shutdown_pipe = IO.pipe
      @state = State.new(@clock)
    end

    def shutdown
      shutdown_pipe[1].close
    end

    def listen
      clients = {}

      server = TCPServer.new(port)
      timer_pipe = IO.pipe

      running = true
      readable = [server, shutdown_pipe[0], timer_pipe[0]]

      timer_thread = Thread.new do
        begin
          while running
            clock.sleep 0.01
            timer_pipe[1].write(".")
          end
        rescue Errno::EPIPE
        end
      end

      while running
        ready_to_read = IO.select(readable + clients.keys).first

        ready_to_read.each do |socket|
          case socket
          when server
            child_socket = socket.accept
            clients[child_socket] = Handler.new(child_socket)
          when shutdown_pipe[0]
            running = false
          when timer_pipe[0]
            state.expire_keys!
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
      running = false

      (readable + clients.keys).each(&:close)
      timer_pipe.close if timer_pipe
      timer_thread.join if timer_thread
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
        @tx_buffer = nil
      end

      def in_tx?
        @tx_buffer
      end

      def process!(state)
        buffer << client.read_nonblock(1024)

        cmds, processed = Protocol.unmarshal(buffer)
        @buffer = buffer[processed..-1]

        cmds.each { |cmd| exec_command(state, cmd) }
      end

      def exec_command(state, cmd)
        response = if in_tx?
                     case cmd.first.downcase
                     when "exec"
                       result = @tx_buffer.map { |cm| dispatch(state, cm) }
                       @tx_buffer = nil
                       result
                     else
                       @tx_buffer << cmd
                       :queued
                     end
                   else
                     dispatch(state, cmd)
                   end

        client.write(Protocol.marshal(response))
      end

      def dispatch(state, cmd)
        case cmd.first.downcase
        when "ping" then :pong
        when "echo" then cmd[1]
        when "multi" then
          @tx_buffer = []
          :ok
        else state.apply_command(cmd)
        end
      end
    end

    private

    attr_reader :port, :clock, :state
  end
end
