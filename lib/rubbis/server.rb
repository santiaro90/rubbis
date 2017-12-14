require "socket"

require "rubbis/handler"
require "rubbis/state"

module Rubbis
  class Server
    attr_reader :shutdown_pipe

    class Clock
      def now
        Time.now.to_f
      end

      def sleep(x)
        ::Kernel.sleep x
      end
    end

    def initialize(port)
      @clock = Clock.new
      @port = port
      @shutdown_pipe = IO.pipe
      @state = Rubbis::State.new(@clock)
    end

    def shutdown
      shutdown_pipe[1].close
    end

    def listen
      clients = {}

      running = true
      server = TCPServer.new(port)
      timer_pipe = IO.pipe

      readable = [server, shutdown_pipe[0], timer_pipe[0]]

      timer_thread = Thread.new do
        begin
          while running
            clock.sleep 0.1
            timer_pipe[1].write(".")
          end
        rescue Errno::EPIPE, IOError
        end
      end

      while running
        ready_to_read = IO.select(readable + clients.keys).first

        ready_to_read.each do |socket|
          case socket
          when server
            child_socket = socket.accept_nonblock
            clients[child_socket] = Rubbis::Handler.new(child_socket)
          when shutdown_pipe[0]
            running = false
          when timer_pipe[0]
            state.expire_keys!
          else
            begin
              clients[socket].process!(state)
            rescue EOFError
              handler = clients.delete(socket)
              handler.disconnect!
              socket.close
            end
          end
        end
      end
    ensure
      running = false

      (readable + clients.keys).each(&:close)

      timer_pipe.each { |f| f.close rescue IOError } if timer_pipe
      timer_thread.join if timer_thread
    end

    private

    attr_reader :port, :clock, :state
  end
end
