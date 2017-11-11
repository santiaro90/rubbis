require "socket"

require "rubbis/protocol"

module Rubbis
  class Server
    attr_reader :shutdown_pipe

    def initialize(port)
      @port = port
      @shutdown_pipe = IO.pipe
      @data = {}
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
              clients[socket].process!(@data)
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

    class Handler
      attr_reader :client, :buffer

      def initialize(socket)
        @client = socket
        @buffer = ""
      end

      def process!(data)
        buffer << client.read_nonblock(1024)

        cmds, processed = unmarshal(buffer)
        @buffer = buffer[processed..-1]

        cmds.each do |cmd|
          response = case cmd.first.downcase
                     when "ping" then :pong
                     when "echo" then cmd[1]
                     when "set" then
                       data[cmd[1]] = cmd[2]
                       :ok
                     when "get" then
                       data[cmd[1]]
                     end

          client.write Rubbis::Protocol.marshal(response)
        end
      end

      class ProtocolError < RuntimeError; end

      def unmarshal(data)
        io = StringIO.new(data)
        result = []
        processed = 0

        begin
          loop do
            header = safe_readline(io)

            raise ProtocolError unless header.start_with?("*")

            n = header[1..-1].to_i

            result << n.times.map do
              raise ProtocolError unless io.readpartial(1) == "$"

              length = safe_readline(io).to_i
              safe_readpartial(io, length).tap do
                safe_readline(io)
              end
            end

            processed = io.pos
          end
        rescue ProtocolError
          processed = io.pos
        rescue EOFError
          # Incomplete command, ignore
        end

        [result, processed]
      end

      def safe_readline(io)
        io.readline("\r\n").tap do |line|
          raise EOFError unless line.end_with?("\r\n")
        end
      end

      def safe_readpartial(io, length)
        io.readpartial(length).tap do |data|
          raise EOFError unless data.length == length
        end
      end
    end

    private

      attr_reader :port
  end
end
