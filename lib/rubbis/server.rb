require "pp"
require "socket"
require "stringio"
require "tempfile"

require "rubbis/handler"
require "rubbis/state"

module Rubbis
  class Server
    class Clock
      def now
        Time.now.to_f
      end

      def sleep(x)
        ::Kernel.sleep x
      end
    end

    class AofClient < StringIO
      def write(*_); end
    end

    def initialize(opts = {})
      @aof_file = opts[:aof_file]
      @port = opts[:port]
      @server_file = opts[:server_file]

      @bgsaves = []
      @clock = Clock.new
      @last_fsync = 0
      @shutdown_pipe = IO.pipe
      @state = Rubbis::State.new(@clock)
    end

    def shutdown
      shutdown_pipe[1].close
    end

    def listen
      @command_log = File.open(aof_file, "a") if aof_file

      if aof_file && File.exist?(aof_file)
        apply_log(File.read(aof_file))
      elsif server_file && File.exist?(server_file)
        @state.deserialize(File.read(server_file))
      end

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
            clients[child_socket] = Rubbis::Handler.new(child_socket, self)
          when shutdown_pipe[0]
            running = false
          when timer_pipe[0]
            state.expire_keys!
            check_background_processes!

            if command_log && clock.now - last_fsync >= 1
              @last_fsync = clock.now
              command_log.fdatasync
            end
          else
            begin
              clients[socket].process!(state)
            rescue EOFError
              handler = clients.delete(socket)
              handler.disconnect!(state)
              socket.close
            end
          end
        end
      end
    ensure
      running = false

      bgsaves.each { |pid| Process.wait(pid) }
      (readable + clients.keys).each(&:close)

      timer_pipe.each { |f| f.close rescue IOError } if timer_pipe
      timer_thread.join if timer_thread
    end

    def apply_log(contents)
      return if contents.empty?
      Handler.new(AofClient.new(contents), self).process!(state)
    end

    def commit!
      return unless command_log

      state.log.each do |cmd|
        command_log.write(Rubbis::Protocol.marshal(cmd))
      end

      state.log.clear
    end

    def bgsave
      return unless server_file

      bgsaves << fork do
        begin
          tmp_file = Tempfile.new(File.basename(server_file))

          tmp_file.write(state.serialize)
          tmp_file.close

          FileUtils.mv(tmp_file, server_file)
        ensure
          if tmp_file
            tmp_file.close
            tmp_file.unlink
          end
        end
      end
    end

    def check_background_processes!
      bgsaves.delete_if do |pid|
        result = Process.waitpid2(pid, Process::WNOHANG)

        if result
          # Check exit status, update metadata
          true
        end
      end
    end

    private

    attr_reader :port, :clock, :server_file,
                :shutdown_pipe, :state, :bgsaves,
                :aof_file, :command_log, :last_fsync
  end
end
