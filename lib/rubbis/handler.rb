require "rubbis/protocol"
require "rubbis/transaction"

module Rubbis
  class Handler
    attr_reader :client, :buffer, :tx

    def initialize(socket)
      @client = socket
      @buffer = ""

      reset_tx!
    end

    def process!(state)
      buffer << client.read_nonblock(1024)

      cmds, processed = Rubbis::Protocol.unmarshal(buffer)
      @buffer = buffer[processed..-1]

      cmds.each { |cmd| exec_command(state, cmd) }
    end

    def reset_tx!
      @tx = Rubbis::Transaction.new
    end

    def exec_command(state, cmd)
      response = if tx.active?
                   case cmd.first.downcase
                   when "exec"
                     result = unless tx.dirty?
                                tx.buffer.map do |cm|
                                  dispatch(state, cm)
                                end
                              end

                     reset_tx!

                     result
                   else
                     tx.queue(cmd)
                     :queued
                   end
                 else
                   dispatch(state, cmd)
                 end

      respond!(response) unless response == :block
      state.process_list_watches!
    end

    def respond!(response)
      client.write(Rubbis::Protocol.marshal(response)) if client
    end

    def active?
      client
    end

    def disconnect!
      @client = nil
    end

    def dispatch(state, cmd)
      case cmd.first.downcase
      when "multi"
        tx.start!
        :ok
      when "watch" then
        current_tx = tx
        state.watch(cmd[1]) { tx.dirty! if current_tx == tx }
      else state.apply_command(self, cmd)
      end
    end
  end
end
