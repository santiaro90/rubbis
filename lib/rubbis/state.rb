require "set"

require "rubbis/state/blocking_commands"
require "rubbis/state/state_commands"
require "rubbis/state/zset"

module Rubbis
  Error = Struct.new(:message) do
    def self.incorrect_args(cmd)
      new "wrong number of arguments for '#{cmd}' command"
    end

    def self.type_error
      new "wrong type for command"
    end

    def self.unknown_cmd(cmd)
      new "unknown command '#{cmd}'"
    end
  end

  class State
    include Rubbis::BlockingCommands
    include Rubbis::StateCommands

    def initialize(clock)
      @clock = clock
      @data = {}
      @expires = {}
      @list_watches = {}
      @ready_keys = []
      @watches = {}
    end

    def self.valid_command?(cmd)
      @valid_comands ||= Set.new(
        StateCommands.public_instance_methods.map(&:to_s) - %w[apply_command watch]
      )

      @valid_comands.include?(cmd) || blocking_command?(cmd)
    end

    def self.blocking_command?(cmd)
      @blocking_commands ||= Set.new(
        BlockingCommands.public_instance_methods.map(&:to_s)
      )

      @blocking_commands.include?(cmd)
    end

    def apply_command(client, cmd)
      return Error.unknown_cmd(cmd[0]) unless State.valid_command?(cmd[0])

      cmd << client if State.blocking_command?(cmd[0])

      public_send(*cmd)
    end

    def expire_keys!(n: 100, threshold: 0.25, rng: Random.new)
      loop do
        expired = expires.keys.sample(n, random: rng).count { |key| get(key) }
        break unless expired > n * threshold
      end
    end

    def process_list_watches!
      ready_keys.each do |key|
        list = get(key)
        watches = list_watches.fetch(key, [])

        while list.any? && watches.any?
          op, client = *watches.shift
          client.respond!(op.call) if client.active?
        end
      end

      ready_keys.clear
    end

    def watch(key, &block)
      watches[key] ||= []
      watches[key] << block if block
      :ok
    end

    private

    def touch!(key)
      ws = watches.delete(key) || []
      ws.each(&:call)
    end

    attr_reader :data, :clock, :expires, :list_watches, :ready_keys, :watches
  end
end
