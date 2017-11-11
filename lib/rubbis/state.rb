require "set"

module Rubbis
  Error = Struct.new(:message) do
    def self.incorrect_args(cmd)
      new "wrong number of arguments for '#{cmd}' command"
    end

    def self.unknown_cmd(cmd)
      new "unknown command '#{cmd}'"
    end
  end

  class State
    def initialize
      @data = {}
    end

    def self.valid_command?(cmd)
      @valid_comands ||= Set.new(
        public_instance_methods(false).map(&:to_s) - ["apply_command"]
      )

      @valid_comands.include?(cmd)
    end

    def apply_command(cmd)
      return Error.unknown_cmd(cmd[0]) unless State.valid_command?(cmd[0])

      public_send(*cmd)
    end

    def set(*args)
      key, value, modifier = args

      return Error.incorrect_args("set") unless key && value

      nx = modifier == "NX"
      xx = modifier == "XX"
      exists = data.key?(key)

      return if xx && !exists
      return if nx && exists

      data[key] = value
      :ok
    end

    def get(key)
      data[key]
    end

    private

    attr_reader :data
  end
end
