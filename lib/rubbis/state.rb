require "set"

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
    def initialize(clock)
      @data = {}
      @expires = {}
      @clock = clock
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

    def exists(key)
      data[key] ? 1 : 0
    end

    def expire(key, value)
      pexpire(key, value.to_i * 1000)
    end

    def expire_keys!(n: 100, threshold: 0.25, rng: Random.new)
      loop do
        expired = expires.keys.sample(n, random: rng).count { |key| get(key) }
        break unless expired > threshold
      end
    end

    def pexpire(key, value)
      if get(key)
        expires[key] = clock.now + (value.to_i / 1000.0)
        1
      else
        0
      end
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
      expiry = expires[key]
      del(key) if expiry && expiry <= clock.now

      data[key]
    end

    def del(key)
      expires.delete(key)
      data.delete(key)
    end

    def hset(hash, key, value)
      data[hash] ||= {}

      data[hash][key] = value
      :ok
    end

    def hget(hash, key)
      value = get(hash)
      value[key] if value
    end

    def hmget(hash, *keys)
      existing = get(hash) || {}

      if existing.is_a?(Hash)
        existing.values_at(*keys)
      else
        Error.type_error
      end
    end

    def hincrby(hash, key, amount)
      value = get(hash)

      return unless value

      existing = value[key]
      value[key] = existing.to_i + amount.to_i
    end

    def keys(pattern)
      raise "unimplemented" unless pattern == "*"
      data.keys
    end

    private

    attr_reader :data, :clock, :expires
  end
end
