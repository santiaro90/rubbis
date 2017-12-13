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
      @clock = clock
      @data = {}
      @expires = {}
      @list_watches = {}
      @ready_keys = []
      @watches = {}
    end

    def self.valid_command?(cmd)
      @valid_comands ||= Set.new(
        public_instance_methods(false).map(&:to_s) - %w[apply_command watch]
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
        break unless expired > n * threshold
      end
    end

    def process_list_watches!
      ready_keys.each do |key|
        list = get(key)
        watches = list_watches.fetch(key, [])

        while list.any? && watches.any?
          op, client = *watches.shift
          client.respond!(op.call)
        end
      end

      ready_keys.clear
    end

    def pexpire(key, value)
      if get(key)
        expires[key] = clock.now + (value.to_i / 1000.0)
        1
      else
        0
      end
    end

    def watch(key, &block)
      watches[key] ||= []
      watches[key] << block if block
      :ok
    end

    def set(*args)
      key, value, modifier = args

      return Error.incorrect_args("set") unless key && value

      nx = modifier == "NX"
      xx = modifier == "XX"
      exists = data.key?(key)

      return if xx && !exists
      return if nx && exists

      touch!(key)
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

    # Sorted sets

    def zadd(key, score, member)
      score = score.to_f
      value = get(key) || data[key] = ZSet.new

      value.add(score, member)
      1
    end

    def zrank(key, member)
      value = get(key)
      value.rank(member) if value
    end

    def zscore(key, member)
      value = get(key)
      value.score(member) if value
    end

    def zrange(key, start, stop)
      value = get(key)

      if value
        value.range(start.to_i, stop.to_i)
      else
        []
      end
    end

    # Lists

    def lpush(key, value)
      list = get(key)
      list ||= data[key] = []

      if list_watches.fetch(key, []).any?
        ready_keys << key
      end

      touch!(key)

      list.unshift(value)
      list.length
    end

    def llen(key)
      list = get(key) || []
      list.length
    end

    def rpop(key)
      list = get(key)
      list ||= data[key] = []

      touch!(key)
      list.pop
    end

    def lrange(key, start, stop)
      list = get(key)

      if list
        list[start.to_i..stop.to_i]
      else
        []
      end
    end

    def brpop(key, client)
      list = get(key)
      list ||= data[key] = []

      action = -> { rpop(key) }

      if list.empty?
        list_watches[key] ||= []
        list_watches[key] << [action, client]
        :block
      else
        action.call
      end
    end

    class ZSet
      attr_reader :entries_to_score, :sorted_by_score

      def initialize
        @entries_to_score = {}
        @sorted_by_score = []
      end

      def add(score, member)
        elem = [score, member]
        index = bsearch_index(sorted_by_score, elem)

        entries_to_score[member] = score
        sorted_by_score.insert(index, elem)
      end

      def range(start, stop)
        sorted_by_score[start..stop].map { |x| x[1] }
      end

      def rank(member)
        score = entries_to_score[member]

        return unless score

        bsearch_index(sorted_by_score, [score, member])
      end

      def score(member)
        entries_to_score[member]
      end

      def bsearch_index(ary, x)
        return 0 if ary.empty?

        low = 0
        high = ary.length - 1

        while high >= low
          idx = low + (high - low) / 2
          comp = ary[idx] <=> x

          return idx if comp.zero?

          if comp > 0
            high = idx - 1
          else
            low = idx + 1
          end
        end

        idx + (comp < 0 ? 1 : 0)
      end
    end

    private

    def touch!(key)
      ws = watches.delete(key) || []
      ws.each(&:call)
    end

    attr_reader :data, :clock, :expires, :list_watches, :ready_keys, :watches
  end
end
