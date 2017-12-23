module Rubbis
  module StateCommands
    def ping
      :pong
    end

    def echo(val)
      val
    end

    def exists(key)
      data[key] ? 1 : 0
    end

    def expire(key, value)
      pexpire(key, value.to_i * 1000)
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

    def rpoplpush(source, dest)
      item = rpop(source)

      return unless item

      lpush(dest, item)
      item
    end

    def publish(channel, message)
      subscribers_for(channel).each do |client|
        client.respond!(["message", channel, message])
      end.length
    end
  end
end
