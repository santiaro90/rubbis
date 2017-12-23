module Rubbis
  module BlockingCommands
    def brpop(key, timeout, client)
      action = -> { rpop(key) }

      if llen(key) > 0
        action.call
      else
        list_watches[key] ||= []
        list_watches[key] << [action, client]
        :block
      end
    end

    def brpoplpush(source, dest, timeout, client)
      action = -> { rpoplpush(source, dest) }

      if llen(source) > 0
        action.call
      else
        list_watches[source] ||= []
        list_watches[source] << [action, client]
        :block
      end
    end

    def subscribe(channel, client)
      subscribers[channel] << client
      channels[client] << channel

      ["subscribe", channel, channel_count(client)]
    end

    def unsubscribe(channel, client)
      subscribers[channel].delete(client)
      channels[client].delete(channel)

      ["unsubscribe", channel, channel_count(client)]
    end

    def psubscribe(channel, client)
      psubscribers[channel] << client
      pchannels[client] << channel

      ["psubscribe", channel, channel_count(client)]
    end

    def punsubscribe(channel, client)
      psubscribers[channel].delete(client)
      pchannels[client].delete(channel)

      ["punsubscribe", channel, channel_count(client)]
    end
  end
end
