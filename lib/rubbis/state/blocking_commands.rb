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
  end
end
