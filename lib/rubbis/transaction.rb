module Rubbis
  class Transaction
    attr_reader :buffer

    def initialize
      @active = false
      @dirty = false
      @buffer = []
    end

    def start!
      raise if active?
      @active = true
    end

    def dirty!
      @dirty = true
    end

    def dirty?
      @dirty
    end

    def active?
      @active
    end

    def queue(cmd)
      raise unless active?
      @buffer << cmd
    end
  end
end
