module Rubbis
  class Protocol
    def self.marshal(ruby)
      case ruby
      when Array then "*#{ruby.length}\r\n#{ruby.map { |x| marshal(x) }.join}"
      when Error then "-ERR #{ruby.message}\r\n"
      when Integer then ":#{ruby.to_i}\r\n"
      when String then "$#{ruby.length}\r\n#{ruby}\r\n"
      when Symbol then "+#{ruby.to_s.upcase}\r\n"
      when nil then "$-1\r\n"
      else raise "Don't know how to marshall #{ruby}"
      end
    end
  end
end
