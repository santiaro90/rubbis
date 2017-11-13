module Rubbis
  class ProtocolError < RuntimeError; end

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

    def self.unmarshal(data)
      io = StringIO.new(data)
      result = []
      processed = 0

      begin
        loop do
          header = safe_readline(io)

          raise ProtocolError unless header.start_with?("*")

          n = header[1..-1].to_i

          result << Array.new(n) do
            raise ProtocolError unless io.readpartial(1) == "$"

            length = safe_readline(io).to_i
            safe_readpartial(io, length).tap do
              safe_readline(io)
            end
          end

          processed = io.pos
        end
      rescue ProtocolError
        processed = io.pos
      rescue EOFError
        # Incomplete command, ignore
      end

      [result, processed]
    end

    def self.safe_readline(io)
      io.readline("\r\n").tap do |line|
        raise EOFError unless line.end_with?("\r\n")
      end
    end

    def self.safe_readpartial(io, length)
      io.readpartial(length).tap do |data|
        raise EOFError unless data.length == length
      end
    end
  end
end
