require "spec_helper"

require "rubbis/protocol"
require "rubbis/state"

describe Rubbis::Protocol do
  describe ".marshal" do
    def self.it_marshals(ruby, wire)
      specify ruby.inspect do
        expect(described_class.marshal(ruby)).to eq(wire)
      end
    end

    it_marshals 10, ":10\r\n"
    it_marshals "hello", "$5\r\nhello\r\n"
    it_marshals %w[a bc], "*2\r\n$1\r\na\r\n$2\r\nbc\r\n"
    it_marshals :ok, "+OK\r\n"
    it_marshals Rubbis::Error.incorrect_args("cmd"), \
                "-ERR wrong number of arguments for 'cmd' command\r\n"
    it_marshals nil, "$-1\r\n"
  end
end
