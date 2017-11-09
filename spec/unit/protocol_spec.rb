require "spec_helper"

require "rubbis/protocol"

describe Rubbis::Protocol do
  describe ".marshal" do
    def self.it_marshals(ruby, wire)
      specify ruby.inspect do
        expect(described_class.marshal ruby).to eq(wire)
      end
    end

    it_marshals :ok, "+OK\r\n"
    it_marshals nil, "$-1\r\n"
    it_marshals "hello", "$5\r\nhello\r\n"
  end
end
