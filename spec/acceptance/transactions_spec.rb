require "spec_helper"

describe Rubbis::Server, :acceptance do
  it "handles transactions" do
    with_server do
      c = client

      result = c.multi do
        c.set("abc", "123")
        c.get("abc")
      end

      expect(result).to eq(%w[OK 123])

      begin
        c.multi do
          c.set("abc", "456")
          raise
        end
      rescue
        expect(c.get("abc")).to eq("123")
      end
    end
  end
end
