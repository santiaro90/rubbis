require "spec_helper"

describe Rubbis, :acceptance do
  it "can persist data" do
    with_server do
      client.set("a", "1")
      client.bgsave
    end

    sleep 0.1

    with_server do
      expect(client.get("a")).to eq("1")
    end
  end
end
