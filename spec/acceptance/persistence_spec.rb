require "spec_helper"

describe Rubbis, :acceptance do
  it "can persist data" do
    with_server(server_file: true) do
      client.set("a", "1")
      client.bgsave
    end

    sleep 0.1

    with_server(server_file: true) do
      expect(client.get("a")).to eq("1")
    end
  end

  it "can persist data with an AOF" do
    with_server(aof_file: true) do
      client.set("a", "1")
      client.bgsave
    end

    sleep 0.1

    with_server(aof_file: true) do
      expect(client.get("a")).to eq("1")
    end
  end
end
