require "spec_helper"

describe "Rubbis", :acceptance do
  it "supports hashes" do
    with_server do
      client.hset("myhash", "abc", "123")
      client.hset("myhash", "def", "456")

      expect(client.hmget("myhash", "abc", "def")).to eq(%w[123 456])
    end
  end
end
