require "spec_helper"

require "rubbis/state"

describe Rubbis::State, :unit do
  let(:state) { described_class.new }

  describe "#set" do
    it "sets a value" do
      expect(state.set("abc", "123")).to eq(:ok)
      expect(state.get("abc")).to eq("123")
    end

    it "doesn't overwrite an existing value with NX" do
      expect(state.set("abc", "123", "NX")).to eq(:ok)
      expect(state.set("abc", "456", "NX")).to eq(nil)
      expect(state.get("abc")).to eq("123")
    end

    it "doesn't set a new value with XX" do
      expect(state.set("abc", "123", "XX")).to eq(nil)

      state.set("abc", "123")

      expect(state.set("abc", "456", "XX")).to eq(:ok)
      expect(state.get("abc")).to eq("456")
    end

    it "returns error for wrong number of arguments" do
      expect(state.set("abc")).to eq(Rubbis::Error.incorrect_args("set"))
    end
  end

  describe "#hset" do
    it "sets a value" do
      expect(state.hset("myhash", "abc", "123")).to eq(:ok)
      expect(state.hset("otherhash", "def", "456")).to eq(:ok)
      expect(state.hget("myhash", "abc")).to eq("123")
    end
  end

  describe "#hmget" do
    it "returns multiple values at once" do
      state.hset("myhash", "abc", "123")
      state.hset("myhash", "def", "456")

      expect(state.hmget("myhash", "abc", "def")).to eq(%w[123 456])
    end
  end

  describe "#hincrby" do
    it "increments a counter stored in a hash" do
      state.hset("myhash", "abc", "123")
      expect(state.hincrby("myhash", "abc", "2")).to eq(125)
    end
  end
end
