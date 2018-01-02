require "spec_helper"

require "rubbis/state"

describe Rubbis::State, :unit do
  let(:clock) { FakeClock.new }
  let(:state) { described_class.new(clock) }

  shared_examples "passive expiry" do |set_cmd, get_cmd|
    it "expires a key passively" do
      key = "abc"

      set_cmd.call(state, key)
      state.expire("abc", "1")

      clock.sleep 0.9
      get_cmd.call(state, key)
      expect(state.exists("abc")).to eq(1)

      clock.sleep 0.1
      get_cmd.call(state, key)
      expect(state.exists("abc")).to eq(0)
    end
  end

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

  describe "#get" do
    include_examples "passive expiry",
                     ->(s, k) { s.set(k, "123") },
                     ->(s, k) { s.get(k) }
  end

  describe "#exists" do
    it "returns the number of keys existing" do
      expect(state.exists("abc")).to eq(0)

      state.set("abc", "123")
      expect(state.exists("abc")).to eq(1)
    end
  end

  describe "#hset" do
    it "sets a value" do
      expect(state.hset("myhash", "abc", "123")).to eq(:ok)
      expect(state.hset("otherhash", "def", "456")).to eq(:ok)
      expect(state.hget("myhash", "abc")).to eq("123")
    end
  end

  describe "#hget" do
    include_examples "passive expiry",
                     ->(s, k) { s.hset(k, "abc", "123") },
                     ->(s, k) { s.hget(k, "abc") }
  end

  describe "#hmget" do
    it "returns multiple values at once" do
      state.hset("myhash", "abc", "123")
      state.hset("myhash", "def", "456")

      expect(state.hmget("myhash", "abc", "def")).to eq(%w[123 456])
    end

    it "returns error when not hash value" do
      state.set("myhash", "bogus")
      expect(state.hmget("myhash", "key")).to eq(Rubbis::Error.type_error)
    end

    it "returns nil when empty" do
      expect(state.hmget("myhash", "key")).to eq([nil])
    end

    include_examples "passive expiry",
                     ->(s, k) { s.hset(k, "abc", "123") },
                     ->(s, k) { s.hmget(k, "abc") }
  end

  describe "#hincrby" do
    it "increments a counter stored in a hash" do
      state.hset("myhash", "abc", "123")
      expect(state.hincrby("myhash", "abc", "2")).to eq(125)
    end

    include_examples "passive expiry",
                     ->(s, k) { s.hset(k, "abc", "123") },
                     ->(s, k) { s.hincrby(k, "abc", "1") }
  end

  describe "#keys" do
    it "returns all keys in the database for *" do
      state.set("abc", "123")
      state.set("def", "123")

      expect(state.keys("*")).to eq(%w[abc def])
    end
  end

  describe "sorted set" do
    it "fetches keys by ranks" do
      state.zadd("leaderboard", "1000", "alice")
      state.zadd("leaderboard", "3000", "bob")
      state.zadd("leaderboard", "2000", "charlie")

      expect(state.zrange("leaderboard", "0", "1")).to eq(%w[alice charlie])
    end

    it "fetches rank by member" do
      state.zadd("leaderboard", "1000", "alice")
      state.zadd("leaderboard", "3000", "bob")
      state.zadd("leaderboard", "2000", "charlie")

      expect(state.zrank("leaderboard", "charlie")).to eq(1)
    end

    it "fetches score by member" do
      state.zadd("leaderboard", "1000", "alice")
      state.zadd("leaderboard", "3000", "bob")
      state.zadd("leaderboard", "2000", "charlie")

      expect(state.zscore("leaderboard", "charlie")).to eq(2000)
    end

    it "breaks ties using values" do
      state.zadd("leaderboard", "1000", "alice")
      state.zadd("leaderboard", "1000", "bob")
      state.zadd("leaderboard", "1000", "charlie")

      expect(state.zrange("leaderboard", "0", "1")).to eq(%w[alice bob])
    end
  end

  describe "lists" do
    it "supports rpoplpush" do
      state.lpush("q", "a")
      state.lpush("q", "b")
      state.lpush("q", "c")

      expect(state.rpoplpush("q", "p")).to eq("a")
      expect(state.lrange("q", "0", "-1")).to eq(%w[c b])
      expect(state.lrange("p", "0", "-1")).to eq(%w[a])
    end

    it "supports cyclic rpoplpush" do
      state.lpush("q", "a")
      state.lpush("q", "b")
      state.lpush("q", "c")

      expect(state.rpoplpush("q", "q")).to eq("a")
      expect(state.lrange("q", "0", "-1")).to eq(%w[a c b])
    end

    it "supports basic operations" do
      state.lpush("q", "a")
      state.lpush("q", "b")
      state.lpush("q", "c")

      expect(state.llen("q")).to eq(3)
      expect(state.lrange("q", "0", "1")).to eq(%w[c b])

      expect(state.rpop("q")).to eq("a")
      expect(state.llen("q")).to eq(2)
    end
  end

  describe "#pexpireat" do
    it "sets absolute expiry" do
      state.set("abc", "123")
      state.pexpireat("abc", "1000")

      clock.sleep 0.9
      expect(state.get("abc")).to eq("123")

      clock.sleep 0.1
      expect(state.get("abc")).to eq(nil)
    end
  end
end
