require "spec_helper"

describe Rubbis, :acceptance do
  it "supports blocking pop" do
    with_server do
      items = %w[a b]

      t1 = Thread.new do
        item = client.brpop("q")
        expect(item).to eq(items.shift)
      end

      t2 = Thread.new do
        item = client.brpop("q")
        expect(item).to eq(items.shift)
      end

      items.dup.each do |item|
        expect(client.lpush("q", item)).to eq(1)
      end

      t1.value
      t2.value
    end
  end

  it "supports brpoplpush" do
    with_server do
      c = client

      t1 = Thread.new do
        item = c.brpoplpush("q", "processing")
        expect(item).to eq("a")
        expect(c.lrange("processing", 0, -1)).to eq(%w[a])
      end

      c.lpush("q", "a")

      t1.value
    end
  end

  it "handles disconnecting clients" do
    with_server do
      s = TCPSocket.new("localhost", 6380)
      s.write("*3\r\n$5\r\nbrpop\r\n$1\r\nq\r\n$1\r\n0\r\n")
      s.close

      expect(client.lpush("q", "a")).to eq(1)
      expect(client.lpush("q", "b")).to eq(2)
    end
  end
end
