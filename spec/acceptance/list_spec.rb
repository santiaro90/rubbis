require "spec_helper"

describe Rubbis, :acceptance do
  it "supports blocking pop" do
    with_server do
      items = %w[a b]

      t1 = Thread.new do
        item = client.brpop("queue")
        expect(item).to eq(items.shift)
      end

      t2 = Thread.new do
        item = client.brpop("queue")
        expect(item).to eq(items.shift)
      end

      items.dup.each do |item|
        expect(client.lpush("queue", item)).to eq(1)
      end

      t1.value
      t2.value
    end
  end
end
