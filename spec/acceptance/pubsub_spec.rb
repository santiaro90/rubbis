require "spec_helper"

describe Rubbis, :acceptance do
  it "supports pub/sub" do
    with_server do
      c = client
      received = nil

      t1 = Thread.new do
        c.subscribe("mychannel") do |on|
          on.message do |channel, msg|
            received = msg
            c.unsubscribe("mychannel")
          end
        end
      end

      sleep 0.1

      client.publish("mychannel", "hello")
      t1.value

      expect(received).to eq("hello")
    end
  end

  it "supports pub/sub with patterns" do
    with_server do
      c = client
      received = nil

      t1 = Thread.new do
        c.psubscribe("my.*") do |on|
          on.message do |channel, msg|
            received = msg
            c.punsubscribe("my.*")
          end
        end
      end

      sleep 0.1

      client.publish("your.channel", "bogus")
      client.publish("my.channel", "hello")
      t1.value

      expect(received).to eq("hello")
    end
  end
end
