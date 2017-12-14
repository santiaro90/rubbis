require "spec_helper"

describe Rubbis, :acceptance do
  it "actively expires keys" do
    with_server do
      n = 10

      n.times do |x|
        client.set("keep#{x}", "123")
        client.set("expire#{x}", "123")
        client.pexpire("expire#{x}", rand(600))
      end

      condition = lambda {
        client.keys("*").count { |x| x.start_with?("expire") } == 0
      }

      start_time = Time.now

      sleep 0.01 while !condition.call && Time.now < start_time + 2

      expect(condition.call).to eq(true)
      expect(client.keys("*").size).to eq(n)
    end
  end
end
