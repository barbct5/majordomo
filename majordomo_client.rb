$: << File.expand_path('.')

require 'ffi-rzmq'

require 'socket'

require 'mdp'
require 'loggable'

class MajorDomoClient
  include Loggable

  def initialize(broker)
    @broker = broker
    @context = ZMQ::Context.new
    @poller = ZMQ::Poller.new

    @client = nil

    @retries = 3
    @timeout = 2500

    reconnect_to_broker

    Signal.trap("INT") do
      $stdout.write("Shutting down...\n")
      @client.close
      @context.terminate
      exit
    end
  end

  def reconnect_to_broker
    logger.info("Connecting to Broker...")
    if @client
      @poller.deregister(@client)
      @client.close
    end

    @client = @context.socket(ZMQ::DEALER)
    @client.setsockopt(ZMQ::LINGER, 0)
    @client.setsockopt(ZMQ::IDENTITY, "#{Socket.gethostname}:#{Process.pid}")
    @client.connect(@broker)

    @poller.register(@client, ZMQ::POLLIN)
  end

  def send(service, request)
    message = MDP::ClientMessage.new(service, request)
    logger.debug("Sending Message: #{message.inspect}")

    @client.send_strings message.pack
  end

  def receive
    items = @poller.poll(@timeout)
    if items > 0
      raw = []
      @client.recv_strings raw

      MDP::ClientMessage.unpack(raw).body
    else
      nil # No available messages
    end
  end

  def terminate!
    @context.terminate
  end
end

if __FILE__ == $0

  client = MajorDomoClient.new("tcp://localhost:5555")
  requests = 10
  requests.times do
    request = "Hello world"
    client.send("echo", request)
  end

  exit
  count = 0
  while count < requests do
    reply = client.receive
    $stdout.write("#{reply}\n")
    count += 1 unless reply.nil?
  end
end
