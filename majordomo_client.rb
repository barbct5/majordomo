require 'ffi-rzmq'
load 'mdp.rb'

class MajorDomoClient

  def initialize(broker)
    @broker = broker
    @context = ZMQ::Context.new
    @poller = ZMQ::Poller.new

    @client = nil

    @retries = 3
    @timeout = 2500

    reconnect_to_broker
  end

  def reconnect_to_broker
    if @client
      @poller.deregister(@client)
      @client.close
    end

    @client = @context.socket(ZMQ::REQ)
    @client.setsockopt(ZMQ::LINGER, 0)
    @client.connect(@broker)

    @poller.register(@client, ZMQ::POLLIN)
  end

  def send(service, request)
    request = [ request ] unless request.is_a?(Array)
    request = [ MDP::C_CLIENT, service ].concat(request)

    retries = @retries

    while retries > 0
      @client.send_multipart(request)
      items = @poller.poll(@timeout)

      reply = nil

      if items
        message = @client.recv_multipart
        header = message.shift
        reply_service  = message.shift

        reply = message
        break
      else
        retries ? reconnect_to_broker : break
        retries -= 1
      end

      reply
    end
  end

  def terminate!
    @context.terminate
  end
end

if __FILE__ == $0
  client = MajorDomoClient.new("tcp://localhost:5555")
  (0..100000).each do
    request = "Hello world"
    reply = client.send("echo", request)
    if reply
      puts "Received reply: #{reply}"
    else
      puts "No reply receive"
    end
  end
end
