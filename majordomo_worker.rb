require 'ffi-rzmq'
load 'mdp.rb'

class MajorDomoWorker
  HEARTBEAT_LIVELINESS = 3

  def initialize(broker, service)
    @broker = broker
    @service = service

    @context = ZMQ::Context.new(1)

    @poller = ZMQ::Poller.new
    @worker = nil
    @heartbeat_at = 0
    @liveliness = 0
    @timeout = 2500
    @heartbeat = 2500
    @reconnect = 2500
    @expect_reply = false
    @reply_to = nil

    Signal.trap("INT") do
      $stdout.write("Shutting down...\n")
      @worker.close
      @context.terminate
      exit
    end

    reconnect_to_broker
  end

  def recv(reply)
    if reply && @reply_to
      reply = reply.is_a?(Array) ? [ @reply_to, '' ].concat(reply) : [ @reply_to, '', reply ]
      send_to_broker(MDP::W_REPLY, nil, reply)
    end

    @expect_reply = true
    loop do
      items = @poller.poll(@timeout)
      if items
        messages = []
        @worker.recv_strings(messages)

        $stdout.write("Received message: #{messages}\n")

        @liveliness = HEARTBEAT_LIVELINESS

        messages.shift
        if messages.shift != MDP::W_WORKER
          $stdout.write("E: header is not MDP::Worker")
        end

        command = messages.shift

        case command
        when MDP::W_REQUEST
          @reply_to = messages.shift
          messages.shift
          return messages
        when MDP::W_HEARTBEAT
          $stdout.write("Heartbeat\n")
        when MDP::W_DISCONNECT
          reconnect_to_broker
        else
          $stdout.write("E: invalid command")
        end
      else
        @liveliness -= 1

        if @liveliness == 0
          sleep 0.001 * @reconnect
          reconnect_to_broker
        end
      end
    end

    if Time.now > @heartbeat_at
      send_to_broker MDP::W_HEARTBEAT
      @heartbeat_at = Time.now + 0.001 * @heartbeat
    end
  end

  def reconnect_to_broker
    if @worker
      @poller.deregister(@worker, ZMQ::DEALER)
      @worker.close
    end

    @worker = @context.socket(ZMQ::DEALER)
    @worker.setsockopt(ZMQ::LINGER, 0)
    @worker.connect(@broker)

    @poller.register(@worker, ZMQ::POLLIN)

    send_to_broker(MDP::W_READY, @service, [])

    @liveliness = HEARTBEAT_LIVELINESS
    @heartbeat_at = Time.now + 0.001 * @heartbeat
  end

  def send_to_broker(command, option = nil, message = nil)
    if message.nil?
      message = []
    elsif !message.is_a?(Array)
      message = [ message ]
    end

    message = [ option ].concat(message) if option

    message = [ '', MDP::W_WORKER, command ].concat(message)
    @worker.send_strings(message)
  end
end

if __FILE__ == $0
  worker = MajorDomoWorker.new("tcp://localhost:5555", 'echo')
  reply = nil

  loop do
    request = worker.recv(reply)
    reply = request
  end
end
