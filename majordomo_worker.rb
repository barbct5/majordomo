$: << File.expand_path('.')

require 'ffi-rzmq'

require 'socket'

require_relative 'mdp'
require_relative 'loggable'

class MajorDomoWorker
  include Loggable

  HEARTBEAT_LIVELINESS = 3
  MAX_RECONNECT = 18000

  def initialize(broker, service)
    @broker   = broker
    @service  = service

    @context = ZMQ::Context.new

    @heartbeat_at = 0
    @liveliness   = HEARTBEAT_LIVELINESS
    @timeout      = 2500
    @heartbeat    = 2500
    @reconnect    = 2500

    register_signals
    connect_to_broker
  end
  attr_reader :broker, :service

  def to_s
    "#<MajorDomoWorker broker: \"#{broker}\" service: \"#{service}\" >"
  end

  def inspect
    to_s
  end

  def timeout(value)
    @timeout = value
    self
  end

  def heartbeat(value)
    @heartbeat = value
    self
  end

  def liveliness(value)
    @liveliness = value
    self
  end

  def reconnect(value)
    @reconnect = value
    self
  end

  def receive
    loop do
      items = @poller.poll(@timeout)
      if items > 0
        raw = []
        @worker.recv_strings(raw)

        message = MDP::WorkerMessageParser.unpack(raw)
        logger.debug("Recieved Message from Broker: #{message.pack}")

        case message.command
        when MDP::W_REQUEST
          @reply_to = message.client_address
          reset_liveliness!
          logger.debug("Received message: #{message} from #{@reply_to}")
          return message.body
        when MDP::W_HEARTBEAT
          logger.debug("Received heartbeat from Broker")
          reset_liveliness!
        when MDP::W_DISCONNECT
          logger.info("Received disconnect. Disconnecting...")
          reconnect_to_broker
        end
      else
        no_messages!
      end

      heartbeat!
    end
  end

  def respond(reply)
    if reply && @reply_to
      message = MDP::ReplyMessage.new(@reply_to, reply)
      send_to_broker(message)
    end
  end

  private

  def no_messages!
    logger.debug("No items available...")

    @liveliness -= 1

    if @liveliness <= 0
      logger.info("No heartbeat from Broker. Attempting reconnect in #{0.001 * @reconnect}s")
      sleep(0.001 * @reconnect)
      reconnect_to_broker
    end
  end

  def reset_liveliness!
    @reconnect = 2500
    @liveliness = HEARTBEAT_LIVELINESS
  end

  def heartbeat!
    if Time.now > @heartbeat_at
      logger.debug("Sending heartbeat to Broker")
      send_to_broker(MDP::HeartbeatMessage.new)
      @heartbeat_at = Time.now + 0.001 * @heartbeat
      true
    else
      logger.debug("Not ready to heartbeat")
      false
    end
  end

  def register_signals
    [ "INT", "TERM", "QUIT" ].map do |signal|
      Signal.trap(signal, &method(:shutdown!))
    end
  end

  def reconnect_to_broker
    disconnect_from_broker
    @reconnect = [ @reconnect * 2, MAX_RECONNECT ].min
    connect_to_broker
  end

  def disconnect_from_broker
    if @worker
      @poller.deregister(@worker, ZMQ::DEALER)
      @worker.close
    end
  end

  def connect_to_broker
    logger.debug("Connecting to Broker...")

    @poller = ZMQ::Poller.new

    @worker = @context.socket(ZMQ::DEALER)
    @worker.setsockopt(ZMQ::LINGER, 0)
    @worker.setsockopt(ZMQ::IDENTITY, "#{Socket.gethostname}:#{Process.pid}")
    @worker.connect(@broker)

    @poller.register_readable(@worker)

    message = MDP::ReadyMessage.new(@service)
    send_to_broker(message)
    logger.info("Connected to Broker [#{@broker}]")

    @liveliness = HEARTBEAT_LIVELINESS
    @heartbeat_at = Time.now + 0.001 * @heartbeat
  end

  def send_to_broker(message)
    logger.debug("Sending message #{message.pack.inspect}...")
    @worker.send_strings(message.pack).tap do
      logger.debug("Message sent!")
    end
  end

  def shutdown!(signal = nil)
    disconnect_from_broker
    @context.terminate
    exit
  end
end

if __FILE__ == $0
  worker = MajorDomoWorker.new("tcp://localhost:5555", 'echo')
  reply = nil

  loop do
    request = worker.receive
    $stdout.write("Processing #{request}\n")
    worker.respond("Done!")
  end
end
