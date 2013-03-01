$: << File.expand_path('.')

require 'ffi-rzmq'

require 'delegate'
require 'socket'

require 'mdp'
require 'loggable'
require 'majordomo_broker/message_listener'
require 'majordomo_broker/worker_handler'
require 'majordomo_broker/addressable_message'
require 'majordomo_broker/worker'
require 'majordomo_broker/service'

class MajorDomoBroker
  include Loggable

  HEARTBEAT_INTERVAL = 2500
  HEARTBEAT_LIVELINESS = 3
  HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVELINESS

  def initialize(address)

    @context = ZMQ::Context.new

    # Configure Sockets
    @socket = @context.socket(ZMQ::ROUTER)
    @socket.setsockopt(ZMQ::LINGER, 0)
    @socket.setsockopt(ZMQ::IDENTITY, "#{Socket.gethostname}:#{Process.pid}")
    @socket.setsockopt(ZMQ::ROUTER_BEHAVIOR, 1)

    # Configure Poller
    @poller = ZMQ::Poller.new
    @poller.register(@socket, ZMQ::POLLIN)

    # Set next heartbeat
    @heartbeat_at = Time.now + (0.001 * HEARTBEAT_INTERVAL)

    @socket.bind(address)

    register_signals
  end

  def mediate
    loop do
      items = @poller.poll(HEARTBEAT_INTERVAL)
      if items > 0
        raw = []
        @socket.recv_strings(raw)

        address = raw.shift
        header = raw[1]

        case header
        when MDP::C_CLIENT
          message = MDP::ClientMessage.unpack(raw)
          process_client(AddressableMessage.new(address, message))
        when MDP::W_WORKER
          message = MDP::WorkerMessageParser.unpack(raw)
          process_worker(AddressableMessage.new(address, message))
        else
          logger.warn("Received invalid message: #{raw.inspect}")
        end
      end

      heartbeat!
    end
  end

  def heartbeat!
    if Time.now > @heartbeat_at
      handle_waiting_workers!

      logger.debug("Workers: #{workers.count}")

      services.each do |service, object|
        logger.debug "Service: #{service} " + \
                     "Requests: #{object.requests.count} " + \
                     "Waiting: #{object.waiting.count}"
      end

      @heartbeat_at = Time.now + (0.001 * HEARTBEAT_INTERVAL)
    else
      logger.debug("Next heartbeat at #{@heartbeat_at}")
    end
  end

  def handle_waiting_workers!
    waiting.each do |worker|
      if Time.now > worker.expiry
        logger.debug("Worker expired. Deleting worker...")
        delete_worker(worker)
      else
        send_to_worker(worker, MDP::HeartbeatMessage.new)
      end
    end
  end

  # Remove a worker from registration. If disconnect is True, send a disconnect message
  #
  # @param [ Worker ] worker
  # @param [ True, False ] disconect
  #
  # @return [ Worker ] The deleted worker
  def delete_worker(worker, disconnect = false)
    logger.info("Deleting worker: #{worker.address.inspect} Disconnect: #{disconnect}")

    send_to_worker(worker, MDP::DisconnectMessage.new) if disconnect

    worker.service.waiting.delete(worker) if worker.service
    waiting.delete(worker)
    workers.delete(worker.address)
  end

  # Build and send a message to a worker
  #
  # @param [ Worker ] worker
  # @param [ Message ] message
  def send_to_worker(worker, message)
    raw = message.pack
    raw.unshift worker.address
    logger.debug("Sending to Worker: #{raw.inspect}")
    @socket.send_strings(raw)
    logger.error "#{ZMQ::Util.error_string} (#{ZMQ::Util.errno})\t#{raw}" if ZMQ::Util.errno == 113
  end

  # Process a message from a client
  #
  # @param [ AddressableMessage ] message
  def process_client(message)
    logger.debug("Recevied message from client #{message.address}: #{message.pack}")
    if message.internal_service?
      service_internal(message)
    else
      service = require_service(message.service)
      dispatch(service, message)
    end
  end

  # Handle an internal service request
  def service_internal(message)
    code = '501'
    if message.service == 'mmi.service'
      code = services.key?(message.body) ? '200' : '404'
    end

    message = MDP::ClientMessage.new(message.address, message.service, code)

    @socket.send_strings(message.pack)
  end

  # Process a message sent from a +MajordomoWorker+
  #
  # @param [ AddressableMessage ] message
  def process_worker(message)
    logger.debug("Received message from worker #{message.address}: #{message.pack}")
    worker_exists = workers[message.address]
    worker = require_worker(message.address)

    case message.command
    when MDP::W_REPLY
      if worker_exists
        raw = MDP::ClientMessage.new(worker.service.name, message.body).pack
        raw.unshift(message.client_address)
        logger.debug("Sending message to client #{message.client_address}: #{raw}")
        @socket.send_strings(raw)
        logger.error "#{ZMQ::Util.error_string} (#{ZMQ::Util.errno})\t#{raw}" unless ZMQ::Util.errno == 0
        worker_waiting(worker)
      else
        delete_worker(worker)
      end
    when MDP::W_READY
      service = message.service

      if worker_exists || service.start_with?(MDP::INTERNAL_SERVICE_PREFIX)
        delete_worker(worker, true)
      else
        worker.service = require_service(service)
        worker_waiting(worker)
        logger.info("Worker added: #{worker.address.inspect}")
      end
    when MDP::W_HEARTBEAT
      if worker_exists
        worker.expiry = Time.now + 0.001 * HEARTBEAT_EXPIRY
      else
        delete_worker(worker, true)
      end
    when MDP::W_DISCONNECT
      delete_worker(worker)
    else
      logger.warn("Invalid message: #{message.inspect}")
    end
  end

  # Dispatch outstanding messages to their destination
  #
  # @param [ Service ] service
  # @param [ Message ] message
  def dispatch(service, message)
    service.requests << message if message

    while service.waiting.any? && service.requests.any?
      message = service.requests.shift
      worker = service.waiting.shift
      waiting.delete(worker)

      send_to_worker(worker, MDP::RequestMessage.new(message.address, message.body))
    end
  end

  def require_worker(address)
    workers[address] ||= Worker.new(address, HEARTBEAT_EXPIRY)
  end

  def require_service(name)
    services[name] ||= Service.new(name)
  end

  def worker_waiting(worker)
    waiting << worker
    worker.service.waiting << worker
    worker.expiry = Time.now + 0.001 * HEARTBEAT_EXPIRY

    dispatch(worker.service, nil)
  end

  private

  def shutdown!(signal = nil)
    $stdout.write("Shutting down...")
    @poller.deregister_readable(@socket)
    @socket.close
    @context.terminate
    exit
  end

  def register_signals
    [ "INT", "TERM", "QUIT"].map do |signal|
      Signal.trap(signal, &method(:shutdown!))
    end
  end

  def waiting
    @waiting ||= []
  end

  def workers
    @workers ||= {}
  end

  def unroutable_messages
    @unroutable_message ||= {}
  end

  def services
    @services ||= {}
  end
end

if __FILE__ == $0
  broker = MajorDomoBroker.new("tcp://*:5555")
  broker.mediate
end
