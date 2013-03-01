require 'ffi-rzmq'
load 'mdp.rb'

class MajorDomoBroker

  HEARTBEAT_INTERVAL = 2500
  HEARTBEAT_LIVELINESS = 3
  HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVELINESS
  INTERNAL_SERVICE_PREFIX = 'mmi.'

  def initialize

    @context = ZMQ::Context.new

    # Configure Sockets
    @socket = @context.socket(ZMQ::ROUTER)
    @socket.setsockopt(ZMQ::LINGER, 0)

    # Configure Poller
    @poller = ZMQ::Poller.new
    @poller.register(@socket, ZMQ::POLLIN)

    # Set next heartbeat
    @heartbeat_at = Time.now + 0.001 * HEARTBEAT_INTERVAL

    Signal.trap("INT") do
      $stdout.write("Shutting down...\n")
      @socket.close
      @context.terminate
      exit
    end
  end

  # Bind broker to endpoint. Can call this multiple times.
  # We use a single socket for both clients and workers
  #
  # @param [ String ] endpoint A String representation of a ZMQ endpoint
  def bind(endpoint)
    @socket.bind(endpoint)
    self
  end

  def mediate
    loop do
      items = @poller.poll(HEARTBEAT_INTERVAL)
      if items > 0
        message = []
        @socket.recv_strings(message)

        $stdout.write("Full message: #{message}\n")

        address = message.shift
        message.shift
        header = message.shift

        case header
        when MDP::C_CLIENT
          process_client(address, message)
        when MDP::W_WORKER
          process_worker(address, message)
        else
          $stdout.write("E: invalid messages: #{message.inspect}\n")
        end
      end

      if Time.now > @heartbeat_at
        waiting.each do |worker|
          if Time.now > worker.expiry
            delete_worker(worker)
          else
            send_to_worker(worker, MDP::W_HEARTBEAT)
          end
        end

        puts "Workers: #{workers.count}"

        services.each do |service, object|
          $stdout.write "Service: #{service}: requests: #{object.requests.count} waiting: #{object.waiting.count}\n"
        end

        @heartbeat_at = Time.now + 0.001 * HEARTBEAT_INTERVAL
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
    puts "delete_worker: #{worker.address.inspect} disconnect: #{disconnect}"

    send_to_worker(worker, MDP::W_DISCONNECT) if disconnect

    worker.service.waiting.delete(worker) if worker.service
    waiting.delete(worker)
    workers.delete(worker.address)
  end

  # Build and send a message to a worker
  #
  # @param [ Worker ] worker
  # @param [ String ] command
  # @param [ Object ] option
  # @param [ Array ] message
  def send_to_worker(worker, command, option = nil, message = [])
    message = [ message ] unless message.is_a?(Array)

    message.unshift(option) if option
    message.unshift(command)
    message.unshift(MDP::W_WORKER)
    message.unshift('')
    message.unshift(worker.address)

    @socket.send_strings(message)
  end

  # Process a message from a client
  #
  # @param [ Address ] address
  # @param [ Array ] message
  def process_client(address, message)
    service = message.shift
    message.unshift('')
    message.unshift(address)

    if service.start_with?(INTERNAL_SERVICE_PREFIX)
      service_internal(service, message)
    else
      dispatch(require_service(service), message)
    end
  end

  # Handle an internal service request
  def service_internal(service, message)
    code = '501'
    if service == 'mmi.service'
      code = services.key?(message.last) ? '200' : '404'
    end

    message.insert(2, [ MDP::C_CLIENT, service ])
    message[-2] = code
    message.flatten!

    @socket.send_strings(message)
  end

  def process_worker(address, message)
    $stderr.write("Received message: #{message}\n")
    command = message.shift

    worker_exists = workers[address]
    worker = require_worker(address)

    case command
    when MDP::W_REPLY
      if worker_exists
        client = message.shift
        message.shift
        message = [ client, '', MDP::C_CLIENT, worker.service.name ].concat(message)

        @socket.send_strings(message)

        worker_waiting(worker)
      else
        delete_worker(worker)
      end
    when MDP::W_READY
      service = message.shift

      if worker_exists || service.start_with?(INTERNAL_SERVICE_PREFIX)
        delete_worker(worker, true)
      else
        worker.service = require_service(service)
        worker_waiting(worker)
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
      $stdout.write("E: invalid message: #{message.inspect}")
    end
  end

  def dispatch(service, message)
    service.requests << message if message

    while service.waiting.any? && service.requests.any?
      message = service.requests.shift
      worker = service.waiting.shift
      waiting.delete(worker)

      send_to_worker(worker, MDP::W_REQUEST, nil, message)
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

  class Worker
    attr_accessor :service, :expiry, :address
    def initialize(address, lifetime)
      @address = address
      @expiry = Time.now + 0.001 * lifetime
    end
  end

  class Service
    attr_accessor :requests, :waiting, :name
    def initialize(name)
      @name = name
      @requests = []
      @waiting = []
    end
  end

  private

  def waiting
    @waiting ||= []
  end

  def workers
    @workers ||= {}
  end

  def services
    @services ||= {}
  end
end

if __FILE__ == $0
  broker = MajorDomoBroker.new
  broker.bind("tcp://*:5555")
  broker.mediate
end
