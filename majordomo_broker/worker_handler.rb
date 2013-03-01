class MajorDomoBroker
  class WorkerHandler
    include Loggable

    def initialize(socket)
      @socket = socket
    end

    # Handle messages to and from Workers
    #
    # @param [ AddressableMessage ] message
    def handle_message(message)
      case message.command
      when MDP::W_REPLY
        handle_reply(message)
      when MDP::W_READY
        handle_ready(message)
      when MDP::W_HEARTBEAT
        handle_heartbeat(message)
      when MDP::W_DISCONNECT
        handle_disconnect(message)
      else
        logger.warn "Received invalid worker message: #{message.inspect}"
        false
      end
    end

    def handle_reply(message)
      worker_exists = exists?(message.address)
      worker = register(message.address)

      if worker_exists

        message = MDP::ClientMessage.new(message.address, message.service, message.body)

        send_to_client(message)

        worker_waiting(worker)
      else
        deregister(worker)
      end
    end

    def handle_ready(message)
      worker_exists = exists?(message.address)
      worker = register(message.address)

      service = message.service

      if worker_exists || service.start_with?(MDP::INTERNAL_SERVICE_PREFIX)
        deregister(worker, true)
      else
        worker.service = require_service(service)
        start_waiting(worker)
        dispatch(worker.service)
      end
    end

    def handle_heartbeat(message)
      worker_exists = exists?(message.address)
      worker = register(message.address)

      if worker_exists
        worker.expiry = Time.now + 0.001 * HEARTBEAT_EXPIRY
      else
        deregister(worker, true)
      end
    end

    def handle_disconnect(message)
      worker = register(message.address)
      deregister(worker) if worker
    end

    def dispatch(service, message = nil)
      service.requests << message if message

      while service.waiting.any? && service.requests.any?
        message = service.requests.shift
        worker = service.waiting.shift
        waiting.delete(worker)

        send_to_worker(worker, MDP::RequestMessage.new(message.address, message.body))
      end
    end

    def start_waiting(worker)
      waiting << worker
      worker.service.waiting << worker
      worker.expiry = Time.now + 0.001 * HEARTBEAT_EXPIRY
    end

    def send_to_client(message)
      socket.send_strings(message.pack)
    end

    def send_to_worker(worker, message)
      raw = message.pack
      raw.unshift(worker.address)
      socket.send_strings(message.pack)
    end

    def register(address)
      workers[address] ||= Worker.new(address, HEARTBEAT_EXPIRY)
    end

    def deregister(worker, disconnect = false)
      logger.info("Removing worker #{worker.address.inspect}")

      send_to_worker(worker, MDP::Disconnect.new) if disconnect

      worker.service.waiting.delete(worker) if worker.service
      waiting.delete(worker)
      workers.delete(worker.address)
    end

    # @param [ String ] address The address of the +Worker+
    #
    # @return [ True, False ]
    def exists?(address)
      workers.has_key?(address)
    end

    private

    attr_reader :socket

    def workers
      @workers ||= {}
    end

    def waiting
      @waiting ||= {}
    end

  end
end
