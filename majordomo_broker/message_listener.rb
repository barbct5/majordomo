class MajorDomoBroker
  class MessageListener
    include Loggable

    def initialize(socket, poller)
      @socket = socket
      @poller = poller
      @worker_handler = WorkerHandler.new(socket)
      @client_handler = ClientHandler.new(socket)
    end

    def listen(timeout = HEARTBEAT_INTERVAL)
      items = poller.poll(timeout)
      if items > 0
        raw = []
        socket.recv_strings(raw)

        address = raw.shift
        header = raw[1]

        case header
        when MDP::C_CLIENT
          message = AddressableMessage.new(address, MDP::ClientMessage.unpack(raw))
          worker_handler.handle_message message
        when MDP::W_WORKER
          message = AddressableMessage.new(address, MDP::WorkerMessageParser.unpack(raw))
          client_handler.handle_message message
        else
          logger.warn "Received an invalid message: #{raw.inspect}"
        end
      end
    end

    private

    attr_reader :socket, :poller, :worker_handler, :client_handler
  end
end
