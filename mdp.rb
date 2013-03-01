module MDP
  C_CLIENT      = "MDPC01"
  W_WORKER      = "MDPW01"

  W_READY       = "0x01"
  W_REQUEST     = "0x02"
  W_REPLY       = "0x03"
  W_HEARTBEAT   = "0x04"
  W_DISCONNECT  = "0x05"

  INTERNAL_SERVICE_PREFIX = 'mmi.'

  module MessageIdGenerator
    extend self

    require 'securerandom'

    def generate
      SecureRandom.uuid
    end

    def valid?(string)
      !!(string =~ regexp)
    end

    def regexp
      @regexp ||= /\A[\da-f]{8}-([\da-f]{4}-){3}[\da-f]{12}\z/i
    end
  end

  class InvalidMessageID < StandardError
  end

  module Identifiable
    attr_reader :uuid
    def uuid
      @uuid ||= MessageIdGenerator.generate
    end
  end

  class ClientMessage
    include Identifiable

    class InvalidClientMessage < StandardError
    end

    def self.unpack(message)
      message.shift # empty frame
      raise InvalidClientMessage if message.shift != C_CLIENT

      uuid = message.shift
      raise InvalidMessageID unless MessageIdGenerator.valid?(uuid)

      obj = allocate
      obj.instance_variable_set(:@uuid, uuid)
      obj.send(:initialize, *message)

      obj
    end

    attr_reader :service, :body
    def initialize(service, body = nil)
      @service = service
      @body = body
    end

    def header
      C_CLIENT
    end

    def internal_service?
      service.start_with?(INTERNAL_SERVICE_PREFIX)
    end

    def pack
      [ '', header, uuid, service, body ]
    end
  end

  class InvalidWorkerMessage < StandardError
  end

  module WorkerMessage
    def header
      W_WORKER
    end
  end

  class ReadyMessage
    include Identifiable
    include WorkerMessage

    attr_reader :service
    def initialize(service)
      @service = service
    end

    def command
      W_READY
    end

    def pack
      [ '', header, command, uuid, service ]
    end
  end

  class RequestMessage
    include Identifiable
    include WorkerMessage

    attr_reader :client_address, :body
    def initialize(client_address, body)
      @client_address= client_address
      @body = body
    end

    def command
      W_REQUEST
    end

    def pack
      [ '', header, command, uuid, client_address, '', body ]
    end
  end

  class ReplyMessage
    include Identifiable
    include WorkerMessage

    attr_reader :client_address, :body
    def initialize(client_address, body)
      @client_address= client_address
      @body = body
    end

    def command
      W_REPLY
    end

    def pack
      [ '', header, command, uuid, client_address, '', body ]
    end
  end

  class HeartbeatMessage
    include Identifiable
    include WorkerMessage

    def command
      W_HEARTBEAT
    end

    def pack
      [ '', header, command, uuid ]
    end
  end

  class DisconnectMessage
    include Identifiable
    include WorkerMessage

    def command
      W_DISCONNECT
    end

    def pack
      [ '', header, command, uuid ]
    end
  end

  class WorkerMessageParser
    def self.unpack(raw)
      orig = raw.dup
      raw.shift # empty frame
      raise InvalidWorkerMessage, orig if raw.shift != W_WORKER

      command = raw.shift

      uuid = raw.shift
      raise InvalidMessageID unless MessageIdGenerator.valid?(uuid)

      case command
      when W_READY
        service = raw.shift
        message = ReadyMessage.new(service)
        message.instance_variable_set(:@uuid, uuid)
        message
      when W_REQUEST
        address = raw.shift
        raw.shift # empty frame
        message = RequestMessage.new(address, raw.shift)
        message.instance_variable_set(:@uuid, uuid)
        message
      when W_REPLY
        address = raw.shift
        raw.shift # empty frame
        message = ReplyMessage.new(address, raw.shift)
        message.instance_variable_set(:@uuid, uuid)
        message
      when W_HEARTBEAT
        message = HeartbeatMessage.new
        message.instance_variable_set(:@uuid, uuid)
        message
      when W_DISCONNECT
        message = DisconnectMessage.new
        message.instance_variable_set(:@uuid, uuid)
        message
      else
        raise InvalidWorkerMessage
      end
    end
  end
end


