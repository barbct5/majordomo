class MajorDomoBroker
  class AddressableMessage < SimpleDelegator
    attr_reader :address
    def initialize(address, message)
      @address = address
      @message = message
      super(message)
    end
  end
end


