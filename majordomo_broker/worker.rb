class MajorDomoBroker
  class Worker
    attr_accessor :service, :expiry, :address
    def initialize(address, lifetime)
      @address = address
      @expiry = Time.now + 0.001 * lifetime
    end
  end
end


