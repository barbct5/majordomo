class MajorDomoBroker
  class Service
    attr_accessor :requests, :waiting, :name
    def initialize(name)
      @name = name
      @requests = []
      @waiting = []
    end
  end
end
