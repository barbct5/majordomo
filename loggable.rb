require 'log4r'
require 'log4r/yamlconfigurator'

module Loggable
  def self.included(base)
    Log4r::YamlConfigurator.load_yaml_file(File.expand_path('./config/log4r.yml'))

    module_eval(%Q{
      def logger
        @logger ||= begin
          Log4r::Logger['default'].tap do |_logger|
            _logger.instance_variable_set(:@fullname, #{base})
          end
        end
      end
    })
  end
end
