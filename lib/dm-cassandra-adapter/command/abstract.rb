# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter
      module Command

        # Abstract base class for commands
        class Abstract

        private

          def consistency_for(model, action)
            ["#{action}_consistency".to_sym, :consistency].each do |method|
              return model.send(method) if model.respond_to?(method)
            end
            nil
          end

        end # Abstract
      end # Command
    end # CassandraAdapter
  end # Adapters
end # DataMapper
