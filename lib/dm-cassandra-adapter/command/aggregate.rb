# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter
      module Command

        # Aggregate records in Cassandra
        class Aggregate < Read

          def initialize(*)
            super
            @operators, @properties = @query.fields.partition do |field|
              field.kind_of?(DataMapper::Query::Operator)
            end
          end

          def result
            first_operator_name = @operators.first.operator.to_s
            map { |row| row[first_operator_name] }
          end

        protected

          def fields
            [operator_fields, property_fields].flatten
          end

        private

          def property_fields
            @properties.map(&:field)
          end

          def operator_fields
            @operators.map do |operator|
              argument = operator.target == :all ? '*' : operator.target.field
              function = operator.operator.to_s.upcase
              "#{function}(#{argument})"
            end
          end

        end
      end
    end
  end
end
