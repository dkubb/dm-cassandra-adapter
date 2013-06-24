# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter
      module Command

        # Aggregate records in Cassandra
        class Aggregate < Read

          def result
            map { |row| row[@operators.first.operator.to_s] }
          end

        protected

          def fields
            @operators = []
            @query.fields.map do |field|
              return field.field unless field.kind_of?(DataMapper::Query::Operator)
              @operators << field
              argument = field.target == :all ? '*' : field.target.field
              function = field.operator.to_s.upcase
              "#{function}(#{argument})"
            end
          end

        end
      end
    end
  end
end
