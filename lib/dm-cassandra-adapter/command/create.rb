# encoding: utf-8

module DataMapper
  module Adapters
    class CassandraAdapter < AbstractAdapter
      class Command

        # Create records in Cassandra
        class Create < self
          def initialize(adapter, resources)
            @adapter   = adapter
            @resources = resources
          end

          def call
            # TODO: batch the statements
            @resources.each do |resource|
              set_serial(resource)
              table     = resource.model.storage_name(@adapter.name)
              statement = Statement.new(table, resource.dirty_attributes)
              @adapter.execute(statement.to_s, *statement.bind_variables)
            end
            self
          end

          def count
            @resources.count
          end

        private

          def set_serial(resource)
            key = resource.model.serial(@adapter.name)
            return if key.nil? || resource.send(key.name)
            resource.send("#{key.name}=", generate_value_for(key))
          end

          def generate_value_for(property)
            case property
            when Property::Serial     then Time.now.strftime('%s%9N').to_i
            when Property::SimpleUUID then property.primitive.new
            else
              raise "Unknown property type: #{property.class}"
            end
          end

          class Statement
            INSERT      = 'INSERT INTO %{table} %{columns} VALUES %{values}'.freeze
            PLACEHOLDER = '?'.freeze
            SEPARATOR   = ', '.freeze

            def initialize(table, attributes)
              @table      = table
              @attributes = attributes
            end

            def columns
              @attributes.keys.map(&:field)
            end

            def bind_variables
              @attributes.values
            end

            def to_s
              INSERT % {
                table:   @table,
                columns: parenthesis(columns),
                values:  parenthesis(columns.map { PLACEHOLDER })
              }
            end

          private

            def parenthesis(*statements)
              "(#{statements.join(SEPARATOR)})"
            end

          end # Statement
        end # Create
      end # Statement
    end # CassandraAdapter
  end # Adapters
end # DataMapper
