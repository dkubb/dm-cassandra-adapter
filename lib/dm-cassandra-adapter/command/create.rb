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
            # TODO: batch the requests
            @resources.each do |resource|
              model      = resource.model
              table      = model.storage_name(@adapter.name)
              key        = model.serial(@adapter.name)
              attributes = resource.dirty_attributes

              # Set the default key
              # TODO: replace with a better way to create an unique integer id
              key.set!(resource, attributes[key] ||= timestamp)

              statement = Statement.new(table, attributes)
              @adapter.execute(statement.to_s, *statement.bind_variables)
            end
            self
          end

          def count
            @resources.count
          end

        private

          def timestamp
            Time.now.strftime('%s%9N').to_i
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
